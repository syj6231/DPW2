# hft/mdms/base/publisher.py
import aiohttp
import asyncio
import json
import logging
import threading
import time
from typing import Optional, Dict, Any

_log = logging.getLogger(__name__)

def now_in_micros() -> int:
    return int(time.time() * 1_000_000)

class WebSocketPublisher:
    """
    공용 WebSocket 베이스:
      - 실제 aiohttp 기반 WS 연결/재연결 루프
      - 동기(on_open) & 비동기(_on_open_async) 훅 모두 지원
      - 서브클래스가 _ws_send_json 또는 _ws_send_json_async로 실제 전송 가능
    """
    def __init__(self, ws_url, transport, *, log_interval_sec=None,
                 verbose=True, max_backlog=100_000, on_full="block", drain_timeout_sec=5.0):
        self._ws_url = ws_url
        self._transport = transport
        self._verbose = verbose
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # 옵션 값들(지금은 사용 안해도 인터페이스 유지를 위해 보관)
        self._log_interval_sec = log_interval_sec
        self._max_backlog = max_backlog
        self._on_full = on_full
        self._drain_timeout_sec = drain_timeout_sec

    # ---------------- lifecycle ----------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_thread, daemon=True, name=self.__class__.__name__)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._loop and self._loop.is_running():
            # 루프 깨우기
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0), self._loop)
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run_thread(self):
        self._loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(self._loop)
        except Exception:
            pass
        self._loop.run_until_complete(self._run_loop())

    async def _run_loop(self):
        # trust_env=True: 회사/학교 프록시 환경에서 OS 환경변수(HTTPS_PROXY 등) 자동 활용
        async with aiohttp.ClientSession(trust_env=True) as session:
            while self._running:
                try:
                    async with session.ws_connect(
                        self._ws_url,
                        heartbeat=20,
                        max_msg_size=0,  # 큰 스냅샷 방어
                    ) as ws:
                        self._ws = ws
                        try:
                            await self._on_open_async(ws)  # 비동기 훅
                        except Exception:
                            _log.exception("error in _on_open_async")

                        # 동기 훅도 유지(서브클래스 호환)
                        try:
                            self.on_open(ws)
                        except Exception:
                            _log.exception("error in on_open")

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.on_message(ws, msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                self.on_message(ws, msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise ws.exception()
                except Exception as e:
                    _log.warning("ws error: %s (reconnect in 3s)", e)
                    await asyncio.sleep(3)
                finally:
                    try:
                        self.on_close(self._ws, 1006, "reconnect")
                    except Exception:
                        _log.exception("error in on_close")
                    self._ws = None

    # ---------------- hooks ----------------
    async def _on_open_async(self, ws) -> None:
        """비동기 구독/인증 프레임 전송 등은 여기서! (서브클래스에서 오버라이드)"""
        pass

    def on_open(self, ws):  _log.info("ws open")          # 동기 훅(레거시 호환)
    def on_close(self, ws, code, msg): _log.info("ws close %s %s", code, msg)
    def on_message(self, ws, message): pass

    # ---------------- send helpers ----------------
    async def _ws_send_json_async(self, ws, obj: Dict[str, Any]) -> None:
        if self._verbose:
            _log.info("send -> %s", obj)
        await ws.send_str(json.dumps(obj))

    def _ws_send_json(self, obj: Dict[str, Any]) -> None:
        """
        동기 컨텍스트에서 호출해도 전송되도록 스레드-세이프하게 코루틴 스케줄.
        (서브클래스 기존 코드 호환용)
        """
        if not (self._loop and self._ws):
            if self._verbose:
                _log.info("send(dropped; not connected yet) -> %s", obj)
            return
        fut = asyncio.run_coroutine_threadsafe(self._ws_send_json_async(self._ws, obj), self._loop)
        # 결과 기다릴 필요는 없음. 실패 시 로그만 찍게 함.
        try:
            fut.result(timeout=2.0)
        except Exception as e:
            _log.warning("send failed: %s", e)

    # ---------------- publish helper ----------------
    def _append_backlog(self, topic: str, payload: Dict[str, Any]) -> None:
        self._transport.publish(topic, payload)
