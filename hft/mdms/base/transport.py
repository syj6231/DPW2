# transport.py
from typing import Any, Dict, List, Tuple

class PubTransport:
    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

class PrintTransport(PubTransport):
    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        bids: List[Tuple[str, str]] = payload.get("bids") or []
        asks: List[Tuple[str, str]] = payload.get("asks") or []
        best_bid = bids[0] if bids else None
        best_ask = asks[0] if asks else None
        print(f"[PUB] {topic} best_bid={best_bid} best_ask={best_ask} seq={payload.get('sequence_num')}")
