# apps/run_coinbase.py
import logging, time
from hft.mdms.base.transport import PrintTransport
from hft.mdms.coinbase.orderbook_publisher import CoinbaseOrderBookPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

if __name__ == "__main__":
    transport = PrintTransport()
    products = ["BTC-USD", "ETH-USD"]   # 필요 심볼
    pub = CoinbaseOrderBookPublisher(
        transport=transport,
        product_ids=products,
        sandbox=False,                   # 실계: False
        enable_rest_snapshot=False,     # JWT 있으면 True 가능
    )
    pub.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pub.stop()
