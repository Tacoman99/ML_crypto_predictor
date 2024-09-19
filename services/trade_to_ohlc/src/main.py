from datetime import timedelta

from quixstreams import Application
from loguru import logger

def init_ohlcv_candle(trade: dict):
    """
    Returns the initial OHLCV candle when the first `trade` in that window happens.
    """
    return {
        'open': trade['price'],
        'high': trade['price'],
        'low': trade['price'],
        'close': trade['price'],
        'volume': trade['quantity'],
        'product_id': trade['product_id'],
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    """
    Updates the OHLCV candle with the new `trade`.
    """
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']
    candle['product_id'] = trade['product_id']
    # candle['timestamp_ms'] = trade['timestamp_ms']

    return candle

def transform_trade_to_ohlcv(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_consumer_group: str,
    ohlcv_window_seconds: int,
):
    """
    Reads incoming trades from the given `kafka_input_topic`, transforms them into OHLC data
    and outputs them to the given `kafka_output_topic`.

    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_input_topic (str): The Kafka topic to read the trades from
        kafka_output_topic (str): The Kafka topic to save the OHLC data
        kafka_consumer_group (str): The Kafka consumer group

    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Create a Quix Streams DataFrame
    sdf = app.dataframe(input_topic)

    # check if we are actually reading incoming trades
    # sdf.update(logger.debug)

    # the meat of the transformation
    # Aggregates trades into 1-minute OHLCV candles
    
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds))
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle)
        # .final()
        .current()
    )

    # unpack the dictionary into separate columns
    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['volume'] = sdf['value']['volume']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp_ms'] = sdf['end']

    # keep only the columns we are interested in
    sdf = sdf[['product_id', 'timestamp_ms', 'open', 'high', 'low', 'close', 'volume']]

    # print the output to the console
    sdf.update(logger.debug)

    # push these message to the output topic
    sdf = sdf.to_topic(output_topic)

    # kick off the application
    app.run(sdf)


if __name__ == "__main__":

    transform_trade_to_ohlcv(
        kafka_broker_address='localhost:19092',
        kafka_input_topic='trade',
        kafka_output_topic='ohlcv',
        kafka_consumer_group='consumer_group_trade_to_ohlcv_2',
        ohlcv_window_seconds=60,
    )