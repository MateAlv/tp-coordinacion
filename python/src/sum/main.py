import os
import logging
import threading
import zlib # Importo para hashing consistente de routing entre instancias de Sum 

from common import middleware, message_protocol, fruit_item

# ENV variables:
ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Positions for client list of items:
FRUITS_POS = 0
MESSAGE_COUNT_POS = 1

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.clients = {}
        self.is_leader = False

    def _get_exchange_to_aggs(self, fruit):
        routing_exchange_key = zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT
        return self.data_output_exchanges[routing_exchange_key]

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")

        client = self.clients.setdefault(client_id, [{}, 0])

        client[MESSAGE_COUNT_POS] += 1
        client[FRUITS_POS][fruit] = client[FRUITS_POS].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")

        client_fruits = self.clients.get(client_id, [{}, 0])[FRUITS_POS]
        for final_fruit_item in client_fruits.values():
            self._get_exchange_to_aggs(final_fruit_item.fruit).send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.clients.pop(client_id, None)


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            client_id = fields[0]
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
