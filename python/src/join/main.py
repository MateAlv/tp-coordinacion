import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partial_tops = {}
        self.received_count = {}  

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")

        client_id, fruit_top = message_protocol.internal.deserialize(message)
        self.partial_tops.setdefault(client_id, []).append(fruit_top)
        self.received_count[client_id] = self.received_count.get(client_id, 0) + 1

        if self.received_count[client_id] < AGGREGATION_AMOUNT:
            # client done, should send eof forwrd
            pass
        
        total_by_fruit = {}
        for partial in self.partial_tops[client_id]:
            for fruit, amount in partial:
                total_by_fruit[fruit] = total_by_fruit.get(fruit, 0) + int(amount)

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, total_by_fruit])
        )

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
