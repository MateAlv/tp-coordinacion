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
SUM_CONTROL_EXCHANGE = str(os.environ["SUM_PREFIX"]) + "_control"
SUM_RESPONSE_QUEUE_PREFIX = f"{SUM_PREFIX}_response"
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

        self.lock = threading.Lock()
        self.clients = {}
        
        self.pending_eof = {}

        self.leader_totals = {}  
        self.expected_totals = {}

        self.my_response_queue_name = f"{SUM_RESPONSE_QUEUE_PREFIX}_{ID}"

    def _get_exchange_to_aggs(self, fruit):
        routing_exchange_key = zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT
        return self.data_output_exchanges[routing_exchange_key]
    
    def _broadcast_control_signal(self, client_id, expected_count):
        control_exchange = self._new_control_exchange()
        control_exchange.send(
            message_protocol.internal.serialize([client_id, int(expected_count), ID])
        )
        control_exchange.close()

    def _handle_control_signal(self, message, ack, nack):
        client_id, expected_totals, leader_id = message_protocol.internal.deserialize(message)

        self.lock.acquire()
        current_count = self.clients.get(client_id, [{}, 0])[MESSAGE_COUNT_POS]
        current_fruits = dict(self.clients.get(client_id, [{}, 0])[FRUITS_POS])
        self.pending_eof[client_id] = (expected_totals, leader_id)
        self.lock.release()

        for fruit_item in current_fruits.values():
            self._forward_data_to_aggs(client_id, fruit_item.fruit, fruit_item.amount)

        self._report_increment_to_leader(client_id, leader_id, increment=current_count)

        ack()

    def _handle_count_report(self, message, ack, nack):
        client_id, count = message_protocol.internal.deserialize(message)

        self.leader_totals[client_id] = self.leader_totals.get(client_id, 0) + count

        expected = self.expected_totals.get(client_id)
        if expected is not None and self.leader_totals[client_id] == expected:
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client_id]))
            del self.leader_totals[client_id]
            del self.expected_totals[client_id]
            self.clients.pop(client_id, None)
            self.pending_eof.pop(client_id, None)

        ack()

    def _forward_data_to_aggs(self, client_id, fruit, amount):
        self._get_exchange_to_aggs(fruit).send(
            message_protocol.internal.serialize([client_id, fruit, int(amount)])
        )

    def _report_increment_to_leader(self, client_id, leader_id, increment=1):
        response_queue = self._new_response_queue(leader_id)
        response_queue.send(
            message_protocol.internal.serialize([client_id, int(increment)])
        )
        response_queue.close()

    def _handle_data_record(self, client_id, fruit, amount):
        with self.lock:
            self._process_data(client_id, fruit, amount)
            pending = self.pending_eof.get(client_id)

        if pending is None:
            return

        _, leader_id = pending
        self._forward_data_to_aggs(client_id, fruit, amount)
        self._report_increment_to_leader(client_id, leader_id, 1)

    def _handle_gateway_eof(self, client_id, expected_count):
        with self.lock:
            self.expected_totals[client_id] = int(expected_count)

        self._broadcast_control_signal(client_id, expected_count)

    def _response_queue_name(self, leader_id):
        return f"{SUM_RESPONSE_QUEUE_PREFIX}_{leader_id}"

    def _new_control_exchange(self):
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

    def _new_response_queue(self, leader_id):
        return middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._response_queue_name(leader_id)
        )

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data for client %s", client_id)

        client = self.clients.setdefault(client_id, [{}, 0])

        client[MESSAGE_COUNT_POS] += 1
        client[FRUITS_POS][fruit] = client[FRUITS_POS].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)

            if len(fields) == 3:
                self._handle_data_record(*fields)
            elif len(fields) == 2:
                self._handle_gateway_eof(*fields)
            else:
                raise ValueError(f"Unexpected message format: {fields}")

            ack()
        except Exception:
            logging.exception("Error processing data message")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
