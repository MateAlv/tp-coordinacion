from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, message):
        [client_id, fruit, amount] = message
        return message_protocol.internal.serialize([client_id, fruit, amount])

    def serialize_eof_message(self, client_id):
        return message_protocol.internal.serialize([client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return (fields[0], fields[1])