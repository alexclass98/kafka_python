from concurrent import futures
import grpc
import my_proto_pb2
import my_proto_pb2_grpc
from kafka import KafkaProducer, KafkaConsumer


class MyServiceServicer(my_proto_pb2_grpc.MyServiceServicer):
    kafka_server = 'localhost:29092'
    producer = KafkaProducer(bootstrap_servers=[kafka_server], value_serializer=lambda x: x.encode('utf-8'))
    consumer = KafkaConsumer("my_topic_name", bootstrap_servers=[kafka_server], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my_group_id', value_deserializer=lambda x: x.decode('utf-8'))

    def MyMethod(self, request, context):
        id_card = request.id_card
        number_card = request.number_card
        cvc = request.cvc
        pin = request.pin
        contract_id = request.contract_id

        message = f"id_card:{id_card}|number_card:{number_card}|cvc:{cvc}|pin:{pin}|contract_id:{contract_id}"
        self.producer.send("my_topic_name", message)
        self.producer.flush()

        for msg in self.consumer:
            if msg.topic == "my_topic_name":
                response_msg = msg.value
                id_card, number_card, cvc, pin, contract_id = response_msg.split("|")
                response = my_proto_pb2.MyResponse(id_card=int(id_card.split(":")[1]),
                                                   number_card=int(number_card.split(":")[1]),
                                                   cvc=int(cvc.split(":")[1]),
                                                   pin=int(pin.split(":")[1]),
                                                   contract_id=int(contract_id.split(":")[1]))
                return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    my_proto_pb2_grpc.add_MyServiceServicer_to_server(MyServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()