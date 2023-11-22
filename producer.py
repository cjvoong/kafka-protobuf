import proto.addressbook_pb2 as addressbook_pb2
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: v.SerializeToString())

address_book = addressbook_pb2.AddressBook()

person = address_book.people.add()

person.id = 1
person.name = "Joe2"
person.email = "Joe2@email.com"
phone_number = person.phones.add()
phone_number.number = "07999999999"
phone_number.type = addressbook_pb2.Person.PhoneType.PHONE_TYPE_MOBILE

producer.send('address_book_proto2', address_book)
producer.flush()