from kafka import KafkaConsumer
import proto.addressbook_pb2 as addressbook_pb2
import pytest

# Iterates though all people in the AddressBook and prints info about them.
def ListPeople(address_book):
  for person in address_book.people:

    print("Person ID:", person.id)
    print("  Name:", person.name)
    if person.HasField('email'):
      print("  E-mail address:", person.email)

    for phone_number in person.phones:
      if phone_number.type == addressbook_pb2.Person.PhoneType.PHONE_TYPE_MOBILE:
        print("  Mobile phone #: ", end="")
      elif phone_number.type == addressbook_pb2.Person.PhoneType.PHONE_TYPE_HOME:
        print("  Home phone #: ", end="")
      elif phone_number.type == addressbook_pb2.Person.PhoneType.PHONE_TYPE_WORK:
        print("  Work phone #: ", end="")
      print(phone_number.number)
      print("------------")

consumer = KafkaConsumer(bootstrap_servers='localhost:29092',auto_offset_reset='earliest')
consumer.subscribe(['address_book_proto2'])
print("------------")
for message in consumer :
    addressbook = addressbook_pb2.AddressBook()
    addressbook.ParseFromString(message.value)
    assert len(addressbook.people) > 0
    filename = "packet.bin"
    #write file
    with open(filename,"wb") as f:
        f.write(addressbook.SerializeToString())
    ListPeople(addressbook)