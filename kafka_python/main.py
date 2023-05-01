import smtplib
import time
from email.mime.text import MIMEText
from json import dumps
from kafka import KafkaProducer, KafkaConsumer


def sendKafka():
    my_producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    message = "1"
    while message != "0":
        message = input("Type your message: ")
        my_producer.send("testnum", value=message)
    print("sendKafka ended")


def getKafka():
    consumer = KafkaConsumer('testnum',
                             bootstrap_servers=['localhost:29092'],
                             group_id='test',
                             auto_offset_reset='earliest')
    for msg in consumer:
        res_str = msg.value.decode("utf-8")
        print("Text:", res_str)
        send_email(res_str, 0)
    print("getKafka ended")


def send_email(message, count):
    sender = "alexorange707@gmail.com"
    password = "jdnpdzqcngqvyruu"
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
    except Exception as _ex:
        time.sleep(5)
        print("Проверьте Ваш интернет! Полученное слово не доставлено")
        if count < 3:
            count += 1
            send_email(message, count)
        else:
            print("Ошибка отправки(")
        print(f"{_ex}\nCheck your internet!")
        return

    try:
        server.login(sender, password)
        msg = MIMEText(message)
        msg["Subject"] = "DZ PO ST Balabanov!"
        server.sendmail(sender, sender, msg.as_string())

        # server.sendmail(sender, sender, f"Subject: CLICK ME PLEASE!\n{message}")
        print("Сообщение доставлено!")
    except Exception as _ex:
        print(f"{_ex}\nCheck your login or password please!")
        return





def print_hi():
    print(f'Здравствуйте!\n Брокер успешно запущен')



def main():
    print_hi()
    while True:
        sendKafka()
        getKafka()



if __name__ == "__main__":
    main()