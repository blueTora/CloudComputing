import mysql.connector
import pika, sys, os
import boto3
import logging
from botocore.exceptions import ClientError
import requests

# RabbitMQ info
AMQP_URL = "amqps://uggaigyk:uQYOzMp_nqVvfHvtas5irINmGn7MXSA7@albatross.rmq.cloudamqp.com/uggaigyk"
queue ='ads_request'

# database info
HOST = "mysql-30bcc0f0-ali-9386.aivencloud.com"
PORT = 19178
USER = "avnadmin"
PASSWORD = "AVNS_xStWFPUejWZo7felKkN"
DATABASE = "defaultdb"

my_db = None
states = ("in waiting line...", "approved.", "opposed.")
mail_resp = ("آگهی شما تایید شده است", "آگهی شما رد شده است")

# imagga info
API_KEY = 'acc_8ff09926baa8031'
API_SECRET = '8fcc65ea3efe9fb6b7ab3b8a3a84110b'

# S3 info
Bucket = 'adphoto'
storage_url = 'https://s3.ir-thr-at1.arvanstorage.ir'
access_key = '607ee48e-7490-48a7-91bc-0b744baffb97'
secret_key = '121c0f44f1fec3a8ebdd26661c88e2bed4f31741'

# mailgun info
api_key = 'd3f12216dccc7033e2d36ffba37e04cb-2de3d545-6bb3df38'
domain = 'sandbox54ae60434ae049f9be24f2a8d2aa9e11.mailgun.org'


def rabbit_receive():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        ad_id = int(body)
        handle_request(ad_id)

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def handle_request(ad_id):
    record = select_data(ad_id)
    download_img(record[4])
    approved, tag = find_tag('images/' + record[4])

    if tag is not None:
        update_database(approved, tag, ad_id)

    response = send_mail(approved, record[1])
    print(response.json())


def find_tag(img_path):

    response = requests.post(
        'https://api.imagga.com/v2/tags',
        auth=(API_KEY, API_SECRET),
        files={'image': open(img_path, 'rb')}
    )
    print(response.json())

    tags = response.json()['result']['tags']

    approved = False
    cat = None
    max_conf = 0

    for tag in tags:
        confidence = tag['confidence']
        tag_name = tag['tag']['en']

        if confidence > max_conf:
            cat = tag_name
            max_conf = confidence

        if tag_name == 'vehicle':
            if confidence >= 50:
                approved = True
            else:
                break

    return approved, cat


def download_img(img_name):
    logging.basicConfig(level=logging.INFO)

    try:
        s3_resource = boto3.resource(
            's3',
            endpoint_url=storage_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
    except Exception as exc:
        logging.error(exc)
    else:
        try:
            bucket = s3_resource.Bucket(Bucket)
            download_path = 'images/' + img_name

            bucket.download_file(
                img_name,
                download_path
            )
        except ClientError as e:
            logging.error(e)


def update_database(approved, tag, uni_id):
    if approved:
        sql = "UPDATE Advertisement SET category = %s, state = %s WHERE id = %s"
        val = (tag, states[1], uni_id)
    else:
        sql = "UPDATE Advertisement SET state = %s WHERE id = %s"
        val = (states[2], uni_id)

    cursor = my_db.cursor()
    cursor.execute(sql, val)

    my_db.commit()
    print(cursor.rowcount, " record(s) affected.")


def send_mail(confirmation, email, subject='وضعیت آگهی'):
    if confirmation:
        text = mail_resp[0]
    else:
        text = mail_resp[1]

    return requests.post(
        f"https://api.mailgun.net/v3/{domain}/messages",
        auth=("api", api_key),
        data={"from": "<mailgun@" + domain + ">",
              "to": [email],
              "subject": subject,
              "text": text})


def select_data(uni_id):
    try:
        cursor = my_db.cursor()

        sql = "SELECT * FROM Advertisement WHERE id = %s"
        val = (uni_id,)

        cursor.execute(sql, val)
        result = cursor.fetchall()
    finally:
        for x in result:
            print(x)

    return result[0]


def start_database():
    # connect to database
    global my_db

    timeout = 10
    my_db = mysql.connector.connect(
        connect_timeout=timeout,
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )

    print(my_db)


def main():
    start_database()

    rabbit_receive()

    my_db.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    # main()
