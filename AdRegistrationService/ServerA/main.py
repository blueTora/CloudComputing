import mysql.connector
from flask import Flask, request
import pika
import boto3
import logging
from botocore.exceptions import ClientError

# create the Flask app
app = Flask(__name__)

hostName = "localhost"
serverPort = 8080

# RabbitMQ info
AMQP_URL = "amqps://uggaigyk:uQYOzMp_nqVvfHvtas5irINmGn7MXSA7@albatross.rmq.cloudamqp.com/uggaigyk"
queue = 'ads_request'

# database info
HOST = "mysql-30bcc0f0-ali-9386.aivencloud.com"
PORT = 19178
USER = "avnadmin"
PASSWORD = "AVNS_xStWFPUejWZo7felKkN"
DATABASE = "defaultdb"

my_db = None
states = ("in waiting line...", "approved.", "opposed.")

# S3 info
Bucket = 'adphoto'
storage_url = 'https://s3.ir-thr-at1.arvanstorage.ir'
access_key = '607ee48e-7490-48a7-91bc-0b744baffb97'
secret_key = '121c0f44f1fec3a8ebdd26661c88e2bed4f31741'


@app.route('/form-data', methods=['POST'])
def form_data():
    if request.method == 'POST':
        mail = request.form.get('email')
        text = request.form.get('description')
        img = request.files.get('image')

        ad_id = request.form.get('id')

        if ad_id is None:
            res = first_api(mail, text, img)
        else:
            res = second_api(ad_id)

        return res


def first_api(mail, text, img):
    last_id = get_last_id() + 1

    t = img.filename.split('.')[-1]
    img_name = str(last_id) + '.' + t

    save_img(img, img_name)
    insert_data(mail, text, img_name)
    rabbit_send(last_id)

    return "آگهی شما با شناسه " + str(last_id) + " ثبت شد"


def second_api(ad_id):
    res = "400 not exist"

    row = select_data(ad_id)

    if row[2] == states[0]:
        res = "آگهی شما در صف بررسی است"
    elif row[2] == states[2]:
        res = "آگهی شما تأیید نشد"
    elif row[2] == states[1]:
        res = ""
        res += "وضعیت: " + row[2] + "\n"
        res += "متن: " + row[3] + "\n"
        res += "دسته بندی: " + row[5] + "\n"
        res += "لینک تصویر: " + get_img_link(row[4])
        download_img(row[4])

    return res


def insert_data(mail, text, img_addr):

    try:
        cursor = my_db.cursor()
        sql = "INSERT INTO Advertisement (description, email, state, imageLink) VALUES (%s, %s, %s, %s)"
        val = (text, mail, states[0], img_addr)
        cursor.execute(sql, val)

        my_db.commit()
    finally:
        print(cursor.rowcount, "record inserted.")


def get_last_id():
    try:
        cursor = my_db.cursor()
        sql = "SELECT id FROM Advertisement ORDER BY id DESC LIMIT 1"
        cursor.execute(sql)
        last_id = cursor.fetchall()

    finally:
        print(cursor.rowcount, "record inserted.")
        return last_id[0][0]


def save_img(img, name):

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
            bucket.put_object(
                ACL='private',
                Body=img,
                Key=str(name)
            )

            print(f'Image Saved to S3: {name}')

        except ClientError as e:
            logging.error(e)


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


def get_img_link(img_name):
    logging.basicConfig(level=logging.INFO)

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=storage_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    except Exception as exc:
        logging.error(exc)
    else:
        try:
            img_link = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': Bucket,
                    'Key': img_name
                },
                ExpiresIn=3600
            )
        except ClientError as e:
            logging.error(e)

    return img_link


def rabbit_send(ad_id):
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    channel.basic_publish(exchange='', routing_key=queue, body=str(ad_id))
    print(f" [x] Sent Ad ID: {ad_id}")
    connection.close()


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


def make_database():
    cursor = my_db.cursor()
    cursor.execute("CREATE TABLE Advertisement (id INT(255) NOT NULL AUTO_INCREMENT, email VARCHAR(255), state VARCHAR(255), description TEXT(32767), imageLink VARCHAR(2047), category VARCHAR(255), PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8")
    print('Database is Created.')


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


# print table <advertisement>
def print_table():
    cursor = my_db.cursor()

    cursor.execute("SELECT * FROM Advertisement")

    result = cursor.fetchall()

    for x in result:
        print(x)


def close():
    print_table()

    my_db.close()
    print('Database Connection is Closed.')


def delete_table(table):
    cursor = my_db.cursor()

    # sql = "DROP TABLE " + table
    sql = "DELETE FROM " + table

    cursor.execute(sql)
    print(f'Database {table} is removed.')


def main():
    start_database()
    # make_database()

    app.run(debug=True, port=8080)

    close()
    # delete_table('Advertisement')


if __name__ == '__main__':
    main()
