import pymysql
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# MySQL credentials
HOST = 'net-rds-slave.cisvtxkbzzbc.ap-south-1.rds.amazonaws.com'
USER = 'db_user'
PASSWORD = 'password'

# Query to check for metadata locks
QUERY = "SHOW PROCESSLIST;"

# Email details
FROM = "noreply@dalmiabharat.com"
TO = ["deepanshu.kumar@goognu.com", "karanti@goognu.com", "Bhatnagar.Aayush@dalmiacement.com", "Kaushal.Ashish@dalmiacement.com"]
USERNAME = "noreply@dalmiabharat.com"
SUBJECT = "Metadata table Lock Alert"
BODY = "Metadata lock detected on the MySQL server."
SMTP_SERVER = "ndc-smtp-001.dalmiabharat.com"
SMTP_PORT = 25

def send_email():
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = ", ".join(TO)
    msg['Subject'] = SUBJECT

    msg.attach(MIMEText(BODY, 'plain'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.sendmail(FROM, TO, msg.as_string())
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

def check_metadata_lock():
    try:
        connection = pymysql.connect(host=HOST,
                                     user=USER,
                                     password=PASSWORD)
        with connection.cursor() as cursor:
            cursor.execute(QUERY)
            result = cursor.fetchall()
            for row in result:
                if "table metadata lock" in row:
                    print("Metadata table lock detected.")
                    send_email()
                    return
        print("No metadata locks detected.")
    except Exception as e:
        print(f"Failed to connect to MySQL: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    check_metadata_lock()
