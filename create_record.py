from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from datetime import datetime, timedelta
import random

CONNECTION_STRING = "mongodb+srv://pinjuc:<encoded password>@cluster0.xwdztln.mongodb.net/test"
client = MongoClient(CONNECTION_STRING)
wc_majority = WriteConcern("majority", wtimeout=1000)

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 2, 15)
time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days
random_number_of_days = random.randrange(days_between_dates)
random_date = start_date + timedelta(days=random_number_of_days)

# Create collections.
client.get_database("transaction", write_concern=wc_majority).account.insert_one({
        "account_id": random.randint(1000000, 9999999),
        "total_amount": random.randint(100, 10000), # initial amount in an account
        "transaction_count": 0,  # initial transaction counts
})

from datetime import datetime, timedelta
import random

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 2, 15)

time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days

account = client.transaction.account
records = []
for i in range(999999):  # generate data
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    record = {
        "account_id": random.randint(10000, 99999),
        "total_amount": random.randint(100, 10000), # initial amount in an account
        "transaction_count": 0,  # initial transaction counts
    }
    records.append(record)
for i in records:
    account.insert_one(i)
