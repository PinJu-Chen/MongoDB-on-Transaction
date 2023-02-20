from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo import ReadPreference
from pymongo.write_concern import WriteConcern
from datetime import datetime
import random

CONNECTION_STRING = "mongodb+srv://pinjuc:<encoded password>@<your cluster in MongoDB>"
client = MongoClient(CONNECTION_STRING)
wc_majority = WriteConcern("majority", wtimeout=1000)

# Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.
# reference: https://www.mongodb.com/docs/manual/core/transactions-in-applications/
def callback(session):
    # pass the session to the operations.
    account = session.client.transaction.account

    all_trans_doc_list = list(account.find({}))  # Getting all docs from collection
    for i in range(20):  # range(len(all_trans_doc_list)-1)
        transcation_amount = random.randint(10, 500)  # transfer limit: 10 - 500 USD
        id = str(random.randint(10000, 99999))
        sender = all_trans_doc_list[i]['account_id']
        account.update_many({"account_id": sender}, {"$inc": {"transaction_count": 1}})
        account.update_many({"account_id": sender}, {"$inc": {"total_amount": -transcation_amount}})
        account.update_many({"account_id": sender}, {"$push": {"record": {"transaction_id": "s"+ id,
                                                                          "transaction_date": datetime.now(),
                                                                          "amount": -transcation_amount,
                                                                          "to": all_trans_doc_list[i+1]['account_id']}}})
        i += 1
        receiver = all_trans_doc_list[i]['account_id']
        account.update_many({"account_id": receiver}, {"$inc": {"transaction_count": 1}})
        account.update_many({"account_id": receiver}, {"$inc": {"total_amount": transcation_amount}})
        account.update_many({"account_id": receiver}, {"$push": {"record": {"transaction_id": "r" + id,
                                                                            "transaction_date": datetime.now(),
                                                                            "amount": transcation_amount,
                                                                            "from": all_trans_doc_list[i-1]['account_id']}}})



# Step 2: Start a client session.
with client.start_session() as session:
    # Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
    session.with_transaction(
        callback,
        read_concern=ReadConcern("local"),
        write_concern=wc_majority,
        read_preference=ReadPreference.PRIMARY,
    )
