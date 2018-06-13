#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import pprint
from uuid import uuid4
from random import randint
from faunadb.client import FaunaClient
from faunadb import query as q

def create_database(scheme, domain, port, secret, db_name):
    #
    # Create an admin client. This is the client we will use to create the database.
    #
    # If you are using the the FaunaDB-Cloud you will need to replace the value of the
    # 'secret' in the command below with your "secret".
    #
    adminClient = FaunaClient(secret=secret, domain=domain, scheme=scheme, port=port)
    print("Connected to FaunaDB as admin!")


    #
    # The code below creates the Database that will be used for this example. Please note that
    # the existence of the database is evaluated, deleted if it exists and recreated with a single
    # call to the Fauna DB.
    #
    res = adminClient.query(
        q.if_(
            q.exists(q.database(db_name)),
            [q.delete(q.database(db_name)), q.create_database({"name": db_name})],
            q.create_database({"name": db_name}))
    )
    print('DB {0} created:'.format(db_name))
    pprint.pprint(res)

    #
    # Create a key specific to the database we just created. We will use this to
    # create a new client we will use in the remainder of the examples.
    #
    res = adminClient.query(q.select(["secret"], q.create_key({"database": q.database(db_name), "role": "server"})))
    print('DB {0} secret: {1}'.format(db_name, res))

    return res

def create_db_client(scheme, domain, port, secret):
    #
    # Create the DB specific DB client using the DB specific key just created.
    #
    client = FaunaClient(secret=secret, domain="127.0.0.1", scheme="http", port=8443)

    return client

def create_classes(client):
    #
    # Create an class to hold customers and transactions
    #
    res = client.query(
        [
            q.create_class({"name": "customers"}),
            q.create_class({"name": "transactions"})
        ]
    )
    print('Create \'customer\' and \'transaction\' classes.')
    pprint.pprint(res)

def create_indices(client):
    #
    # Create two indexes here. The first index is to query customers when you know specific id's.
    # The second is used to query customers by range. Examples of each type of query are presented
    # below.
    #
    res = client.query([
        q.create_index({
            "name": "customer_by_id",
            "source": q.class_("customers"),
            "unique": True,
            "terms": {"field": ["data", "id"]}
        }),
        q.create_index({
            "name": "customer_id_filter",
            "source": q.class_("customers"),
            "unique": True,
            "values": [{"field": ["data", "id"]}, {"field": ["ref"]}]
        }),
        q.create_index({
            "name": "transaction_uuid_filter",
            "source": q.class_("transactions"),
            "unique": True,
            "values": [{"field": ["data", "id"]}, {"field": ["ref"]}]
        })
    ])
    print('Create \'customer_by_id\', \'customer_id_filter\' and \'transaction_uuid_filter\' indices')
    pprint.pprint(res)

def create_customer(client, cust_id, balance):
    #
    # Create a customer (record) using a python dictionary
    #
    customer = {"id": cust_id, "balance": balance}
    res = client.query(
        q.create(q.class_("customers"), {"data": customer})
    )
    print('Create \'customer\' {0}:'.format(cust_id))
    pprint.pprint(res)

def create_customers(client, num_customers, init_balance):
    #
    # Create 'numCustomers' customer records with ids from 1 to 'numCustomers'
    #
    # in this example we use a list of dictionary items to build the 'data'
    # payload for the create function in Fauna
    #
    # THe return is a list of Fauna RefV that can be used to access records
    # directly.
    #
    cust_list = []
    for cust_id in range(1, num_customers + 1):
        customer = {"id": cust_id, "balance": init_balance}
        cust_list.append(customer)

    res = client.query(
        q.map_(
            lambda customer: q.create(q.class_("customers"),
                                {"data": customer}),
            cust_list)
    )
    print('Create {0} customers:'.format(num_customers))

    cust_refs = []
    for cust_ref in res:
        cust_refs.append(cust_ref['ref'])

    return cust_refs

def sum_customer_balanaces(client, cust_refs):
    #
    # This is going to take the customer references that were created during the
    # createCustomers routine and aggregate all the balances for them. We could so this,
    # and probably would, with class index. In this case we want to take this approach to show
    # how to use references.
    #
    balance_sum = 0

    res = client.query(
        q.map_(
            lambda cust_ref: q.select("data", q.get(cust_ref)),
            cust_refs)
    )

    for customer in res:
        balance_sum = balance_sum + customer['balance']

    print('Customer Balance Sum: {0}'.format(balance_sum))

    return balance_sum

def create_transaction(client, num_customers, max_txn_amount):
    #
    # This method is going to create a random transaction that moves a random amount
    # from a source customer to a destination customer. Prior to committing the transaction
    # a check will be performed to insure that the source customer has a sufficient balance
    # to cover the amount and not go into an overdrawn state.
    #
    uuid = uuid4().urn[9:]

    source_id = randint(1, num_customers)
    dest_id = randint(1, num_customers)
    while dest_id == source_id:
        dest_id = randint(1, num_customers)
    amount = randint(1, max_txn_amount)

    transaction = {"uuid": uuid, "sourceCust": source_id, "destCust": dest_id , "amount": amount}

    res = client.query(
        q.let(
            {"source_customer": q.get(q.match(q.index("customer_by_id"), source_id)),
             "dest_customer": q.get(q.match(q.index("customer_by_id"), dest_id))},
            q.let(
                {"source_balance": q.select(["data", "balance"], q.var("source_customer")),
                 "dest_balance": q.select(["data", "balance"], q.var("dest_customer"))},
                q.let(
                    {"new_source_balance": q.subtract(q.var("source_balance"), amount),
                     "new_dest_balance": q.add(q.var("dest_balance"), amount)},
                    q.if_(
                        q.gte(q.var("new_source_balance"), 0),
                        q.do(
                            q.create(q.class_("transactions"), {"data": transaction}),
                            q.update(q.select("ref", q.var("source_customer")),
                                     {"data": {"txnID": uuid, "balance": q.var("new_source_balance")}}),
                            q.update(q.select("ref", q.var("dest_customer")),
                                     {"data": {"txnID": uuid, "balance": q.var("new_dest_balance")}})
                        ),
                        "Error. Insufficient funds."
                    )
                )
            )
        )
    )


def main(argv):
    #
    # Set up the connection information for FaunaDB running locally as the
    # developer version
    #
    scheme = "http"
    domain = "127.0.0.1"
    port = "8443"
    secret = "secret"

    db_name = "LedgerExample"

    db_secret = create_database(scheme, domain, port, secret, db_name)

    client = create_db_client(scheme, domain, port, db_secret)

    create_classes(client)

    create_indices(client)

    # create_customer(client, 0, 101)

    cust_refs = create_customers(client, 50, 100)

    sum_customer_balanaces(client, cust_refs)

    for i in range(0, 1000):
        create_transaction(client, 50, 10)

    sum_customer_balanaces(client, cust_refs)


if __name__ == "__main__":
    main(sys.argv)
