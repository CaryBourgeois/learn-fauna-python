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

 #
 # These are the required imports for Fauna.
 #
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
    print('DB {0} created: {1}'.format(db_name, res))

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

def create_schema(client):
    #
    # Create an class to hold customers
    #
    res = client.query(
        q.create_class({"name": "customers"})
    )
    print('Create \'customer\' class: {0}'.format(res))

    #
    # Create an index to access customer records by id
    #
    res = client.query(
        q.create_index({
            "name": "customer_by_id",
            "source": q.class_("customers"),
            "unique": True,
            "terms": {"field": ["data", "id"]}
        })
    )
    print('Create \'customer_by_id\' index: {0}'.format(res))

def create_customer(client, cust_id, balance):
    #
    # Create a customer (record)
    #
    res = client.query(
        q.create(q.class_("customers"), {"data": {"id": cust_id, "balance": balance}})
    )
    print('Create \'customer\' {0}: {1}'.format(cust_id, res))

def read_customer(client, cust_id):
    #
    # Read the customer we just created
    #
    res = client.query(
        q.select("data", q.get(q.match(q.index("customer_by_id"), cust_id)))
    )
    print('Read \'customer\' {0}: {1}'.format(cust_id, res))

def update_customer(client, cust_id, new_balance):
    #
    # Update the customer we just created
    #
    balance = 200.0
    res = client.query(
        q.update(
            q.select("ref", q.get(q.match(q.index("customer_by_id"), cust_id))),
            {"data": {"balance": new_balance}}
        )
    )
    print('Update \'customer\' {0}: {1}'.format(cust_id, res))

def delete_customer(client, cust_id):
    #
    # Delete the customer
    #
    res = client.query(
        q.delete(q.select("ref", q.get(q.match(q.index("customer_by_id"), cust_id))))
    )
    print('Delete \'customer\' {0}: {1}'.format(cust_id, res))


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

    create_schema(client)

    cust_id = 0
    balance = 100.0
    create_customer(client, cust_id, balance)

    read_customer(client, cust_id)

    new_balance = 200.0
    update_customer(client, cust_id, new_balance)

    read_customer(client, cust_id)

    delete_customer(client, cust_id)

if __name__ == "__main__":
    main(sys.argv)
