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
        })
    ])
    print('Create \'customer_by_id\' index & \'customer_id_filter\' index : {0}'.format(res))

def create_customers(client):
    #
    # Create 20 customer records with ids from 1 to 20
    #
    client.query(
        q.map_(
            lambda id: q.create(q.class_("customers"),
                                {"data": {"id": id, "balance": 100.0}}),
            list(range(1, 21)))
    )

def read_customer(client, cust_id):
    #
    # Read the customer we just created
    #
    res = client.query(
        q.select("data", q.get(q.match(q.index("customer_by_id"), cust_id)))
    )
    print('Read \'customer\' {0}: {1}'.format(cust_id, res))

def read_three_customers(client, cust_id1, cust_id2, cust_id3):
    #
    # Here is a more general use case where we retrieve multiple class references
    # by id and return the actual data underlying them.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(x)),
               q.paginate(
                   q.union(
                       q.match(q.index("customer_by_id"), cust_id1),
                       q.match(q.index("customer_by_id"), cust_id2),
                       q.match(q.index("customer_by_id"), cust_id3)
                   )
               ))
    )
    print('Union specific \'customer\' 1, 3, 8: {0}'.format(res))

def read_list_of_customers(client, cust_list):
    #
    # Finally a much more general use case where we can supply any number of id values
    # and return the data for each.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(x)),
               q.paginate(
                   q.union(
                       q.map_(lambda y: q.match(q.index("customer_by_id"), y), cust_list)
                   )
               )
               )
    )
    print('Union variable \'customer\' {0}: {1}'.format(cust_list, res))

def read_customers_less_than(client, max_cust_id):
    #
    # In this example we use the values based filter 'customer_id_filter'.
    # using this filter we can query by range. This is an example of returning
    # all the values less than(<) or before 5. The keyword 'after' can replace
    # 'before' to yield the expected results.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(q.select(1, x))),
               q.paginate(q.match(q.index("customer_id_filter")), before=[max_cust_id])
               )
    )
    print('Query for id\'s < {0} : {1}'.format(max_cust_id, res))

def read_customers_between(client, min_cust_id, max_cust_id):
    #
    # Extending the previous example to show getting a range between two values.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(q.select(1, x))),
               q.filter_(lambda y: q.lte(min_cust_id, q.select(0, y)),
                         q.paginate(q.match(q.index("customer_id_filter")), before=[max_cust_id])
                         )
               )
    )
    print('Query for id\'s > {0} and < {1} : {2}'.format(min_cust_id, max_cust_id, res))

def read_all_customers(client):
    #
    # Read all the records that we created. This is a more generalized usage of the
    # paginate functionality. Notice the capture and passing of the "after" cursor
    # that is part of the return of the paginate if records are remaining.
    #
    # Use a small'ish page size so that we can demonstrate a paging example.
    #
    # NOTE: after is inclusive of the value.
    #
    pageSize = 8
    cursorPos = None
    while True:
        res = client.query(
            q.map_(lambda x: q.select("data", q.get(q.select(1,x))),
                       q.paginate(q.match(q.index("customer_id_filter")), after=cursorPos, size=pageSize)
                       )
        )

        for i in res['data']:
            print(i)

        if 'after' in res:
            cursorPos = res['after']
        else:
            break


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

    create_customers(client)

    read_customer(client, 1)

    read_three_customers(client, 1, 3, 8)

    cust_list = [1, 3, 6, 7]
    read_list_of_customers(client, cust_list)

    read_customers_less_than(client, 5)

    read_customers_between(client, 5, 11)

    read_all_customers(client)

if __name__ == "__main__":
    main(sys.argv)
