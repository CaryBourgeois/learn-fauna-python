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

def main(argv):
    #
    # Create an admin client. This is the client we will use to create the database.
    #
    # If you are using the the FaunaDB-Cloud you will need to replace the value of the
    # 'secret' in the command below with your "secret".
    #
    adminClient = FaunaClient(secret="secret", domain="127.0.0.1", scheme="http", port=8443)

    #
    # The code below creates the Database that will be used for this example. Please note that
    # the existence of the database is evaluated, deleted if it exists and recreated with a single
    # call to the Fauna DB.
    #
    dbName = "LedgerExample"

    res = adminClient.query(
        q.if_(
            q.exists(q.database(dbName)),
            [q.delete(q.database(dbName)), q.create_database({"name": dbName})],
            q.create_database({"name": dbName}))
    )
    print('DB {0} created: {1}'.format(dbName, res))

    #
    # Create a key specific to the database we just created. We will use this to
    # create a new client we will use in the remainder of the examples.
    #
    res = adminClient.query(q.select(["secret"], q.create_key({"database": q.database(dbName), "role": "server"})))
    print('DB {0} secret: {1}'.format(dbName, res))

    #
    # Create the DB specific DB client using the DB specific key just created.
    #
    client = FaunaClient(secret=res, domain="127.0.0.1", scheme="http", port=8443)

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

    #
    # Create 20 customer records with ids from 1 to 20
    #
    client.query(
        q.map_(
            lambda id: q.create(q.class_("customers"),
                                {"data": {"id": id, "balance": q.multiply(id, 10)}}),
            list(range(1, 21)))
    )

    #
    # Read a single record and return the data it holds
    # We saw this from the previous Lesson code
    #
    custID = 1
    res = client.query(
        q.select("data", q.get(q.match(q.index("customer_by_id"), custID)))
    )
    print('Read \'customer\' {0}: {1}'.format(custID, res))

    #
    # Here is a more general use case where we retrieve multiple class references
    # by id and return the actual data underlying them.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(x)),
                   q.paginate(
                       q.union(
                           q.match(q.index("customer_by_id"), 1),
                           q.match(q.index("customer_by_id"), 3),
                           q.match(q.index("customer_by_id"), 8)
                       )
                   ))
    )
    print('Union specific \'customer\' 1, 3, 8: {0}'.format(res))

    #
    # Finally a much more general use case where we can supply any number of id values
    # and return the data for each.
    #
    custIDs = [1, 3, 6, 7]
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(x)),
                   q.paginate(
                       q.union(
                           q.map_(lambda y: q.match(q.index("customer_by_id"), y), custIDs)
                       )
                   )
                   )
    )
    print('Union variable \'customer\' {0}: {1}'.format(custIDs, res))

    #
    # In this example we use the values based filter 'customer_id_filter'.
    # using this filter we can query by range. This is an example of returning
    # all the values less than(<) or before 5. The keyword 'after' can replace
    # 'before' to yield the expected results.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(q.select(1, x))),
                   q.paginate(q.match(q.index("customer_id_filter")), before=[5])
                   )
    )
    print('Query for id\'s < 5 : {0}'.format(res))

    #
    # Extending the previous example to show getting a range between two values.
    #
    res = client.query(
        q.map_(lambda x: q.select("data", q.get(q.select(1, x))),
                   q.filter_(lambda y: q.lte(5, q.select(0, y)),
                                 q.paginate(q.match(q.index("customer_id_filter")), before=[11])
                                 )
                   )
    )
    print('Query for id\'s > 5 and < 11 : {0}'.format(res))

    #
    # Read all the records that we created.
    # Use a small'ish page size so that we can demonstrate a paging example.
    #
    # NOTE: after is inclusive of the value.
    #
    cursorPos = 1
    pageSize = 8
    while True:
        res = client.query(
            q.map_(lambda x: q.select("data", q.get(q.select(1,x))),
                       q.paginate(q.match(q.index("customer_id_filter")), after=cursorPos, size=pageSize)
                       )
        )
        print('Page through id\'s >= {0} and < {1} : {2}'.format(cursorPos, cursorPos+pageSize, res))
        for i in res['data']:
            print(i)

        if 'after' in res:
            cursorPos = cursorPos + pageSize
        else:
            break

if __name__ == "__main__":
    main(sys.argv)
