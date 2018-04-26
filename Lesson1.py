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
    # 'secret' in the command below with your "secret". As is, this should work with a
    # local developer version of FaunaDB either the Jar file or Docker image based option
    #
    adminClient = FaunaClient(secret="secret", domain="127.0.0.1", scheme="http", port=8443)

    dbName = "TestDB"

    #
    # Call to Create the database
    #
    res = adminClient.query(
        q.create_database({"name": dbName})
    )
    print('DB {0} created: {1}'.format(dbName, res))

    #
    # Call to check to see if database exists and to delete it id it does.
    #
    res = adminClient.query(
        q.if_(q.exists(q.database(dbName)), q.delete(q.database(dbName)), True)
    )
    print('DB {0} deleted: {1}'.format(dbName, res))


if __name__ == "__main__":
    main(sys.argv)