# Learn Fauna a Hands on Approach 
In this series I will take you through the examples I used to get a handle on using FaunaDB. These examples are available in a number of languages include Python, Java, Scala and Go at this time. As you can see here: https://fauna.com/documentation/reference, Fauna supports a variety of languages and if you have a favorite, perhaps you could recreate these examples and share them back.


## Prerequisites
The early examples will be based on a local "Developers" copy of Fauna. This is a single node that is quite easy to work with as it only requires a JDK v1.8 or newer be installed on the host machine. The download can be found [here](https://fauna.com/releases). Download and extract to a directory of your choice. Once in that directory, execute the command below and the outlook should look like this.

```
$ java -jar faunadb-developer.jar

FaunaDB Developer Edition 2.5.1-9845cf2
=======================================
Starting...
No configuration file specified; loading defaults...
Data path: ./data
Temp path: ./data/tmp
FaunaDB is ready.
API endpoint: 127.0.0.1:8443

```
An alternative is to use the FaunaDB Cloud. You can follow the instructions [here](https://fauna.com/serverless) and create a developers account. We will cover how to alter the endpoints later if you choose to go this route.

(Optional) It is also very instructive to use the fauna-dashboard to examine the results of the code examples. You can get a copy of the dashboard from GitHub [here](https://github.com/fauna/dashboard). This requires that you have npm installed. The additional instructions needed to run the dashboard are contained in the "README.md" file.

These examples were developed using the various version of the JetBrains tools including IntelliJ. If you use these tools you should be able to import the projects directly.

## Lesson1 - Connect and Create a DB
Introduced in this example is a connection to the FaunaDb using the "admin" client. We also use this client to create and delete a database. 

## Lesson2 - Connect, Create DB, Schema, Index and perform basic CRUD activities.
Securely connect to a specific DB and build out a simple ledger schema including an index that allows us to access records by id. We create a record, read it, update it, read it, and then delete the record. So simple CRUD activity.
Of note, notice that DB creation takes advantage of the ability to include logic in a query request. In this case we are checking the existence of the database before creating and deleting if necessary.

## Lesson3 - Writing and Reading data
Deeper dive into query patterns using indexes. Specifically we add a new index type using values as opposed to terms. This will allow us to perform range style queries. Many of the examples take advantage of that. They also show many more examples of using the various composite commands including mapping functions within the client query.