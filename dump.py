#!/usr/bin/python

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import csv

# ----- CHANGE IT  ----- #
"""
    This script will export data from cassandra depends on the limit of the buffersize
    If the target file already exists, it will append row by row to the end of the file.
    If the target file doesn't exist, it will create a new file, and write to it.

    Default is no column header.
    * If you want header, please add column header manually in columnHeader argument.
    * If you specify column header, it will overwrite the old file (if the target filename doesn't change).
"""

username = ""
password = ""
query = "select * from keyspace.columnfamily"
target = '/path/to/target/directory'
bufferSize = 100000

# example : columnHeader = ["col1", "col2", ...]
columnHeader = []

# ---------------------- #

def appendToCsv(targetFile, line):
    with open(targetFile, 'a+') as file:
        writer = csv.writer(file)
        writer.writerow(line)


# Add Column header
if len(columnHeader) > 0:
    with open(targetFile, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(columnHeader)

# connect a session
auth_provider = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(['cassandra-host-name',], port=9042, auth_provider=auth_provider)
session = cluster.connect()

statement = SimpleStatement(query, fetch_size=bufferSize)
results = session.execute(statement)

for row in results:
    value = [row[i] for i in range(len(row))]
    appendToCsv(target, value)
