# ABANDONED
I never had time to get this past a proof of concept stage, check out:
https://github.com/chanzuckerberg/sqlalchemy-aurora-data-api

## DB-API 2.0 driver & SqlAlchemy dialect for the AWS RDS Data API

[![Test Status](https://github.com/dschep/rdsdataapi/workflows/Test/badge.svg)](https://github.com/dschep/rdsdataapi/actions)
[![PyPI](https://img.shields.io/pypi/v/rdsdataapi)](https://pypi.org/project/rdsdataapi/)

**NOTE: This is currently ALPHA quality software and has not been thoroughly tested yet**

The RDS Data API allows use of MySQL and PostgreSQL RDS databases via an HTTP API, making it ideal
for use in AWS Lambda because it allow syou to use RDS without running your lambda in a VPC. This
library wraps that API in both a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/) driver and
[SqlAlchemy](https://www.sqlalchemy.org/) dialect allowing you to tap into the vast python
ecosystem for working with relational databases.


### Installation
```
pip install rdsdataapi
```


### Usage

To use the DB-API 2.0 interface:
```python
from rdsdataapi import connect
con = connect(
    resource_arn="arn:aws:rds:us-east-1:490103061721:cluster:database-2",
    secret_arn="arn:aws:secretsmanager:us-east-1:490103061721:secret:pgdb-gIucWr",
    database="postgres",
)
cur = con.cursor()
cur.execute("select :foo as bar", {"foo": "foobar"})
result = cur.fetchall()
```

Or via SqlAlchemy:
```python
engine = create_engine(
    'rdsdataapi://',
    connect_args=dict(
        resource_arn="arn:aws:rds:us-east-1:490103061721:cluster:database-2",
        secret_arn="arn:aws:secretsmanager:us-east-1:490103061721:secret:pgdb-gIucWr",
        database="postgres",
    )
)
with engine.connect() as con:
    result = con.execute("select :foo as bar", foo="foobar")
```
