import numbers
import boto3
from datetime import date, datetime, time
from time import localtime

client = boto3.client("rds-data")


__all__ = [
    # constants
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety"
    # exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # connection func
    "connect",
    # types
]

# DBAPI 2 required constructor -- https://www.python.org/dev/peps/pep-0249/#constructors
def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


# DBAPI 2 required constants -- https://www.python.org/dev/peps/pep-0249/#globals
apilevel = "2.0"
threadsafety = 1  # just being safe.. miight be 3 actually!
paramstyle = "named"  # seems RDS Data API uses this, regardless of mysql or postgresql


# DBAPI 2 required exceptions -- https://www.python.org/dev/peps/pep-0249/#exceptions
class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# https://www.python.org/dev/peps/pep-0249/#connection-objects
class Connection:
    def __init__(self, resource_arn, secret_arn, database):
        self.resource_arn = resource_arn
        self.secret_arn = secret_arn
        self.database = database
        self.transaction_id = None

    def close(self):
        pass

    def commit(self):
        if self.transaction_id is None:
            return
        client.commit_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=self.transaction_id,
        )
        self.transaction_id = None

    def rollback(self):
        if self.transaction_id is None:
            return
        client.rollback_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=self.transaction_id,
        )
        self.transaction_id = None

    def cursor(self):
        return Cursor(self)

    def transaction(self):
        """
        start a transaction
        unless using this or cursor as a contextmanager, autocommit is used
        """
        self.transaction_id = client.begin_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            database=self.database,
        )["transactionId"]

    # Warnings as connection peroperties
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError


# https://www.python.org/dev/peps/pep-0249/#cursor-objects
class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self.arraysize = 1
        self._rowcount = -1
        self.result = None

    @property
    def connection(self):
        return self._connection

    @property
    def description(self):
        if self.result is None or "columnMetadata" not in self.result:
            return None
        return [
            (col["name"], col["typeName"], None, None, None, None, None)
            for col in self.result["columnMetadata"]
        ]

    @property
    def rowcount(self):
        return self._rowcount

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        if parameters is None:
            parameters = {}
        boto_params = dict(
            sql=operation,
            parameters=[
                {"name": key, "value": _aws_type(value)}
                for key, value in parameters.items()
            ],
            resourceArn=self.connection.resource_arn,
            secretArn=self.connection.secret_arn,
            database=self.connection.database,
            includeResultMetadata=True,
        )
        if self.connection.transaction_id is not None:
            boto_params["transactionId"] = self.connection.transaction_id
        try:
            self.result = client.execute_statement(**boto_params)
        except Exception as e:
            raise self.connection.Error(e)

    def executemany(self, operation, seq_of_parameters):
        boto_params = dict(
            sql=operation,
            parameterSets=[
                [
                    {"name": key, "value": _aws_type(value)}
                    for key, value in parameters.items()
                ]
                for parameters in seq_of_parameters
            ],
            resourceArn=self.connection.resource_arn,
            secretArn=self.connection.secret_arn,
            database=self.connection.database,
        )
        if self.connection.transaction_id is not None:
            boto_params["transactionId"] = self.connection.transaction_id
        try:
            client.batch_execute_statement(**boto_params)
        except Exception as e:
            raise self.connection.Error(e)

    def fetchone(self):
        if self.result is None or "records" not in self.result:
            raise self.connection.Error("No result to fetch!")
        try:
            return [_python_type(col) for col in self.result["records"].pop(0)]
        except IndexError:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        if self.result is None or "records" not in self.result:
            raise self.connection.Error("No result to fetch!")
        result = []
        for _ in range(size):
            try:
                result.append(
                    [_python_type(col) for col in self.result["records"].pop(0)]
                )
            except IndexError:
                pass
        return result

    def fetchall(self):
        if self.result is None or "records" not in self.result:
            raise self.connection.Error("No result to fetch!")
        try:
            return [
                [_python_type(col) for col in record]
                for record in self.result["records"]
            ]
        finally:
            self.result["records"] = []

    def nextset(self):
        raise NotSupportedError

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    # context manager use creates transactions
    def __enter__(self):
        self.connection.transaction()
        return self

    def __exit__(self, type_, value, traceback):
        if type_:
            self.connection.rollback()
        else:
            self.connection.commit()


# DBAPI 2 required types -- https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
def Date(year, month, day):
    return date(year, month, day)


def Time(hour, minute, second):
    return time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    return DATETIME(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*localtime(ticks)[:6])


def Binary(value):
    return value


STRING = "varchar"
BINARY = bytes
NUMBER = numbers.Integral
DATETIME = datetime
ROWID = int


# Interal utilities
def _python_type(value_dict):
    if value_dict.get("isNull"):
        return None
    return next(iter(value_dict.values()))


def _aws_type(value):
    # TODO - how to detect blob in python 2?
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, bytes):
        return {"blobValue": value}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, numbers.Integral):
        return {"longValue": value}
    if value is None:
        return {"isNull": true}
