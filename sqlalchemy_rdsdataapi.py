from sqlalchemy.engine.default import DefaultDialect
try:
    from urllib.parse import urlparse, unquote # python 3
except ImportError:
    from urlparse import urlparse, unquote # python 2


class RdsDataApiDialect(DefaultDialect):
    name = 'rdsdataapi'
    driver = 'rdsdataapi'

    @classmethod
    def dbapi(cls):
        return __import__("rdsdataapi")

    def create_connect_args(self, url):
        kwargs = {}
        if url.database:
            kwargs["database"] = url.database
        if url.host:
            kwargs["resource_arn"] = unquote(url.host)
        if url.username:
            kwargs["secret_arn"] = unquote(url.username)
        return [], kwargs

    def _check_unicode_returns(self, connection):
        return True

def register():
    registry.register(
        "rdsdataapi", "sqlalchemy_rdsdataapi", "RdsDataApiDialect"
    )
