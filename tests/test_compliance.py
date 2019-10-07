import dbapi20
import rdsdataapi


class RdsDataApiTest(dbapi20.DatabaseAPI20Test):
    driver = rdsdataapi
    connect_args = ()
    connect_kw_args = dict(
        resource_arn="arn:aws:rds:us-east-1:490103061721:cluster:database-2",
        secret_arn="arn:aws:secretsmanager:us-east-1:490103061721:secret:pgdb-gIucWr",
        database="postgres",
    )

    def setUp(self):
        self.tearDown()  # builtin teardown cleans up, do it before too.

    def test_close(self):
        """
        Override test bc expecting errors from calls to close is over kill since there is no
        persistent connection and I see no point in faking it
        https://github.com/baztian/dbapi-compliance/blob/c259a6ab3a90db89b40936a9a36745a0a6383d5e/dbapi20.py#L327-L340
        """
        con = self._connect()
        try:
            cur = con.cursor()
        finally:
            con.close()

    def test_non_idempotent_close(self):
        """
        Per the upstream docstring, this test is of questionable utility. and it really doesn't
        mean much in the context of using the Data API since there isn't a peristent connection
        anyway.
        https://github.com/baztian/dbapi-compliance/blob/c259a6ab3a90db89b40936a9a36745a0a6383d5e/dbapi20.py#L346
        """
        pass

    def test_nextset(self):
        """ Not implemented. """
        pass

    def test_setoutputsize(self):
        """ Not implemented. """
        pass
