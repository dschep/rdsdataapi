import dbapi20
import rdsdataapi
import boto3

ssm = boto3.client("ssm")

params = ssm.get_parameters(
    Names=["/rdsdataapi/resource_arn", "/rdsdataapi/secret_arn", "/rdsdataapi/database"]
)["Parameters"]


class RdsDataApiTest(dbapi20.DatabaseAPI20Test):
    driver = rdsdataapi
    connect_args = ()
    connect_kw_args = {
        param["Name"].rsplit("/", 1)[1]: param["Value"] for param in params
    }

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

    def _test_describe_type(self, db_type, dbapi20_type):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %sbooze (name %s)" % (self.table_prefix, db_type))
            self.assertEqual(
                cur.description,
                None,
                "cursor.description should be none after executing a "
                "statement that can return no rows (such as DDL)",
            )
            cur.execute("select name from %sbooze" % self.table_prefix)
            self.assertEqual(
                len(cur.description), 1, "cursor.description describes too many columns"
            )
            self.assertEqual(
                len(cur.description[0]),
                7,
                "cursor.description[x] tuples must have 7 elements",
            )
            self.assertEqual(
                cur.description[0][0].lower(),
                "name",
                "cursor.description[x][0] must return column name",
            )
            self.assertTrue(
                cur.description[0][1] == dbapi20_type,
                "cursor.description[x][1] must return column type. Got %r"
                % cur.description[0][1],
            )

            # Make sure self.description gets reset
            self.executeDDL2(cur)
            self.assertEqual(
                cur.description,
                None,
                "cursor.description not being set to None when executing "
                "no-result statements (eg. DDL)",
            )
        finally:
            con.close()

    def test_describe_float(self):
        self._test_describe_type("FLOAT", self.driver.NUMBER)

    def test_describe_double(self):
        self._test_describe_type("DOUBLE PRECISION", self.driver.NUMBER)

    def test_describe_int(self):
        self._test_describe_type("INT", self.driver.NUMBER)

    def test_describe_bigint(self):
        self._test_describe_type("BIGINT", self.driver.NUMBER)

    def test_describe_text(self):
        self._test_describe_type("TEXT", self.driver.STRING)

    def test_describe_bytea(self):
        self._test_describe_type("bytea", self.driver.BINARY)

    def test_describe_timestamp(self):
        self._test_describe_type("timestamp", self.driver.DATETIME)
