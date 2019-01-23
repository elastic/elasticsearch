/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.ClassRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@TestLogging(JdbcTestUtils.SQL_TRACE)
public class DebugSqlSpec extends SqlSpecTestCase {

    @ClassRule
    public static final EmbeddedSqlServer EMBEDDED_SERVER = new EmbeddedSqlServer();

    public DebugSqlSpec(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber, query);
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/datetime.sql-spec", parser);
    }

    @Override
    public Connection esJdbc() throws SQLException {
        // use the same random path as the rest of the tests
        randomBoolean();
        return EMBEDDED_SERVER.connection(connectionProperties());
    }

    @Override
    protected boolean logEsResultSet() {
        return true;
    }

    @Override
    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;

        //
        // uncomment this to printout the result set and create new CSV tests
        //
        JdbcTestUtils.logLikeCLI(elastic, log);
        //JdbcAssert.assertResultSets(expected, elastic, log);
    }
}
