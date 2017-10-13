/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@TestLogging(JdbcTestUtils.SQL_TRACE)
public abstract class DebugCsvSpec extends CsvSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = SqlSpecTestCase.PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/debug.csv-spec", parser);
    }

    public DebugCsvSpec(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }

    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;

        //
        // uncomment this to printout the result set and create new CSV tests
        //
        //JdbcTestUtils.logResultSetMetadata(elastic, log);
        //JdbcTestUtils.logResultSetData(elastic, log);
        JdbcAssert.assertResultSets(expected, elastic, log);
    }

    @Override
    protected boolean logEsResultSet() {
        return true;
    }
}