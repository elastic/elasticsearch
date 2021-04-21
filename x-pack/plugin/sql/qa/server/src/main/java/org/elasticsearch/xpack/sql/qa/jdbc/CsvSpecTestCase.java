/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.csvConnection;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.executeCsvQuery;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.specParser;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with hard coded result sets.
 */
public abstract class CsvSpecTestCase extends SpecBaseIntegrationTestCase {
    private final CsvTestCase testCase;

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = TestUtils.classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found (" + urls.size() + ") " + urls.toString(), urls.size() >= 23);
        return readScriptSpec(urls, specParser());
    }

    public CsvSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber);
        this.testCase = testCase;
    }

    @Override
    protected final void doTest() throws Throwable {
        // Run the time tests always in UTC
        // TODO: https://github.com/elastic/elasticsearch/issues/40779
        try (Connection csv = csvConnection(testCase); Connection es = esJdbc()) {
            executeAndAssert(csv, es);
        }
    }

    @Override
    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;
        JdbcAssert.assertResultSets(expected, elastic, log, false, true);
    }

    private void executeAndAssert(Connection csv, Connection es) throws SQLException {
        // pass the testName as table for debugging purposes (in case the underlying reader is missing)
        ResultSet expected = executeCsvQuery(csv, testName);
        ResultSet elasticResults = executeJdbcQuery(es, testCase.query);
        assertResults(expected, elasticResults);
    }
}
