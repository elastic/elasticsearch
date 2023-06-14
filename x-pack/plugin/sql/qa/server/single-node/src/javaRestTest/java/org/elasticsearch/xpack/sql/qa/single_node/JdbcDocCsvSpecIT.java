/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xpack.sql.qa.jdbc.DataLoader;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcAssert;
import org.elasticsearch.xpack.sql.qa.jdbc.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.SqlSpecTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.SpecReader.Parser;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.csvConnection;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.executeCsvQuery;

/**
 * CSV test specification for DOC examples.
 * While we could use the existing tests, their purpose is to test corner-cases which
 * gets reflected in the dataset structure.
 * The doc tests while redundant, try to be expressive first and foremost and sometimes
 * the dataset isn't exactly convenient.
 *
 * Also looking around for the tests across the test files isn't trivial.
 *
 * That's not to say the two cannot be merged however that felt like too much of an effort
 * at this stage and, to not keep things stalling, started with this approach.
 */
public class JdbcDocCsvSpecIT extends SpecBaseIntegrationTestCase {

    private final CsvTestCase testCase;

    @Override
    protected String indexName() {
        return "library";
    }

    @Override
    protected void loadDataset(RestClient client) throws Exception {
        DataLoader.loadDocsDatasetIntoEs(client);
    }

    @ParametersFactory(shuffle = false, argumentFormatting = SqlSpecTestCase.PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/docs/docs.csv-spec", parser);
    }

    public JdbcDocCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber);
        this.testCase = testCase;
    }

    @Override
    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;

        //
        // uncomment this to printout the result set and create new CSV tests
        //
        // JdbcTestUtils.logLikeCLI(elastic, log);
        JdbcAssert.assertResultSets(expected, elastic, log, true, true);
    }

    @Override
    protected boolean logEsResultSet() {
        return false;
    }

    @Override
    protected final void doTest() throws Throwable {
        try (Connection csv = csvConnection(testCase); Connection es = esJdbc()) {

            // pass the testName as table for debugging purposes (in case the underlying reader is missing)
            ResultSet expected = executeCsvQuery(csv, testName);
            ResultSet elasticResults = executeJdbcQuery(es, testCase.query);
            assertResults(expected, elasticResults);
        }
    }
}
