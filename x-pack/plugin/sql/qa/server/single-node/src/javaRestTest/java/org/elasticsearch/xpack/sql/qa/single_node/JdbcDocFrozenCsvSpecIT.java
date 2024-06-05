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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.DataLoader;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcAssert;
import org.elasticsearch.xpack.sql.qa.jdbc.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.SqlSpecTestCase;
import org.junit.ClassRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.SpecReader.Parser;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.csvConnection;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.executeCsvQuery;

public class JdbcDocFrozenCsvSpecIT extends SpecBaseIntegrationTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = SqlTestCluster.getCluster(true);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private final CsvTestCase testCase;

    @Override
    protected String indexName() {
        return "library";
    }

    @Override
    protected void loadDataset(RestClient client) throws Exception {
        DataLoader.loadDocsDatasetIntoEs(client, true);
    }

    @ParametersFactory(shuffle = false, argumentFormatting = SqlSpecTestCase.PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/docs/docs-frozen.csv-spec", parser);
    }

    public JdbcDocFrozenCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber);
        this.testCase = testCase;
    }

    @Override
    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;

        JdbcAssert.assertResultSets(expected, elastic, log, true, true);
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
