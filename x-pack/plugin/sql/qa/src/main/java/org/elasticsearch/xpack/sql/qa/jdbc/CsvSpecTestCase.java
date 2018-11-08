/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
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
        Parser parser = specParser();
        List<Object[]> tests = new ArrayList<>();
        tests.addAll(readScriptSpec("/select.csv-spec", parser));
        tests.addAll(readScriptSpec("/command.csv-spec", parser));
        tests.addAll(readScriptSpec("/fulltext.csv-spec", parser));
        tests.addAll(readScriptSpec("/agg.csv-spec", parser));
        tests.addAll(readScriptSpec("/columns.csv-spec", parser));
        tests.addAll(readScriptSpec("/datetime.csv-spec", parser));
        tests.addAll(readScriptSpec("/alias.csv-spec", parser));
        tests.addAll(readScriptSpec("/null.csv-spec", parser));
        tests.addAll(readScriptSpec("/nested.csv-spec", parser));
        tests.addAll(readScriptSpec("/functions.csv-spec", parser));
        tests.addAll(readScriptSpec("/math.csv-spec", parser));
        return tests;
    }

    public CsvSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber);
        this.testCase = testCase;
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
