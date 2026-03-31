/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.APPROXIMATION_V6;

/**
 * Tests for query approximation generated from existing CSV tests.
 * <p>
 * To be eligible for this test, a CSV test must contain STATS (since query
 * approximation is only supported for queries containing STATS).
 */
public abstract class GenerativeApproximationRestTest extends EsqlSpecTestCase {
    public GenerativeApproximationRestTest(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected void doTest() throws Throwable {
        // The query from the test may not be supported for query approximation.
        // Therefore, there may be an "approximation is not supported" warning.
        // For simplicity, we just allow all warnings here.
        testCase.allowAllWarnings();

        doTest("SET approximation=true; " + testCase.query);
    }

    @Override
    protected void assertResults(
        CsvTestUtils.ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        Logger logger
    ) {
        // Query approximation may add additional columns starting with "_approximation_".
        // Since these columns are not present in the original CSV test, they are removed.
        for (int col = 0; col < actualColumns.size(); col++) {
            if (actualColumns.get(col).get("name").startsWith("_approximation_")) {
                actualColumns.remove(col);
                final int colFinal = col;
                actualValues.forEach(row -> row.remove(colFinal));
                col--;
            }
        }

        // All CST tests (except for the approximation tests, which are skipped) have small data,
        // therefore the exact result should be returned even with approximation enabled.
        super.assertResults(expected, actualColumns, actualValues, logger);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        assumeFalse("No approximation tests", testCase.requiredCapabilities.contains(APPROXIMATION_V6.capabilityName()));
        assumeTrue("Test must contain STATS to be included in approximation tests", testCase.query.toLowerCase().contains("stats"));
    }
}
