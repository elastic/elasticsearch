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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.APPROXIMATION_V7;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FIX_SUM_AGG_LONG_OVERFLOW;
import static org.elasticsearch.xpack.esql.approximation.ApproximationPlan.CERTIFIED_COLUMN_PREFIX;
import static org.elasticsearch.xpack.esql.approximation.ApproximationPlan.CONFIDENCE_INTERVAL_COLUMN_PREFIX;
import static org.hamcrest.Matchers.equalTo;

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

        // Sample a huge number of rows, so that exact results are computed.
        doTest("SET approximation={\"rows\":2000000000}; " + testCase.query);
    }

    @Override
    protected void assertResults(
        CsvTestUtils.ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        Logger logger
    ) {
        Map<String, Integer> columnIndex = new HashMap<>();

        // Query approximation may add additional columns starting with "_approximation_".
        // These columns are not present in the original CSV test, so they must be added.
        for (int col = 0; col < actualColumns.size(); col++) {
            String columnName = actualColumns.get(col).get("name");
            columnIndex.put(columnName, col);

            if (columnName.startsWith(CONFIDENCE_INTERVAL_COLUMN_PREFIX) && columnName.endsWith(")")) {
                String originalColumnName = columnName.substring(CONFIDENCE_INTERVAL_COLUMN_PREFIX.length(), columnName.length() - 1);
                int originalColumnIndex = columnIndex.get(originalColumnName);

                // Add confidence interval column and corresponding certified column.
                expected.columnNames().add(columnName);
                expected.columnTypes().add(expected.columnTypes().get(originalColumnIndex));
                String certifiedColumnName = columnName.replace(CONFIDENCE_INTERVAL_COLUMN_PREFIX, CERTIFIED_COLUMN_PREFIX);
                assertThat(
                    "approximation confidence interval column should be followed by a corresponding certified column",
                    actualColumns.get(col + 1).get("name"),
                    equalTo(certifiedColumnName)
                );
                expected.columnNames().add(certifiedColumnName);
                expected.columnTypes().add(CsvTestUtils.Type.BOOLEAN);

                for (int rowIndex = 0; rowIndex < expected.values().size(); rowIndex++) {
                    // The results are always exact, so add a zero-width confidence interval with certified=true.
                    // If the result is null or multivalued (which happens for INLINE STATS ... BY mv_field),
                    // the confidence interval and certified must be null instead.
                    Object originalValue = expected.values().get(rowIndex).get(originalColumnIndex);
                    boolean isSingleValued = originalValue != null && originalValue instanceof List == false;
                    expected.values().get(rowIndex).add(isSingleValued ? List.of(originalValue, originalValue) : null);
                    expected.values().get(rowIndex).add(isSingleValued ? true : null);
                }
            }
        }

        super.assertResults(expected, actualColumns, actualValues, logger);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        assumeFalse("No approximation tests", testCase.requiredCapabilities.contains(APPROXIMATION_V7.capabilityName()));
        assumeFalse(
            "Approximation casts integer SUM to double, preventing long overflow",
            testCase.requiredCapabilities.contains(FIX_SUM_AGG_LONG_OVERFLOW.capabilityName())
        );
        assumeTrue("Test must contain STATS to be included in approximation tests", testCase.query.toLowerCase().contains("stats"));
    }
}
