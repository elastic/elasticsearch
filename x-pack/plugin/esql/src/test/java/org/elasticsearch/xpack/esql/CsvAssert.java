/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.hamcrest.Matchers;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import static org.elasticsearch.xpack.esql.CsvTestUtils.logMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

final class CsvAssert {
    private CsvAssert() {}

    static void assertResults(ExpectedResults expected, ActualResults actual, Logger logger) {
        assertMetadata(expected, actual, logger);
        assertData(expected, actual, logger);
    }

    static void assertMetadata(ExpectedResults expected, ActualResults actual, Logger logger) {
        if (logger != null) {
            logMetaData(actual, logger);
        }

        var expectedNames = expected.columnNames();
        var actualNames = actual.columnNames();

        var expectedTypes = expected.columnTypes();
        var actualTypes = actual.columnTypes();

        assertThat(
            format(
                null,
                "Different number of columns returned; expected [{}] but actual was [{}]",
                expectedNames.size(),
                actualNames.size()
            ),
            actualNames,
            Matchers.hasSize(expectedNames.size())
        );

        // loop through the metadata
        // first check the column names
        // then check the actual types
        for (int column = 0; column < expectedNames.size(); column++) {
            String expectedName = expectedNames.get(column);
            String actualName = actualNames.get(column);

            if (expectedName.equals(actualName) == false) {
                // to help debugging, indicate the previous column (which also happened to match and thus was correct)
                String expectedSet = expectedName;
                String actualSet = actualName;
                if (column > 1) {
                    expectedSet = expectedNames.get(column - 1) + "," + expectedName;
                    actualSet = actualNames.get(column - 1) + "," + actualName;
                }

                assertEquals("Different column name [" + column + "]", expectedSet, actualSet);
            }

            var expectedType = expectedTypes.get(column);
            var actualType = actualTypes.get(column);

            if (actualType == Type.INTEGER && expectedType == Type.LONG) {
                actualType = Type.LONG;
            }

            assertEquals(
                "Different column type for column [" + expectedName + "] (" + expectedType + " != " + actualType + ")",
                expectedType,
                actualType
            );

            // perform another check against each returned page to make sure they have the same metadata
            var pages = actual.pages();

            for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                var page = pages.get(pageIndex);
                var block = page.getBlock(column);
                var blockType = Type.asType(block.elementType());

                if (blockType == Type.LONG && expectedType == Type.DATETIME) {
                    blockType = Type.DATETIME;
                }

                assertEquals(
                    format(
                        null,
                        "Different column type for column [{}][{}] as block inside page [{}]; ({} != {})",
                        expectedName,
                        column,
                        pageIndex,
                        expectedType,
                        blockType
                    ),
                    expectedType,
                    blockType
                );
            }
        }
    }

    static void assertData(ExpectedResults expected, ActualResults actual, Logger logger) {
        var columns = expected.columnNames();
        var expectedValues = expected.values();
        var actualValues = actual.values();

        int row = 0;
        try {
            for (row = 0; row < expectedValues.size(); row++) {
                assertTrue("Expected more data but no more entries found after [" + row + "]", row < actualValues.size());

                if (logger != null) {
                    logger.info(row(actualValues, row));
                }

                var expectedRow = expectedValues.get(row);
                var actualRow = actualValues.get(row);

                int column = 0;
                for (column = 0; column < expectedRow.size(); column++) {
                    assertTrue("Missing column [" + column + "] at row  [" + row + "]", column < expectedRow.size());

                    var expectedValue = expectedRow.get(column);
                    var actualValue = actualRow.get(column);

                    // convert the long from CSV back to its STRING form
                    if (expectedValue != null && expected.columnTypes().get(column) == Type.DATETIME) {
                        expectedValue = DateFormat.DEFAULT_DATE_FORMATTER.formatMillis((long) expectedValue);
                    }
                    assertEquals(expectedValue, actualValue);
                }

                var delta = actualRow.size() - expectedRow.size();
                if (delta > 0) {
                    fail("Plan has extra columns, returned [" + actualRow.size() + "], expected [" + expectedRow.size() + "]");
                }
            }

        } catch (AssertionError ae) {
            if (logger != null && row + 1 < actualValues.size()) {
                logger.info("^^^ Assertion failure ^^^");
                logger.info(row(actualValues, row + 1));
            }
            throw ae;
        }
        if (row + 1 < actualValues.size()) {
            fail("Elasticsearch still has data after [" + row + "] entries:\n" + row(actualValues, row));
        }
    }

    static String row(List<List<Object>> values, int row) {
        return values.get(row).toString();
    }
}
