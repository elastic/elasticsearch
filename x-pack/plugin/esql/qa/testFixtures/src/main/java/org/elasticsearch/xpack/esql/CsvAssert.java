/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import static org.elasticsearch.xpack.esql.CsvTestUtils.logMetaData;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class CsvAssert {
    private CsvAssert() {}

    static void assertResults(ExpectedResults expected, ActualResults actual, Logger logger) {
        assertMetadata(expected, actual, logger);
        assertData(expected, actual, logger);
    }

    static void assertMetadata(ExpectedResults expected, ActualResults actual, Logger logger) {
        assertMetadata(expected, actual.columnNames(), actual.columnTypes(), actual.pages(), logger);
    }

    public static void assertMetadata(ExpectedResults expected, List<Map<String, String>> actualColumns, Logger logger) {
        var actualColumnNames = new ArrayList<String>(actualColumns.size());
        var actualColumnTypes = actualColumns.stream()
            .peek(c -> actualColumnNames.add(c.get("name")))
            .map(c -> CsvTestUtils.Type.asType(c.get("type")))
            .toList();
        assertMetadata(expected, actualColumnNames, actualColumnTypes, List.of(), logger);
    }

    private static void assertMetadata(
        ExpectedResults expected,
        List<String> actualNames,
        List<Type> actualTypes,
        List<Page> pages,
        Logger logger
    ) {
        if (logger != null) {
            logMetaData(actualNames, actualTypes, logger);
        }

        var expectedNames = expected.columnNames();
        var expectedTypes = expected.columnTypes();

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
            for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                var page = pages.get(pageIndex);
                var block = page.getBlock(column);
                var blockType = Type.asType(block.elementType());

                if (blockType == Type.LONG && expectedType == Type.DATETIME) {
                    continue;
                }
                if (blockType == Type.KEYWORD && (expectedType == Type.IP || expectedType == Type.VERSION)) {
                    // Type.asType translates all bytes references into keywords
                    continue;
                }
                if (blockType == Type.NULL) {
                    // Null pages don't have any real type information beyond "it's all null, man"
                    continue;
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
        assertData(expected, actual.values(), logger, Function.identity());
    }

    public static void assertData(
        ExpectedResults expected,
        List<List<Object>> actualValues,
        Logger logger,
        Function<Object, Object> valueTransformer
    ) {
        var expectedValues = expected.values();

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
                    var expectedValue = expectedRow.get(column);
                    var actualValue = actualRow.get(column);

                    if (expectedValue != null) {
                        var expectedType = expected.columnTypes().get(column);
                        // convert the long from CSV back to its STRING form
                        if (expectedType == Type.DATETIME) {
                            expectedValue = rebuildExpected(expectedValue, Long.class, x -> UTC_DATE_TIME_FORMATTER.formatMillis((long) x));
                        } else if (expectedType == Type.IP) {
                            // convert BytesRef-packed IP to String, allowing subsequent comparison with what's expected
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> DocValueFormat.IP.format((BytesRef) x));
                        } else if (expectedType == Type.VERSION) {
                            // convert BytesRef-packed Version to String
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> new Version((BytesRef) x).toString());
                        }

                    }
                    assertEquals(valueTransformer.apply(expectedValue), valueTransformer.apply(actualValue));
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

    private static Object rebuildExpected(Object expectedValue, Class<?> clazz, Function<Object, Object> mapper) {
        if (List.class.isAssignableFrom(expectedValue.getClass())) {
            assertThat(((List<?>) expectedValue).get(0), instanceOf(clazz));
            return ((List<?>) expectedValue).stream().map(mapper).toList();
        } else {
            assertThat(expectedValue, instanceOf(clazz));
            return mapper.apply(expectedValue);
        }
    }

    static String row(List<List<Object>> values, int row) {
        return values.get(row).toString();
    }
}
