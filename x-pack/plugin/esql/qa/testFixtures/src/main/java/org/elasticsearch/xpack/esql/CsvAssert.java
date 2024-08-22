/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.StringDescription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.CsvTestUtils.logMetaData;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public final class CsvAssert {
    private CsvAssert() {}

    static void assertResults(ExpectedResults expected, ActualResults actual, boolean ignoreOrder, Logger logger) {
        assertMetadata(expected, actual, logger);
        assertData(expected, actual, ignoreOrder, logger);
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
            format(null, "Different number of columns returned; expected {} but actual was {}", expectedNames, actualNames),
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
            if (actualType == null) {
                actualType = Type.NULL;
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
                var blockType = Type.asType(block.elementType(), actualType);

                if (blockType == Type.LONG
                    && (expectedType == Type.DATETIME
                        || expectedType == Type.DATE_NANOS
                        || expectedType == Type.GEO_POINT
                        || expectedType == Type.CARTESIAN_POINT
                        || expectedType == UNSIGNED_LONG)) {
                    continue;
                }
                if (blockType == Type.KEYWORD && (expectedType == Type.IP || expectedType == Type.VERSION || expectedType == Type.TEXT)) {
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

    static void assertData(ExpectedResults expected, ActualResults actual, boolean ignoreOrder, Logger logger) {
        assertData(expected, actual.values(), ignoreOrder, logger, (t, v) -> v);
    }

    public static void assertData(
        ExpectedResults expected,
        Iterator<Iterator<Object>> actualValuesIterator,
        boolean ignoreOrder,
        Logger logger,
        BiFunction<Type, Object, Object> valueTransformer
    ) {
        assertData(expected, EsqlTestUtils.getValuesList(actualValuesIterator), ignoreOrder, logger, valueTransformer);
    }

    private record DataFailure(int row, int column, Object expected, Object actual) {}

    public static void assertData(
        ExpectedResults expected,
        List<List<Object>> actualValues,
        boolean ignoreOrder,
        Logger logger,
        BiFunction<Type, Object, Object> valueTransformer
    ) {
        if (ignoreOrder) {
            expected.values().sort(resultRowComparator(expected.columnTypes()));
            actualValues.sort(resultRowComparator(expected.columnTypes()));
        }
        var expectedValues = expected.values();
        List<DataFailure> dataFailures = new ArrayList<>();

        for (int row = 0; row < expectedValues.size(); row++) {
            try {
                if (row >= actualValues.size()) {
                    if (dataFailures.isEmpty()) {
                        fail("Expected more data but no more entries found after [" + row + "]");
                    } else {
                        dataFailure(dataFailures, "Expected more data but no more entries found after [" + row + "]\n");
                    }
                }

                if (logger != null) {
                    logger.info(row(actualValues, row));
                }

                var expectedRow = expectedValues.get(row);
                var actualRow = actualValues.get(row);

                for (int column = 0; column < expectedRow.size(); column++) {
                    var expectedValue = expectedRow.get(column);
                    var actualValue = actualRow.get(column);
                    var expectedType = expected.columnTypes().get(column);

                    if (expectedValue != null) {
                        // convert the long from CSV back to its STRING form
                        if (expectedType == Type.DATETIME) {
                            expectedValue = rebuildExpected(expectedValue, Long.class, x -> UTC_DATE_TIME_FORMATTER.formatMillis((long) x));
                        } else if (expectedType == Type.DATE_NANOS) {
                            expectedValue = rebuildExpected(
                                expectedValue,
                                Long.class,
                                x -> DateFormatter.forPattern("strict_date_optional_time_nanos").formatNanos((long) x)
                            );
                        } else if (expectedType == Type.GEO_POINT) {
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> GEO.wkbToWkt((BytesRef) x));
                        } else if (expectedType == Type.CARTESIAN_POINT) {
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> CARTESIAN.wkbToWkt((BytesRef) x));
                        } else if (expectedType == Type.GEO_SHAPE) {
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> GEO.wkbToWkt((BytesRef) x));
                        } else if (expectedType == Type.CARTESIAN_SHAPE) {
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> CARTESIAN.wkbToWkt((BytesRef) x));
                        } else if (expectedType == Type.IP) {
                            // convert BytesRef-packed IP to String, allowing subsequent comparison with what's expected
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> DocValueFormat.IP.format((BytesRef) x));
                        } else if (expectedType == Type.VERSION) {
                            // convert BytesRef-packed Version to String
                            expectedValue = rebuildExpected(expectedValue, BytesRef.class, x -> new Version((BytesRef) x).toString());
                        } else if (expectedType == UNSIGNED_LONG) {
                            expectedValue = rebuildExpected(expectedValue, Long.class, x -> unsignedLongAsNumber((long) x));
                        }
                    }
                    var transformedExpected = valueTransformer.apply(expectedType, expectedValue);
                    var transformedActual = valueTransformer.apply(expectedType, actualValue);
                    if (Objects.equals(transformedExpected, transformedActual) == false) {
                        dataFailures.add(new DataFailure(row, column, transformedExpected, transformedActual));
                    }
                    if (dataFailures.size() > 10) {
                        dataFailure(dataFailures);
                    }
                }

                var delta = actualRow.size() - expectedRow.size();
                if (delta > 0) {
                    fail("Plan has extra columns, returned [" + actualRow.size() + "], expected [" + expectedRow.size() + "]");
                }
            } catch (AssertionError ae) {
                if (logger != null && row + 1 < actualValues.size()) {
                    logger.info("^^^ Assertion failure ^^^");
                    logger.info(row(actualValues, row + 1));
                }
                throw ae;
            }
        }
        if (dataFailures.isEmpty() == false) {
            dataFailure(dataFailures);
        }
        if (expectedValues.size() < actualValues.size()) {
            fail(
                "Elasticsearch still has data after [" + expectedValues.size() + "] entries:\n" + row(actualValues, expectedValues.size())
            );
        }
    }

    private static void dataFailure(List<DataFailure> dataFailures) {
        dataFailure(dataFailures, "");
    }

    private static void dataFailure(List<DataFailure> dataFailures, String prefixError) {
        fail(prefixError + "Data mismatch:\n" + dataFailures.stream().map(f -> {
            Description description = new StringDescription();
            ListMatcher expected;
            if (f.expected instanceof List<?> e) {
                expected = ListMatcher.matchesList(e);
            } else {
                expected = ListMatcher.matchesList().item(f.expected);
            }
            List<?> actualList;
            if (f.actual instanceof List<?> a) {
                actualList = a;
            } else {
                // Do not use List::of - actual can be null.
                actualList = Collections.singletonList(f.actual);
            }
            expected.describeMismatch(actualList, description);
            String prefix = "row " + f.row + " column " + f.column + ":";
            return prefix + description.toString().replace("\n", "\n" + prefix);
        }).collect(Collectors.joining("\n")));
    }

    private static Comparator<List<Object>> resultRowComparator(List<Type> types) {
        return (x, y) -> {
            for (int i = 0; i < x.size(); i++) {
                Object left = x.get(i);
                if (left instanceof List<?> l) {
                    left = l.isEmpty() ? null : l.get(0);
                }
                Object right = y.get(i);
                if (right instanceof List<?> r) {
                    right = r.isEmpty() ? null : r.get(0);
                }
                if (left == null && right == null) {
                    continue;
                }
                if (left == null) {
                    return 1;
                }
                if (right == null) {
                    return -1;
                }
                int result = types.get(i).comparator().compare(left, right);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };
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
