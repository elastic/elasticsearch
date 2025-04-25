/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
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
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.aggregateMetricDoubleLiteralToString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
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

                assertEquals("Different column name [" + column + "]: " + actualName, expectedSet, actualSet);
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
                if (blockType == Type.KEYWORD
                    && (expectedType == Type.IP
                        || expectedType == Type.VERSION
                        || expectedType == Type.TEXT
                        || expectedType == Type.SEMANTIC_TEXT)) {
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
                    dataFailure(
                        "Expected more data but no more entries found after [" + row + "]",
                        dataFailures,
                        expected,
                        actualValues,
                        valueTransformer
                    );
                }

                if (logger != null) {
                    logger.info(row(actualValues, row));
                }

                var expectedRow = expectedValues.get(row);
                var actualRow = actualValues.get(row);

                for (int column = 0; column < expectedRow.size(); column++) {
                    var expectedType = expected.columnTypes().get(column);
                    var expectedValue = convertExpectedValue(expectedType, expectedRow.get(column));
                    var actualValue = actualRow.get(column);

                    var transformedExpected = valueTransformer.apply(expectedType, expectedValue);
                    var transformedActual = valueTransformer.apply(expectedType, actualValue);
                    if (Objects.equals(transformedExpected, transformedActual) == false) {
                        dataFailures.add(new DataFailure(row, column, transformedExpected, transformedActual));
                    }
                    if (dataFailures.size() > 10) {
                        dataFailure("", dataFailures, expected, actualValues, valueTransformer);
                    }
                }

                if (actualRow.size() != expectedRow.size()) {
                    dataFailure(
                        "Plan has extra columns, returned [" + actualRow.size() + "], expected [" + expectedRow.size() + "]",
                        dataFailures,
                        expected,
                        actualValues,
                        valueTransformer
                    );
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
            dataFailure("", dataFailures, expected, actualValues, valueTransformer);
        }
        if (expectedValues.size() < actualValues.size()) {
            dataFailure(
                "Elasticsearch still has data after [" + expectedValues.size() + "] entries",
                dataFailures,
                expected,
                actualValues,
                valueTransformer
            );
        }
    }

    private static void dataFailure(
        String description,
        List<DataFailure> dataFailures,
        ExpectedResults expectedValues,
        List<List<Object>> actualValues,
        BiFunction<Type, Object, Object> valueTransformer
    ) {
        var expected = pipeTable(
            "Expected:",
            expectedValues.columnNames(),
            expectedValues.columnTypes(),
            expectedValues.values(),
            (type, value) -> valueTransformer.apply(type, convertExpectedValue(type, value))
        );
        var actual = pipeTable("Actual:", expectedValues.columnNames(), expectedValues.columnTypes(), actualValues, valueTransformer);
        fail(description + System.lineSeparator() + describeFailures(dataFailures) + actual + expected);
    }

    private static final int MAX_ROWS = 250;

    private static String pipeTable(
        String description,
        List<String> headers,
        List<Type> types,
        List<List<Object>> values,
        BiFunction<Type, Object, Object> valueTransformer
    ) {
        int rows = Math.min(MAX_ROWS, values.size());
        int[] width = new int[headers.size()];
        String[][] printableValues = new String[rows][headers.size()];
        for (int c = 0; c < headers.size(); c++) {
            width[c] = header(headers.get(c), types.get(c)).length();
        }
        for (int r = 0; r < rows; r++) {
            assertThat("Mismatched header size and values", headers.size() == values.get(r).size());
            for (int c = 0; c < headers.size(); c++) {
                printableValues[r][c] = String.valueOf(valueTransformer.apply(types.get(c), values.get(r).get(c)));
                width[c] = Math.max(width[c], printableValues[r][c].length());
            }
        }

        var result = new StringBuilder().append(System.lineSeparator()).append(description).append(System.lineSeparator());
        // headers
        appendPaddedValue(result, header(headers.get(0), types.get(0)), width[0]);
        for (int c = 1; c < width.length; c++) {
            result.append(" | ");
            appendPaddedValue(result, header(headers.get(c), types.get(c)), width[c]);
        }
        result.append(System.lineSeparator());
        // values
        for (int r = 0; r < printableValues.length; r++) {
            appendPaddedValue(result, printableValues[r][0], width[0]);
            for (int c = 1; c < printableValues[r].length; c++) {
                result.append(" | ");
                appendPaddedValue(result, printableValues[r][c], width[c]);
            }
            result.append(System.lineSeparator());
        }
        if (values.size() > rows) {
            result.append("...").append(System.lineSeparator());
        }
        return result.toString();
    }

    private static String header(String name, Type type) {
        return name + ':' + Strings.toLowercaseAscii(type.name());
    }

    private static void appendPaddedValue(StringBuilder result, String value, int width) {
        result.append(value);
        for (int i = 0; i < width - (value != null ? value.length() : 4); i++) {
            result.append(' ');
        }
    }

    private static String describeFailures(List<DataFailure> dataFailures) {
        return "Data mismatch:" + System.lineSeparator() + dataFailures.stream().map(f -> {
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
        }).collect(Collectors.joining(System.lineSeparator()));
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

    private static Object convertExpectedValue(Type expectedType, Object expectedValue) {
        if (expectedValue == null) {
            return null;
        }

        // convert the long from CSV back to its STRING form
        return switch (expectedType) {
            case Type.DATETIME -> rebuildExpected(expectedValue, Long.class, x -> UTC_DATE_TIME_FORMATTER.formatMillis((long) x));
            case Type.DATE_NANOS -> rebuildExpected(
                expectedValue,
                Long.class,
                x -> DateFormatter.forPattern("strict_date_optional_time_nanos").formatNanos((long) x)
            );
            case Type.GEO_POINT, Type.GEO_SHAPE -> rebuildExpected(expectedValue, BytesRef.class, x -> GEO.wkbToWkt((BytesRef) x));
            case Type.CARTESIAN_POINT, Type.CARTESIAN_SHAPE -> rebuildExpected(
                expectedValue,
                BytesRef.class,
                x -> CARTESIAN.wkbToWkt((BytesRef) x)
            );
            case Type.IP -> // convert BytesRef-packed IP to String, allowing subsequent comparison with what's expected
                rebuildExpected(expectedValue, BytesRef.class, x -> DocValueFormat.IP.format((BytesRef) x));
            case Type.VERSION -> // convert BytesRef-packed Version to String
                rebuildExpected(expectedValue, BytesRef.class, x -> new Version((BytesRef) x).toString());
            case UNSIGNED_LONG -> rebuildExpected(expectedValue, Long.class, x -> unsignedLongAsNumber((long) x));
            case AGGREGATE_METRIC_DOUBLE -> rebuildExpected(
                expectedValue,
                AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral.class,
                x -> aggregateMetricDoubleLiteralToString((AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral) x)
            );
            default -> expectedValue;
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
