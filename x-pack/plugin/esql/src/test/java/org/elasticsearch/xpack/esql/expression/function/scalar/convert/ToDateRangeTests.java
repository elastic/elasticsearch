/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.nullValue;

public class ToDateRangeTests extends AbstractScalarFunctionTestCase {
    public ToDateRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        // DATE_RANGE passthrough - uses shared dateRangeCases() so future edge cases are covered
        TestCaseSupplier.forUnaryDateRange(suppliers, read, DataType.DATE_RANGE, v -> v, List.of());

        // String types (KEYWORD, TEXT, etc.) to DATE_RANGE - parses "start..end" format
        for (DataType stringType : DataType.stringTypes()) {
            suppliers.add(new TestCaseSupplier(stringType.typeName() + " date range string", List.of(stringType), () -> {
                long fromMillis = randomLongBetween(0L, 1_000_000_000_000L);
                long toMillis = randomLongBetween(fromMillis + 1, fromMillis + 1_000_000_000_000L);

                String fromStr = Instant.ofEpochMilli(fromMillis).toString();
                String toStr = Instant.ofEpochMilli(toMillis).toString();
                String rangeString = fromStr + ".." + toStr;

                long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis(fromStr);
                long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis(toStr);
                var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);

                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), stringType, "field")),
                    "ToDateRangeFromStringEvaluator[field=" + read + "]",
                    DataType.DATE_RANGE,
                    equalTo(expectedRange)
                );
            }));
        }

        // Helper-based cases: easy to add timezone/locale later when TO_DATE_RANGE supports them
        suppliers.addAll(
            casesForKeyword("2020-01-01T00:00:00.000Z..2021-01-01T00:00:00.000Z", "2020-01-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z")
        );
        suppliers.addAll(
            casesForKeyword("2024-06-15T00:00:00.000Z..2024-06-16T00:00:00.000Z", "2024-06-15T00:00:00.000Z", "2024-06-16T00:00:00.000Z")
        );

        // Edge cases (review): from > to, from == to, invalid from, invalid to; boundary dates to avoid overflow
        suppliers.add(
            caseForKeywordInvalid(
                "from bigger than to",
                "2021-01-01T00:00:00.000Z..2020-01-01T00:00:00.000Z",
                "java.lang.IllegalArgumentException: date range 'from' [2021-01-01T00:00:00.000Z] "
                    + "must be less than or equal to 'to' [2020-01-01T00:00:00.000Z]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "from same as to",
                "2020-01-01T00:00:00.000Z..2020-01-01T00:00:00.000Z",
                "java.lang.IllegalArgumentException: date range 'from' [2020-01-01T00:00:00.000Z] "
                    + "must be less than or equal to 'to' [2020-01-01T00:00:00.000Z]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "from unparseable (invalid from)",
                "not-a-date..2020-01-01T00:00:00.000Z",
                "java.lang.IllegalArgumentException: failed to parse date field [not-a-date] with format [strict_date_optional_time]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "to unparseable (invalid to)",
                "2020-01-01T00:00:00.000Z..not-a-date",
                "java.lang.IllegalArgumentException: failed to parse date field [not-a-date] with format [strict_date_optional_time]"
            )
        );
        suppliers.addAll(
            casesForKeyword("0001-01-01T00:00:00.000Z..0002-01-01T00:00:00.000Z", "0001-01-01T00:00:00.000Z", "0002-01-01T00:00:00.000Z")
        );
        suppliers.addAll(
            casesForKeyword("9999-01-01T00:00:00.000Z..9999-12-31T00:00:00.000Z", "9999-01-01T00:00:00.000Z", "9999-12-31T00:00:00.000Z")
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    /**
     * Helper to build KEYWORD test cases for TO_DATE_RANGE(string). Simplifies adding cases and future timezone/locale tests.
     * Uses UTC; when TO_DATE_RANGE is configuration-aware, extend with zone/locale params like DateParseTests.casesFor.
     */
    private static List<TestCaseSupplier> casesForKeyword(String rangeString, String expectedFromStr, String expectedToStr) {
        long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis(expectedFromStr);
        long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis(expectedToStr);
        var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);
        final String read = "Attribute[channel=0]";
        return List.of(
            new TestCaseSupplier(
                "keyword: " + rangeString,
                List.of(DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                    "ToDateRangeFromStringEvaluator[field=" + read + "]",
                    DataType.DATE_RANGE,
                    equalTo(expectedRange)
                )
            )
        );
    }

    /**
     * Helper for invalid KEYWORD input: parsing throws, evaluator returns null and registers a warning.
     * @param exceptionWarningFragment substring that must appear in the exception warning
     *                                  (e.g. exception message or "IllegalArgumentException")
     */
    private static TestCaseSupplier caseForKeywordInvalid(String name, String rangeString, String exceptionWarningFragment) {
        final String read = "Attribute[channel=0]";
        return new TestCaseSupplier(
            "keyword invalid: " + name,
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                "ToDateRangeFromStringEvaluator[field=" + read + "]",
                DataType.DATE_RANGE,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: " + exceptionWarningFragment)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDateRange(source, args.get(0));
    }

    private static org.hamcrest.Matcher<Object> equalTo(LongRangeBlockBuilder.LongRange expected) {
        return org.hamcrest.Matchers.equalTo(expected);
    }
}
