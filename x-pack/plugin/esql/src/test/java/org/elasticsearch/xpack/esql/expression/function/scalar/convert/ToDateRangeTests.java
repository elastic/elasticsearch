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

public class ToDateRangeTests extends AbstractScalarFunctionTestCase {
    public ToDateRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        // DATE_RANGE passthrough - returns input unchanged
        suppliers.add(new TestCaseSupplier("date_range passthrough", List.of(DataType.DATE_RANGE), () -> {
            long from = randomLongBetween(0L, 1_000_000_000_000L);
            long to = randomLongBetween(from, from + 1_000_000_000_000L);
            var range = new LongRangeBlockBuilder.LongRange(from, to);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                read,
                DataType.DATE_RANGE,
                equalTo(range)
            );
        }));

        // KEYWORD to DATE_RANGE - parses "start..end" format
        suppliers.add(new TestCaseSupplier("keyword date range string", List.of(DataType.KEYWORD), () -> {
            long fromMillis = randomLongBetween(0L, 1_000_000_000_000L);
            long toMillis = randomLongBetween(fromMillis + 1, fromMillis + 1_000_000_000_000L);

            String fromStr = Instant.ofEpochMilli(fromMillis).toString();
            String toStr = Instant.ofEpochMilli(toMillis).toString();
            String rangeString = fromStr + ".." + toStr;

            // parseDateRange subtracts 1 from the 'to' value
            long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis(fromStr);
            long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis(toStr) - 1;
            var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                "ToDateRangeFromStringEvaluator[field=" + read + "]",
                DataType.DATE_RANGE,
                equalTo(expectedRange)
            );
        }));

        // TEXT to DATE_RANGE - same parsing as keyword
        suppliers.add(new TestCaseSupplier("text date range string", List.of(DataType.TEXT), () -> {
            long fromMillis = randomLongBetween(0L, 1_000_000_000_000L);
            long toMillis = randomLongBetween(fromMillis + 1, fromMillis + 1_000_000_000_000L);

            String fromStr = Instant.ofEpochMilli(fromMillis).toString();
            String toStr = Instant.ofEpochMilli(toMillis).toString();
            String rangeString = fromStr + ".." + toStr;

            // parseDateRange subtracts 1 from the 'to' value
            long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis(fromStr);
            long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis(toStr) - 1;
            var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.TEXT, "field")),
                "ToDateRangeFromStringEvaluator[field=" + read + "]",
                DataType.DATE_RANGE,
                equalTo(expectedRange)
            );
        }));

        // Test with specific well-known dates
        suppliers.add(new TestCaseSupplier("specific date range - year 2020", List.of(DataType.KEYWORD), () -> {
            String rangeString = "2020-01-01T00:00:00.000Z..2020-12-31T23:59:59.999Z";
            long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01T00:00:00.000Z");
            long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-12-31T23:59:59.999Z") - 1;
            var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                "ToDateRangeFromStringEvaluator[field=" + read + "]",
                DataType.DATE_RANGE,
                equalTo(expectedRange)
            );
        }));

        // Test with minimal date range (same day)
        suppliers.add(new TestCaseSupplier("same day date range", List.of(DataType.KEYWORD), () -> {
            String rangeString = "2024-06-15T00:00:00.000Z..2024-06-15T23:59:59.999Z";
            long expectedFrom = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-06-15T00:00:00.000Z");
            long expectedTo = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-06-15T23:59:59.999Z") - 1;
            var expectedRange = new LongRangeBlockBuilder.LongRange(expectedFrom, expectedTo);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                "ToDateRangeFromStringEvaluator[field=" + read + "]",
                DataType.DATE_RANGE,
                equalTo(expectedRange)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDateRange(source, args.get(0));
    }

    private static org.hamcrest.Matcher<Object> equalTo(LongRangeBlockBuilder.LongRange expected) {
        return org.hamcrest.Matchers.equalTo(expected);
    }
}
