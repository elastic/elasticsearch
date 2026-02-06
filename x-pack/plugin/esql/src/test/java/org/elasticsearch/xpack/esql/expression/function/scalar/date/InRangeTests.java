/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for InRange(date, date_range) -> boolean.
 * Validates that dates within/outside ranges return correct boolean values.
 */
public class InRangeTests extends AbstractScalarFunctionTestCase {
    public InRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Test with DATETIME (millis)
        suppliers.add(new TestCaseSupplier("date inside range (datetime)", List.of(DataType.DATETIME, DataType.DATE_RANGE), () -> {
            long dateValue = 1000L; // Date in millis
            long rangeFrom = 500L;
            long rangeTo = 1500L;

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dateValue, DataType.DATETIME, "date"), typedDateRange(rangeFrom, rangeTo)),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));

        suppliers.add(new TestCaseSupplier("date outside range (datetime)", List.of(DataType.DATETIME, DataType.DATE_RANGE), () -> {
            long dateValue = 2000L; // Date outside range
            long rangeFrom = 500L;
            long rangeTo = 1500L;

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dateValue, DataType.DATETIME, "date"), typedDateRange(rangeFrom, rangeTo)),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(false)
            );
        }));

        suppliers.add(new TestCaseSupplier("date at range start (datetime)", List.of(DataType.DATETIME, DataType.DATE_RANGE), () -> {
            long dateValue = 500L; // Exactly at range start
            long rangeFrom = 500L;
            long rangeTo = 1500L;

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dateValue, DataType.DATETIME, "date"), typedDateRange(rangeFrom, rangeTo)),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));

        suppliers.add(new TestCaseSupplier("date at range end (datetime)", List.of(DataType.DATETIME, DataType.DATE_RANGE), () -> {
            long dateValue = 1500L; // Exactly at range end
            long rangeFrom = 500L;
            long rangeTo = 1500L;

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dateValue, DataType.DATETIME, "date"), typedDateRange(rangeFrom, rangeTo)),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));

        // Test with DATE_NANOS
        suppliers.add(new TestCaseSupplier("date inside range (date_nanos)", List.of(DataType.DATE_NANOS, DataType.DATE_RANGE), () -> {
            long dateValueNanos = DateUtils.toNanoSeconds(1000L); // Convert millis to nanos
            long rangeFromNanos = DateUtils.toNanoSeconds(500L);
            long rangeToNanos = DateUtils.toNanoSeconds(1500L);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(dateValueNanos, DataType.DATE_NANOS, "date"),
                    typedDateRange(rangeFromNanos, rangeToNanos)
                ),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));

        suppliers.add(new TestCaseSupplier("date outside range (date_nanos)", List.of(DataType.DATE_NANOS, DataType.DATE_RANGE), () -> {
            long dateValueNanos = DateUtils.toNanoSeconds(2000L);
            long rangeFromNanos = DateUtils.toNanoSeconds(500L);
            long rangeToNanos = DateUtils.toNanoSeconds(1500L);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(dateValueNanos, DataType.DATE_NANOS, "date"),
                    typedDateRange(rangeFromNanos, rangeToNanos)
                ),
                "InRangeEvaluator[date=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(false)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    /**
     * Helper to create a TypedData for DATE_RANGE from long values.
     * Uses a LongRangeBlockBuilder.LongRange to represent the range.
     */
    private static TestCaseSupplier.TypedData typedDateRange(long from, long to) {
        // Use LongRange record to represent the date_range value
        var longRange = new LongRangeBlockBuilder.LongRange(from, to);
        return new TestCaseSupplier.TypedData(longRange, DataType.DATE_RANGE, "range");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new InRange(source, args.get(0), args.get(1));
    }
}
