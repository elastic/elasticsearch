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
 * Tests for RANGE_WITHIN(left, right) -> boolean for all four type combinations:
 * (date_range, date), (date, date_range), (date_range, date_range), (date, date).
 */
public class RangeWithinTests extends AbstractScalarFunctionTestCase {
    public RangeWithinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // (date, date_range): point in range
        suppliers.add(pointInRange("date inside range (datetime)", DataType.DATETIME, 1000L, 500L, 1500L, true));
        suppliers.add(pointInRange("date outside range (datetime)", DataType.DATETIME, 2000L, 500L, 1500L, false));
        suppliers.add(pointInRange("date at range start (datetime)", DataType.DATETIME, 500L, 500L, 1500L, true));
        suppliers.add(pointInRange("date at range end (datetime)", DataType.DATETIME, 1500L, 500L, 1500L, true));
        suppliers.add(
            pointInRange(
                "date inside range (date_nanos)",
                DataType.DATE_NANOS,
                DateUtils.toNanoSeconds(1000L),
                DateUtils.toNanoSeconds(500L),
                DateUtils.toNanoSeconds(1500L),
                true
            )
        );
        suppliers.add(
            pointInRange(
                "date outside range (date_nanos)",
                DataType.DATE_NANOS,
                DateUtils.toNanoSeconds(2000L),
                DateUtils.toNanoSeconds(500L),
                DateUtils.toNanoSeconds(1500L),
                false
            )
        );

        // (date_range, date): range contains point (same as point in range)
        suppliers.add(rangeContainsPoint("range contains point", 500L, 1500L, 1000L, true));
        suppliers.add(rangeContainsPoint("range does not contain point", 500L, 1500L, 2000L, false));
        suppliers.add(rangeContainsPointNanos("range contains point (date_nanos)", 500L, 1500L, DateUtils.toNanoSeconds(1000L), true));

        // (date_range, date_range): first contains second
        suppliers.add(rangeContainsRange("first contains second", 100L, 2000L, 500L, 1500L, true));
        suppliers.add(rangeContainsRange("first does not contain second (overlap)", 100L, 1000L, 500L, 1500L, false));
        suppliers.add(rangeContainsRange("equal ranges", 500L, 1500L, 500L, 1500L, true));
        suppliers.add(rangeContainsRange("point range in range", 500L, 1500L, 1000L, 1000L, true));

        // (date, date): equality
        suppliers.add(dateEqualsDate("dates equal", 1000L, 1000L, true));
        suppliers.add(dateEqualsDate("dates not equal", 1000L, 2000L, false));
        suppliers.add(new TestCaseSupplier("dates equal (date_nanos)", List.of(DataType.DATE_NANOS, DataType.DATE_NANOS), () -> {
            long t = DateUtils.toNanoSeconds(1000L);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(t, DataType.DATE_NANOS, "a"),
                    new TestCaseSupplier.TypedData(t, DataType.DATE_NANOS, "b")
                ),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));
        // (datetime, date_nanos) and (date_nanos, datetime): cross-type equality; raw long comparison so different units => false
        suppliers.add(
            new TestCaseSupplier(
                "dates not equal (datetime, date_nanos)",
                List.of(DataType.DATETIME, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1000L, DataType.DATETIME, "a"),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(1000L), DataType.DATE_NANOS, "b")
                    ),
                    "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                    DataType.BOOLEAN,
                    equalTo(false)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "dates not equal (date_nanos, datetime)",
                List.of(DataType.DATE_NANOS, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(1000L), DataType.DATE_NANOS, "a"),
                        new TestCaseSupplier.TypedData(1000L, DataType.DATETIME, "b")
                    ),
                    "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                    DataType.BOOLEAN,
                    equalTo(false)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static TestCaseSupplier pointInRange(
        String name,
        DataType dateType,
        long dateVal,
        long rangeFrom,
        long rangeTo,
        boolean expected
    ) {
        return new TestCaseSupplier(
            name,
            List.of(dateType, DataType.DATE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dateVal, dateType, "date"), typedDateRange(rangeFrom, rangeTo)),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangeContainsPoint(String name, long rangeFrom, long rangeTo, long point, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(rangeFrom, rangeTo), new TestCaseSupplier.TypedData(point, DataType.DATETIME, "date")),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangeContainsPointNanos(String name, long rangeFrom, long rangeTo, long pointNanos, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATE_NANOS),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    typedDateRange(DateUtils.toNanoSeconds(rangeFrom), DateUtils.toNanoSeconds(rangeTo)),
                    new TestCaseSupplier.TypedData(pointNanos, DataType.DATE_NANOS, "date")
                ),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangeContainsRange(String name, long aFrom, long aTo, long bFrom, long bTo, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(aFrom, aTo), typedDateRange(bFrom, bTo)),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier dateEqualsDate(String name, long a, long b, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATETIME, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(a, DataType.DATETIME, "a"),
                    new TestCaseSupplier.TypedData(b, DataType.DATETIME, "b")
                ),
                "RangeWithinEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier.TypedData typedDateRange(long from, long to) {
        return new TestCaseSupplier.TypedData(new LongRangeBlockBuilder.LongRange(from, to), DataType.DATE_RANGE, "range");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RangeWithin(source, args.get(0), args.get(1));
    }
}
