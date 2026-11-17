/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@code RANGE_WITHIN(value, range) -> boolean}.
 */
public class RangeWithinTests extends AbstractScalarFunctionTestCase {
    public RangeWithinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            // (date, date_range): point in range
            suppliers.add(pointInRange("date inside range (datetime)", DataType.DATETIME, 1000L, 500L, 1500L, true));
            suppliers.add(pointInRange("date outside range (datetime)", DataType.DATETIME, 2000L, 500L, 1500L, false));
            suppliers.add(pointInRange("date at range start (datetime)", DataType.DATETIME, 500L, 500L, 1500L, true));
            // Block stores [from, to); use range (500, 1501) so point 1500 is included
            suppliers.add(pointInRange("date at range end (datetime)", DataType.DATETIME, 1500L, 500L, 1501L, true));

            // (date_range, date_range): first within second
            suppliers.add(rangeWithinRange("first within second", 500L, 1500L, 100L, 2000L, true));
            suppliers.add(rangeWithinRange("first not within second (overlap)", 100L, 1000L, 500L, 1500L, false));
            suppliers.add(rangeWithinRange("equal ranges", 500L, 1500L, 500L, 1500L, true));
            suppliers.add(rangeWithinRange("point range within wider range", 1000L, 1000L, 500L, 1500L, true));
        }
        suppliers.add(doublePointInRange("double inside range", 1.0, 0.5, 1.5, true));
        suppliers.add(doublePointInRange("double outside range", 2.0, 0.5, 1.5, false));
        suppliers.add(doublePointInRange("double at range start", 0.5, 0.5, 1.5, true));
        suppliers.add(doublePointInRange("double at range end", 1.5, 0.5, 1.5, false));

        suppliers.add(doubleRangeWithinRange("double range within range", 0.5, 1.5, 0.0, 2.0, true));
        suppliers.add(doubleRangeWithinRange("overlapping double range is not within", 0.0, 1.0, 0.5, 1.5, false));
        suppliers.add(doubleRangeWithinRange("equal double ranges", 0.5, 1.5, 0.5, 1.5, true));

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
                "RangeWithinPointEvaluator[point=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangeWithinRange(String name, long aFrom, long aTo, long bFrom, long bTo, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(aFrom, aTo), typedDateRange(bFrom, bTo)),
                "RangeWithinRangeEvaluator[a=Attribute[channel=0], b=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier.TypedData typedDateRange(long from, long to) {
        return new TestCaseSupplier.TypedData(new LongRangeBlockBuilder.LongRange(from, to), DataType.DATE_RANGE, "range");
    }

    private static TestCaseSupplier doublePointInRange(String name, double point, double rangeFrom, double rangeTo, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DOUBLE, DataType.DOUBLE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(point, DataType.DOUBLE, "point").withAppliesTo(
                        appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.6.0", "", false)
                    ),
                    typedDoubleRange(rangeFrom, rangeTo)
                ),
                "RangeWithinDoublePointEvaluator[point=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier doubleRangeWithinRange(
        String name,
        double aFrom,
        double aTo,
        double bFrom,
        double bTo,
        boolean expected
    ) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DOUBLE_RANGE, DataType.DOUBLE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDoubleRange(aFrom, aTo), typedDoubleRange(bFrom, bTo)),
                "RangeWithinDoubleRangeEvaluator[a=Attribute[channel=0], b=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier.TypedData typedDoubleRange(double from, double to) {
        return new TestCaseSupplier.TypedData(new DoubleRangeBlockBuilder.DoubleRange(from, to), DataType.DOUBLE_RANGE, "range")
            .withAppliesTo(appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.6.0", "", false));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RangeWithin(source, args.get(0), args.get(1));
    }
}
