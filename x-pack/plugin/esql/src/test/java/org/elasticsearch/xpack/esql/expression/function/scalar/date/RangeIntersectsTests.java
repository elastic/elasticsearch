/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
 * Tests for RANGE_INTERSECTS(left, right) -> boolean covering the supported type combinations:
 * (date_range, date_range), (date, date_range), (date_range, date). The (date, date) case lowers to
 * Equals via SurrogateExpression and isn't exercised through this evaluator path.
 */
public class RangeIntersectsTests extends AbstractScalarFunctionTestCase {
    public RangeIntersectsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            // (date_range, date_range)
            suppliers.add(rangeRange("identical ranges", 500L, 1500L, 500L, 1500L, true));
            suppliers.add(rangeRange("ranges overlap on the right", 500L, 1500L, 1000L, 2000L, true));
            suppliers.add(rangeRange("ranges overlap on the left", 1000L, 2000L, 500L, 1500L, true));
            suppliers.add(rangeRange("first contains second", 100L, 2000L, 500L, 1500L, true));
            suppliers.add(rangeRange("second contains first", 500L, 1500L, 100L, 2000L, true));
            suppliers.add(rangeRange("disjoint, first before second", 100L, 500L, 1000L, 1500L, false));
            suppliers.add(rangeRange("disjoint, second before first", 1000L, 1500L, 100L, 500L, false));
            // Half-open ranges touching at the edge do NOT intersect: [500,1000) and [1000,2000) share no point.
            suppliers.add(rangeRange("ranges touch at right edge (half-open)", 500L, 1000L, 1000L, 2000L, false));
            suppliers.add(rangeRange("ranges touch at left edge (half-open)", 1000L, 2000L, 500L, 1000L, false));

            // (date, date_range): point in range
            suppliers.add(pointRange("date inside range", DataType.DATETIME, 1000L, 500L, 1500L, true));
            suppliers.add(pointRange("date outside range", DataType.DATETIME, 2000L, 500L, 1500L, false));
            suppliers.add(pointRange("date at range start (inclusive)", DataType.DATETIME, 500L, 500L, 1500L, true));
            // Half-open: range end is exclusive, so the point at the end is NOT in range.
            suppliers.add(pointRange("date at range end (exclusive)", DataType.DATETIME, 1500L, 500L, 1500L, false));
            // Use a slightly wider range so the boundary point falls inside.
            suppliers.add(pointRange("date just before range end", DataType.DATETIME, 1499L, 500L, 1500L, true));

            // (date_range, date): symmetric to point-in-range
            suppliers.add(rangePoint("range contains date", 500L, 1500L, DataType.DATETIME, 1000L, true));
            suppliers.add(rangePoint("range does not contain date", 500L, 1500L, DataType.DATETIME, 2000L, false));

            // (date, date): degenerate; surrogate lowers to Equals, so the framework runs EqualsLongsEvaluator.
            suppliers.add(dateDate("equal dates", 1000L, 1000L, true));
            suppliers.add(dateDate("different dates", 1000L, 2000L, false));
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static TestCaseSupplier rangeRange(String name, long aFrom, long aTo, long bFrom, long bTo, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATE_RANGE),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(aFrom, aTo), typedDateRange(bFrom, bTo)),
                "RangeIntersectsRangeEvaluator[a=Attribute[channel=0], b=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier pointRange(
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
                "RangeIntersectsPointEvaluator[point=Attribute[channel=0], range=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangePoint(
        String name,
        long rangeFrom,
        long rangeTo,
        DataType dateType,
        long dateVal,
        boolean expected
    ) {
        // (date_range, date) is reordered to (date, date_range) at evaluator-build time, so the channels swap.
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, dateType),
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(rangeFrom, rangeTo), new TestCaseSupplier.TypedData(dateVal, dateType, "date")),
                "RangeIntersectsPointEvaluator[point=Attribute[channel=1], range=Attribute[channel=0]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier dateDate(String name, long lhs, long rhs, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATETIME, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DATETIME, "rhs")
                ),
                "EqualsLongsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
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
        return new RangeIntersects(source, args.get(0), args.get(1));
    }
}
