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
 * Tests for RANGE_CONTAINS(container, value) -> boolean. The function lowers to RANGE_WITHIN(value, container)
 * via {@code OnlySurrogateExpression.surrogate()}. The test framework builds the layout from the *original*
 * RangeContains tree (channel 0 = container, channel 1 = value); after the surrogate lowers to
 * RangeWithin(value, container) the generated evaluator reads value from channel 1 and container from
 * channel 0 — that's why the matchers below show channel 1 first.
 */
public class RangeContainsTests extends AbstractScalarFunctionTestCase {
    public RangeContainsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            // (date_range, date_range): first contains second iff RANGE_WITHIN(second, first) is true
            suppliers.add(rangeRange("first contains second", 100L, 2000L, 500L, 1500L, true));
            suppliers.add(rangeRange("identical ranges", 500L, 1500L, 500L, 1500L, true));
            suppliers.add(rangeRange("first does not contain second (overlap)", 100L, 1000L, 500L, 1500L, false));
            suppliers.add(rangeRange("disjoint ranges", 100L, 500L, 1000L, 1500L, false));

            // (date_range, date): range contains date iff date is within range
            suppliers.add(rangePoint("date inside range", 500L, 1500L, 1000L, true));
            suppliers.add(rangePoint("date outside range", 500L, 1500L, 2000L, false));
            suppliers.add(rangePoint("date at range start (inclusive)", 500L, 1500L, 500L, true));
            // Range is half-open [from, to); date == to is NOT contained. Use 1501 so that 1500 is inside [500, 1501).
            suppliers.add(rangePoint("date just before range end", 500L, 1501L, 1500L, true));
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static TestCaseSupplier rangeRange(String name, long aFrom, long aTo, long bFrom, long bTo, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATE_RANGE),
            // toEvaluator wires RangeWithinRangeEvaluator with arguments swapped: 'a' = our right (channel 1),
            // 'b' = our left (channel 0). The evaluator semantics are processRange(value, container).
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(aFrom, aTo), typedDateRange(bFrom, bTo)),
                "RangeWithinRangeEvaluator[a=Attribute[channel=1], b=Attribute[channel=0]]",
                DataType.BOOLEAN,
                equalTo(expected)
            )
        );
    }

    private static TestCaseSupplier rangePoint(String name, long rangeFrom, long rangeTo, long dateVal, boolean expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DATE_RANGE, DataType.DATETIME),
            // Same swap as above: point = our right (channel 1, the date), range = our left (channel 0).
            () -> new TestCaseSupplier.TestCase(
                List.of(typedDateRange(rangeFrom, rangeTo), new TestCaseSupplier.TypedData(dateVal, DataType.DATETIME, "date")),
                "RangeWithinPointEvaluator[point=Attribute[channel=1], range=Attribute[channel=0]]",
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
        return new RangeContains(source, args.get(0), args.get(1));
    }
}
