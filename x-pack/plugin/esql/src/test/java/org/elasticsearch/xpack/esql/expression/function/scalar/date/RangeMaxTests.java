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
 * Tests for RangeMax(date_range) -> date.
 * Validates that the maximum (end) value of a date_range is correctly extracted.
 */
public class RangeMaxTests extends AbstractScalarFunctionTestCase {
    public RangeMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Basic test cases
        suppliers.add(new TestCaseSupplier("basic range", List.of(DataType.DATE_RANGE), () -> {
            long from = 1000L;
            long to = 2000L;
            var range = new LongRangeBlockBuilder.LongRange(from, to);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                "RangeMaxEvaluator[range=" + read + "]",
                DataType.DATETIME,
                equalTo(to)
            );
        }));

        // Test with different ranges
        suppliers.add(new TestCaseSupplier("large range", List.of(DataType.DATE_RANGE), () -> {
            long from = 0L;
            long to = 1_000_000_000_000L;
            var range = new LongRangeBlockBuilder.LongRange(from, to);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                "RangeMaxEvaluator[range=" + read + "]",
                DataType.DATETIME,
                equalTo(to)
            );
        }));

        suppliers.add(new TestCaseSupplier("small range", List.of(DataType.DATE_RANGE), () -> {
            long from = 500L;
            long to = 501L;
            var range = new LongRangeBlockBuilder.LongRange(from, to);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                "RangeMaxEvaluator[range=" + read + "]",
                DataType.DATETIME,
                equalTo(to)
            );
        }));

        // Test with null range
        suppliers.add(new TestCaseSupplier("null range", List.of(DataType.DATE_RANGE), () -> {
            var range = new LongRangeBlockBuilder.LongRange(null, null);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                "RangeMaxEvaluator[range=" + read + "]",
                DataType.DATETIME,
                equalTo(null)
            );
        }));

        // Test with null to value
        suppliers.add(new TestCaseSupplier("null to value", List.of(DataType.DATE_RANGE), () -> {
            var range = new LongRangeBlockBuilder.LongRange(1000L, null);

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DATE_RANGE, "field")),
                "RangeMaxEvaluator[range=" + read + "]",
                DataType.DATETIME,
                equalTo(null)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RangeMax(source, args.get(0));
    }
}
