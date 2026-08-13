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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for extracting the maximum (end) value of a range.
 */
public class RangeMaxTests extends AbstractScalarFunctionTestCase {
    public RangeMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            // Block stores [from, to); RANGE_MAX returns the exclusive upper bound directly
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
        }

        suppliers.add(new TestCaseSupplier("double range", List.of(DataType.DOUBLE_RANGE), () -> {
            double to = 42.25;
            var range = new DoubleRangeBlockBuilder.DoubleRange(-12.5, to);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(range, DataType.DOUBLE_RANGE, "field")),
                "RangeMaxDoubleEvaluator[range=" + read + "]",
                DataType.DOUBLE,
                equalTo(to)
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RangeMax(source, args.get(0));
    }
}
