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
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for constructing range values from scalar bounds.
 */
public class ToRangeTests extends AbstractScalarFunctionTestCase {
    public ToRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read0 = "Attribute[channel=0]";
        final String read1 = "Attribute[channel=1]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            suppliers.add(new TestCaseSupplier("basic range", List.of(DataType.DATETIME, DataType.DATETIME), () -> {
                long from = 1000L;
                long to = 2000L;
                var expected = new LongRangeBlockBuilder.LongRange(from, to);

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(from, DataType.DATETIME, "from"),
                        new TestCaseSupplier.TypedData(to, DataType.DATETIME, "to")
                    ),
                    "ToRangeLongEvaluator[from=" + read0 + ", to=" + read1 + "]",
                    DataType.DATE_RANGE,
                    equalTo(expected)
                );
            }));

            suppliers.add(new TestCaseSupplier("large epoch values", List.of(DataType.DATETIME, DataType.DATETIME), () -> {
                long from = 0L;
                long to = 1_000_000_000_000L;
                var expected = new LongRangeBlockBuilder.LongRange(from, to);

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(from, DataType.DATETIME, "from"),
                        new TestCaseSupplier.TypedData(to, DataType.DATETIME, "to")
                    ),
                    "ToRangeLongEvaluator[from=" + read0 + ", to=" + read1 + "]",
                    DataType.DATE_RANGE,
                    equalTo(expected)
                );
            }));
        }

        suppliers.add(new TestCaseSupplier("double range", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            double from = randomDoubleBetween(-1000.0, 0.0, true);
            double to = randomDoubleBetween(0.0, 1000.0, true);
            var expected = new DoubleRangeBlockBuilder.DoubleRange(from, to);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(from, DataType.DOUBLE, "from"),
                    new TestCaseSupplier.TypedData(to, DataType.DOUBLE, "to")
                ),
                "ToRangeDoubleEvaluator[from=" + read0 + ", to=" + read1 + "]",
                DataType.DOUBLE_RANGE,
                equalTo(expected)
            );
        }));

        suppliers.add(new TestCaseSupplier("unbounded double range", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            var expected = new DoubleRangeBlockBuilder.DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(Double.NEGATIVE_INFINITY, DataType.DOUBLE, "from"),
                    new TestCaseSupplier.TypedData(Double.POSITIVE_INFINITY, DataType.DOUBLE, "to")
                ),
                "ToRangeDoubleEvaluator[from=" + read0 + ", to=" + read1 + "]",
                DataType.DOUBLE_RANGE,
                equalTo(expected)
            );
        }));

        suppliers.add(invalidDoubleRange("equal bounds", 1.0, 1.0));
        suppliers.add(invalidDoubleRange("NaN lower bound", Double.NaN, 1.0));

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    private static TestCaseSupplier invalidDoubleRange(String name, double from, double to) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.DOUBLE, DataType.DOUBLE),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(from, DataType.DOUBLE, "from"),
                    new TestCaseSupplier.TypedData(to, DataType.DOUBLE, "to")
                ),
                "ToRangeDoubleEvaluator[from=Attribute[channel=0], to=Attribute[channel=1]]",
                DataType.DOUBLE_RANGE,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: 'from' [" + from + "] must be less than 'to' [" + to + "]")
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToRange(source, args.get(0), args.get(1));
    }
}
