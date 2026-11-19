/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToGaugeTests extends AbstractScalarFunctionTestCase {
    public ToGaugeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // long -> long (pass-through evaluator)
        TestCaseSupplier.forUnaryLong(suppliers, read, DataType.LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());

        // integer -> integer (pass-through evaluator)
        TestCaseSupplier.forUnaryInt(suppliers, read, DataType.INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());

        // double -> double (pass-through evaluator)
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            read,
            DataType.DOUBLE,
            d -> d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // counter_long -> long (strip counter annotation)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier("counter_long", () -> randomLongBetween(1, Long.MAX_VALUE), DataType.COUNTER_LONG)
            ),
            DataType.LONG,
            v -> v,
            List.of()
        );

        // counter_integer -> integer (strip counter annotation)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "counter_integer",
                    () -> randomIntBetween(1, Integer.MAX_VALUE),
                    DataType.COUNTER_INTEGER
                )
            ),
            DataType.INTEGER,
            v -> v,
            List.of()
        );

        // counter_double -> double (strip counter annotation)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "counter_double",
                    () -> randomDoubleBetween(0.0, Double.MAX_VALUE, true),
                    DataType.COUNTER_DOUBLE
                )
            ),
            DataType.DOUBLE,
            v -> v,
            List.of()
        );

        // aggregate_metric_double -> aggregate_metric_double (pass-through evaluator)
        TestCaseSupplier.forUnaryAggregateMetricDouble(suppliers, read, DataType.AGGREGATE_METRIC_DOUBLE, agg -> agg, List.of());

        // false: return type depends entirely on input type; an untyped null input has no inferable gauge variant
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToGauge(source, args.get(0));
    }
}
