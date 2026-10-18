/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/** Executes the metric expression produced by the PromQL builder, including its warning contract. */
public class PredictLinearInputTests extends AbstractScalarFunctionTestCase {
    public PredictLinearInputTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> supplier) {
        testCase = supplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier(
                    "single",
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(7.0, DataType.DOUBLE, "metric")),
                        "MvSingleValueOrNull[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        equalTo(7.0)
                    )
                ),
                new TestCaseSupplier(
                    "multi",
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(List.of(1.0, 2.0), DataType.DOUBLE, "metric")),
                        "MvSingleValueOrNull[field=Attribute[channel=0]]",
                        DataType.DOUBLE,
                        nullValue()
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value")
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        var timestamp = new Literal(source, 0L, DataType.DATETIME);
        var context = new PromqlContext(timestamp, AggregateFunction.NO_WINDOW, timestamp, ConfigurationTestUtils.randomConfiguration());
        var prediction = (PredictLinear) PredictLinear.PROMQL_DEFINITION.esqlBuilder()
            .build(source, args.getFirst(), context, List.of(new Literal(source, 3.0, DataType.DOUBLE)));
        return prediction.field();
    }
}
