/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class SqrtTests extends AbstractScalarFunctionTestCase {
    public SqrtTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Sqrt of Double", () -> {
            // TODO: include larger values here
            double arg = randomDouble();
            return new TestCase(
                List.of(new TypedData(arg, DataTypes.DOUBLE, "arg")),
                "SqrtDoubleEvaluator[val=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(Math.sqrt(arg))
            );
        })));
    }

    private Matcher<Object> resultsMatcher(List<TypedData> typedData) {
        return equalTo(Math.sqrt((Double) typedData.get(0).data()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sqrt(source, args.get(0));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(numerics()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }
}
