/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class PiTests extends AbstractScalarFunctionTestCase {
    public PiTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Pi Test", () -> {
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(1, DataTypes.INTEGER, "foo")),
                "LiteralsEvaluator[lit=3.141592653589793]",
                DataTypes.DOUBLE,
                equalTo(Math.PI)
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Pi(Source.EMPTY);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        assertThat(((DoubleBlock) value).asVector().getDouble(0), equalTo(Math.PI));
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(Math.PI);
    }
}
