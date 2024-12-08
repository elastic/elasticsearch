/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class TauTests extends AbstractScalarFunctionTestCase {
    public TauTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Tau Test", List.of(DataType.INTEGER), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(1, DataType.INTEGER, "foo")),
                "LiteralsEvaluator[lit=6.283185307179586]",
                DataType.DOUBLE,
                equalTo(Tau.TAU)
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Tau(Source.EMPTY);
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(Math.PI * 2);
    }
}
