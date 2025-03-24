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

public class ETests extends AbstractScalarFunctionTestCase {
    public ETests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(
            true,
            List.of(
                new TestCaseSupplier(
                    "E Test",
                    List.of(),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(),
                        "LiteralsEvaluator[lit=2.718281828459045]",
                        DataType.DOUBLE,
                        equalTo(Math.E)
                    )
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new E(Source.EMPTY);
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(Math.E);
    }
}
