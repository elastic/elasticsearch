/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class IsFiniteTests extends AbstractRationalUnaryPredicateTests {
    public IsFiniteTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier("NaN", () -> makeTestCase(Double.NaN, false)),
                new TestCaseSupplier("positive Infinity", () -> makeTestCase(Double.POSITIVE_INFINITY, false)),
                new TestCaseSupplier("negative Infinity", () -> makeTestCase(Double.NEGATIVE_INFINITY, false)),
                new TestCaseSupplier("positive small double", () -> makeTestCase(randomDouble(), true)),
                new TestCaseSupplier("negative small double", () -> makeTestCase(-randomDouble(), true)),
                new TestCaseSupplier("positive large double", () -> makeTestCase(1 / randomDouble(), true)),
                new TestCaseSupplier("negative large double", () -> makeTestCase(-1 / randomDouble(), true))
            )
        );
    }

    private static TestCaseSupplier.TestCase makeTestCase(double val, boolean expected) {
        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(val, DataTypes.DOUBLE, "arg")),
            "IsFiniteEvaluator[val=Attribute[channel=0]]",
            DataTypes.BOOLEAN,
            equalTo(expected)
        );
    }

    @Override
    protected RationalUnaryPredicate build(Source source, Expression value) {
        return new IsFinite(source, value);
    }

    @Override
    protected Matcher<Object> resultMatcher(double d) {
        return equalTo(Double.isNaN(d) == false && Double.isInfinite(d) == false);
    }

}
