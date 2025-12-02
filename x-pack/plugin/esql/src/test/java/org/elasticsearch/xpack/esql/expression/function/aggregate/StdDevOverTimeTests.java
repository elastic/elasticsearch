/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

public class StdDevOverTimeTests extends AbstractFunctionTestCase {
    public StdDevOverTimeTests(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        testCase = testCaseSupplier.get();
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return StdDevTests.parameters();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StdDevOverTime(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW);
    }
}
