/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BreakerTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        Expression expression = new Div(
            Source.synthetic("[1] / (long) 2"),
            AbstractFunctionTestCase.field("f", DataType.LONG),
            new Literal(Source.EMPTY, 2, DataType.INTEGER)
        );
        for (int b = 0; b < 136; b++) {
            params.add(new Object[] { ByteSizeValue.ofBytes(b), expression });
        }
        return params;
    }

    private final List<CircuitBreaker> breakers = new ArrayList<>();

    private final ByteSizeValue limit;
    private final Expression expression;

    public BreakerTests(ByteSizeValue limit, Expression expression) {
        this.limit = limit;
        this.expression = expression;
    }

    public void testBreaker() {
        DriverContext unlimited = driverContext(ByteSizeValue.ofGb(1));
        DriverContext context = driverContext(limit);
        EvalOperator.ExpressionEvaluator eval = AbstractScalarFunctionTestCase.evaluator(expression).get(context);
        try (Block b = unlimited.blockFactory().newConstantNullBlock(1)) {
            Exception e = expectThrows(CircuitBreakingException.class, () -> eval.eval(new Page(b)));
            assertThat(e.getMessage(), equalTo("over test limit"));
        }
    }

    /**
     * A {@link DriverContext} that won't throw {@link CircuitBreakingException}.
     */
    private DriverContext driverContext(ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }
}
