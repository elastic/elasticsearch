/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that the {@link ExpressionEvaluator.Factory}s generated for {@code @Evaluator}
 * methods release everything they built when {@link ExpressionEvaluator.Factory#get}
 * fails part way through. Nothing else has a reference to those things so nothing
 * else can release them.
 */
public class EvaluatorFactoryFailureTests extends ESTestCase {
    private final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1))
        .withCircuitBreaking();
    private final CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
    private final DriverContext context = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);

    /**
     * The child evaluators that the factories under test built.
     */
    private final List<TrackingEvaluator> built = new ArrayList<>();

    /**
     * A factory with a {@code THREAD_LOCAL} scratch and two children whose second child fails.
     */
    public void testSecondChildFails() {
        ExpressionEvaluator.Factory factory = new RepeatEvaluator.Factory(Source.EMPTY, this::scratch, this::succeed, this::fail);
        assertFailureReleasesEverything(factory, 1);
    }

    /**
     * A factory with a {@code THREAD_LOCAL} scratch and an array of children whose last child fails.
     */
    public void testChildInArrayFails() {
        int succeeding = between(0, 5);
        ExpressionEvaluator.Factory[] values = new ExpressionEvaluator.Factory[succeeding + 1];
        for (int i = 0; i < succeeding; i++) {
            values[i] = this::succeed;
        }
        values[succeeding] = this::fail;
        ExpressionEvaluator.Factory factory = new ConcatEvaluator.Factory(Source.EMPTY, this::scratch, values);
        assertFailureReleasesEverything(factory, succeeding);
    }

    /**
     * A factory whose {@code THREAD_LOCAL} scratch fails. The scratch isn't releasable
     * but the children built before it are.
     */
    public void testScratchFails() {
        ExpressionEvaluator.Factory factory = new LeftEvaluator.Factory(
            Source.EMPTY,
            c -> { throw failure(); },
            c -> { throw failure(); },
            this::succeed,
            this::succeed
        );
        assertFailureReleasesEverything(factory, 2);
    }

    public void testSuccessReleasesNothing() {
        ExpressionEvaluator.Factory factory = new RepeatEvaluator.Factory(Source.EMPTY, this::scratch, this::succeed, this::succeed);
        try (ExpressionEvaluator ignored = factory.get(context)) {
            assertThat(built, hasSize(2));
            for (TrackingEvaluator child : built) {
                assertFalse(child.closed);
            }
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        for (TrackingEvaluator child : built) {
            assertTrue(child.closed);
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private void assertFailureReleasesEverything(ExpressionEvaluator.Factory factory, int expectedBuilt) {
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> factory.get(context));
        assertThat(e.getMessage(), equalTo("simulated failure"));
        assertThat(built, hasSize(expectedBuilt));
        for (TrackingEvaluator child : built) {
            assertTrue(child.closed);
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private BreakingBytesRefBuilder scratch(DriverContext context) {
        return new BreakingBytesRefBuilder(context.breaker(), "test");
    }

    private ExpressionEvaluator succeed(DriverContext context) {
        assertThat(context, sameInstance(this.context));
        TrackingEvaluator evaluator = new TrackingEvaluator();
        built.add(evaluator);
        return evaluator;
    }

    private ExpressionEvaluator fail(DriverContext context) {
        throw failure();
    }

    private static CircuitBreakingException failure() {
        return new CircuitBreakingException("simulated failure", CircuitBreaker.Durability.TRANSIENT);
    }

    /**
     * A child evaluator that records being closed. It stands in for a real evaluator
     * because all the factories under test do with their children is build and close them.
     */
    private static class TrackingEvaluator implements ExpressionEvaluator {
        private boolean closed;

        @Override
        public Block eval(Page page) {
            throw new AssertionError("the factory must not evaluate its children");
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            assertFalse("closed twice", closed);
            closed = true;
        }
    }
}
