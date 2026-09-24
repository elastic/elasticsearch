/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;
import java.util.function.Supplier;

/** Tests ownership of child resources when constructing, evaluating, or closing a sampling evaluator fails. */
public class HashOffsetResourceTests extends ComputeTestCase {

    public void testPartialEvaluationReleasesBlocks() {
        var blocks = blockFactory();
        var context = new DriverContext(blocks.bigArrays(), blocks, null);
        Block first = blocks.newConstantLongBlockWith(1, 1);
        Block second = blocks.newConstantLongBlockWith(2, 1);
        var failure = new IllegalStateException("evaluation failed");
        try (
            var evaluator = new HashOffset.HashOffsetEvaluator(
                context,
                List.of(new TestEvaluator(() -> first), new TestEvaluator(() -> second), new TestEvaluator(() -> {
                    throw failure;
                })),
                List.of(ElementType.LONG, ElementType.LONG, ElementType.LONG)
            )
        ) {
            assertSame(failure, expectThrows(IllegalStateException.class, () -> evaluator.eval(new Page(1))));
            assertTrue(first.isReleased());
            assertTrue(second.isReleased());
        }
    }

    public void testFactoryFailureClosesAllChildren() {
        var blocks = blockFactory();
        var context = new DriverContext(blocks.bigArrays(), blocks, null);
        var first = new TestEvaluator(() -> { throw new AssertionError("not evaluated"); });
        first.closeFailure = new IllegalStateException("close failed");
        var second = new TestEvaluator(() -> { throw new AssertionError("not evaluated"); });
        var failure = new IllegalStateException("factory failed");
        var factory = new HashOffset.HashOffsetEvaluatorFactory(
            List.of(c -> first, c -> second, c -> { throw failure; }),
            List.of(ElementType.LONG, ElementType.LONG, ElementType.LONG)
        );
        assertSame(failure, expectThrows(IllegalStateException.class, () -> factory.get(context)));
        assertTrue(first.closed);
        assertTrue(second.closed);
    }

    public void testConstructorFailureClosesChildren() {
        // A zero limit fails the encoder's real scratch allocation after the child has been constructed.
        var blocks = blockFactory(ByteSizeValue.ZERO);
        var context = new DriverContext(blocks.bigArrays(), blocks, null);
        var child = new TestEvaluator(() -> { throw new AssertionError("not evaluated"); });
        var factory = new HashOffset.HashOffsetEvaluatorFactory(List.of(c -> child), List.of(ElementType.LONG));
        expectThrows(CircuitBreakingException.class, () -> factory.get(context));
        assertTrue(child.closed);
    }

    public void testCloseFailureStillReleasesChildrenAndEncoder() {
        var blocks = blockFactory();
        var context = new DriverContext(blocks.bigArrays(), blocks, null);
        var first = new TestEvaluator(() -> { throw new AssertionError("not evaluated"); });
        first.closeFailure = new IllegalStateException("close failed");
        var second = new TestEvaluator(() -> { throw new AssertionError("not evaluated"); });
        var evaluator = new HashOffset.HashOffsetEvaluator(context, List.of(first, second), List.of(ElementType.LONG, ElementType.LONG));
        assertSame(first.closeFailure, expectThrows(IllegalStateException.class, evaluator::close));
        assertTrue(first.closed);
        assertTrue(second.closed);
        assertEquals(0L, blocks.breaker().getUsed());
    }

    /** Injects lifecycle failures that ordinary attribute evaluators cannot produce; blocks and breakers remain real. */
    private static class TestEvaluator implements ExpressionEvaluator {
        private final Supplier<Block> evaluation;
        private RuntimeException closeFailure;
        private boolean closed;

        TestEvaluator(Supplier<Block> evaluation) {
            this.evaluation = evaluation;
        }

        @Override
        public Block eval(Page page) {
            return evaluation.get();
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            assertFalse(closed);
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
