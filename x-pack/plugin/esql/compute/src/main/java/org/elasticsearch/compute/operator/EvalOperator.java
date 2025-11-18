/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates a tree of functions for every position in the block, resulting in a
 * new block which is appended to the page.
 */
public class EvalOperator extends AbstractPageMappingOperator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EvalOperator.class);

    public record EvalOperatorFactory(ExpressionEvaluator.Factory evaluator) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new EvalOperator(driverContext, evaluator.get(driverContext));
        }

        @Override
        public String describe() {
            return "EvalOperator[evaluator=" + evaluator + "]";
        }
    }

    private final DriverContext ctx;
    private final ExpressionEvaluator evaluator;

    public EvalOperator(DriverContext ctx, ExpressionEvaluator evaluator) {
        this.ctx = ctx;
        this.evaluator = evaluator;
        ctx.breaker().addEstimateBytesAndMaybeBreak(BASE_RAM_BYTES_USED + evaluator.baseRamBytesUsed(), "ESQL");
    }

    @Override
    protected Page process(Page page) {
        Block block = evaluator.eval(page);
        return page.appendBlock(block);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[evaluator=" + evaluator + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            evaluator,
            () -> ctx.breaker().addWithoutBreaking(-BASE_RAM_BYTES_USED - evaluator.baseRamBytesUsed()),
            super::close
        );
    }

    /**
     * Evaluates an expression {@code a + b} or {@code log(c)} one {@link Page} at a time.
     * <h2>Eval</h2>
     * <p>
     *     The primary interface is the {@link ExpressionEvaluator#eval(Page)} method which
     *     performs the actual evaluation. Generally implementations are built in a tree structure
     *     with member {@link ExpressionEvaluator} for each of their parameters. So
     *     {@linkplain ExpressionEvaluator#eval(Page)} will typically look like:
     * </p>
     * <pre>{@code
     *   Block lhs = this.lhs.eval(page);
     *   Block rhs = this.lhs.eval(page);
     *   try (Block.Builder result = ...) {
     *       for (int p = 0; p < lhs.getPositionCount(); p++) {
     *           result.add(doTheThing(lhs.get(p), rhs.get(p)));
     *       }
     *   }
     * }</pre>
     * <p>
     *     There are hundreds of them and none of them look just like that, but that's the theory.
     *     Get {@link Block}s from the children, then evaluate all the rows in a tight loop that
     *     hopefully can get vectorized.
     * </p>
     * <p>
     *     Implementations need not be thread safe. A new one is built for each {@link Driver} and
     *     {@linkplain Driver}s are only ever run in one thread at a time. Many implementations
     *     allocate "scratch" buffers for temporary memory that they reuse on each call to
     *     {@linkplain ExpressionEvaluator#eval}.
     * </p>
     * <p>
     *     Implementations <strong>must</strong> be ok with being called in by different threads,
     *     though never at the same time. It's possible that the instance belonging to a particular
     *     {@linkplain Driver} is called on thread {@code A} many times. And then the driver yields.
     *     After a few seconds the {@linkplain Driver} could be woken on thread {@code B} and will
     *     then call {@linkplain ExpressionEvaluator#eval(Page)}. No two threads will ever call
     *     {@linkplain ExpressionEvaluator#eval(Page)} at the same time on the same instance.
     *     This rarely matters, but some implementations that interact directly with Lucene will need
     *     to check that the {@link Thread#currentThread()} is the same as the previous thread. If
     *     it isn't they'll need to reinit Lucene stuff.
     * </p>
     * <h2>Memory tracking</h2>
     * <p>
     *     Implementations should track their memory usage because it's possible for us a single
     *     ESQL operation to make hundreds of them. Unlike with {@link Accountable} we have a
     *     {@link ExpressionEvaluator#baseRamBytesUsed} which can be read just after creation
     *     and is the sum of the ram usage of the tree of {@link ExpressionEvaluator}s while
     *     "empty". If an implementation much allocate any scratch memory this is not included.
     * </p>
     * <p>
     *     {@link ExpressionEvaluator#baseRamBytesUsed} memory is tracked in {@link EvalOperator}.
     *     Implementation that don't allocate any scratch memory need only implement this and
     *     use {@link DriverContext#blockFactory()} to build results.
     * </p>
     * <p>
     *     Implementations that <strong>do</strong> allocate memory should use {@link BreakingBytesRefBuilder}
     *     or {@link BigArrays} or some other safe allocation mechanism. If that isn't possible
     *     they should communicate with the {@link CircuitBreaker} directly via {@link DriverContext#breaker}.
     * </p>
     */
    public interface ExpressionEvaluator extends Releasable {
        /**
         * A Factory for creating ExpressionEvaluators. This <strong>must</strong>
         * be thread safe.
         */
        interface Factory {
            ExpressionEvaluator get(DriverContext context);

            /**
             * {@code true} if it is safe and fast to evaluate this expression eagerly
             * in {@link ExpressionEvaluator}s that need to be lazy, like {@code CASE}.
             * This defaults to {@code false}, but expressions
             * that evaluate quickly and can not produce warnings may override this to
             * {@code true} to get a significant speed-up in {@code CASE}-like operations.
             */
            default boolean eagerEvalSafeInLazy() {
                return false;
            }
        }

        /**
         * Evaluate the expression.
         * @return the returned Block has its own reference and the caller is responsible for releasing it.
         */
        Block eval(Page page);

        /**
         * Heap used by the evaluator <strong>excluding</strong> any memory that's separately tracked
         * like the {@link BreakingBytesRefBuilder} used for string concat.
         */
        long baseRamBytesUsed();
    }

    private record ConstantNullEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantNull";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantNullEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantNullBlock(page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantNullEvaluator get(DriverContext context) {
                return new ConstantNullEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }

    public static final ExpressionEvaluator.Factory CONSTANT_NULL_FACTORY = new ConstantNullEvaluator.Factory();

    private record ConstantTrueEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantTrue";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantTrueEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantTrueEvaluator get(DriverContext context) {
                return new ConstantTrueEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }

    public static final ExpressionEvaluator.Factory CONSTANT_TRUE_FACTORY = new ConstantTrueEvaluator.Factory();

    private record ConstantFalseEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantFalse";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantFalseEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantBooleanBlockWith(false, page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantFalseEvaluator get(DriverContext context) {
                return new ConstantFalseEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }

    public static final ExpressionEvaluator.Factory CONSTANT_FALSE_FACTORY = new ConstantFalseEvaluator.Factory();
}
