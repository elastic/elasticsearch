/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;

/**
 * Evaluates an expression like {@code a + b} or {@code log(c)} one {@link Page} at a time.
 * <h2>Eval</h2>
 * <p>
 * The primary interface is the {@link ExpressionEvaluator#eval(Page)} method which
 * performs the actual evaluation. Generally implementations are built in a tree structure
 * with member {@link ExpressionEvaluator} for each of their parameters. So
 * {@linkplain ExpressionEvaluator#eval(Page)} will typically look like:
 * </p>
 * <pre>{@code
 *   Block lhs = this.lhs.eval(page);
 *   Block rhs = this.rhs.eval(page);
 *   try (Block.Builder result = ...) {
 *       for (int p = 0; p < lhs.getPositionCount(); p++) {
 *           result.add(doTheThing(lhs.get(p), rhs.get(p)));
 *       }
 *   }
 * }</pre>
 * <p>
 * There are hundreds of them and none of them look just like that, but that's the theory.
 * Get {@link Block}s from the children, then evaluate all the rows in a tight loop that
 * hopefully can get vectorized.
 * </p>
 * <p>
 * Implementations need not be thread safe. A new one is built for each {@link Driver} and
 * {@linkplain Driver}s are only ever run in one thread at a time. Many implementations
 * allocate "scratch" buffers for temporary memory that they reuse on each call to
 * {@linkplain ExpressionEvaluator#eval}.
 * </p>
 * <p>
 * Implementations <strong>must</strong> be ok with being called in by different threads,
 * though never at the same time. It's possible that the instance belonging to a particular
 * {@linkplain Driver} is called on thread {@code A} many times. And then the driver yields.
 * After a few seconds the {@linkplain Driver} could be woken on thread {@code B} and will
 * then call {@linkplain ExpressionEvaluator#eval(Page)}. No two threads will ever call
 * {@linkplain ExpressionEvaluator#eval(Page)} at the same time on the same instance.
 * This rarely matters, but some implementations that interact directly with Lucene will need
 * to check that the {@link Thread#currentThread()} is the same as the previous thread. If
 * it isn't they'll need to reinit Lucene stuff.
 * </p>
 * <h2>Memory tracking</h2>
 * <p>
 * Implementations should track their memory usage because it's possible for us a single
 * ESQL operation to make hundreds of them. Unlike with {@link Accountable} we have a
 * {@link ExpressionEvaluator#baseRamBytesUsed} which can be read just after creation
 * and is the sum of the ram usage of the tree of {@link ExpressionEvaluator}s while
 * "empty". If an implementation much allocate any scratch memory this is not included.
 * </p>
 * <p>
 * {@link ExpressionEvaluator#baseRamBytesUsed} memory is tracked in {@link EvalOperator}.
 * Implementation that don't allocate any scratch memory need only implement this and
 * use {@link DriverContext#blockFactory()} to build results.
 * </p>
 * <p>
 * Implementations that <strong>do</strong> allocate memory should use {@link BreakingBytesRefBuilder}
 * or {@link BigArrays} or some other safe allocation mechanism. If that isn't possible
 * they should communicate with the {@link CircuitBreaker} directly via {@link DriverContext#breaker}.
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
     *
     * @return the returned Block has its own reference and the caller is responsible for releasing it.
     */
    Block eval(Page page);

    /**
     * Heap used by the evaluator <strong>excluding</strong> any memory that's separately tracked
     * like the {@link BreakingBytesRefBuilder} used for string concat.
     */
    long baseRamBytesUsed();
}
