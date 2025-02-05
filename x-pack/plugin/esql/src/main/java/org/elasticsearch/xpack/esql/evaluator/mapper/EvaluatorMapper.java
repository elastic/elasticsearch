/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * Expressions that have a mapping to an {@link ExpressionEvaluator}.
 */
public interface EvaluatorMapper {
    interface ToEvaluator {
        ExpressionEvaluator.Factory apply(Expression expression);

        FoldContext foldCtx();

        default List<EsPhysicalOperationProviders.ShardContext> shardContexts() {
            throw new UnsupportedOperationException("Shard contexts should only be needed for evaluation operations");
        }
    }

    /**
     * Convert this into an {@link ExpressionEvaluator}.
     * <p>
     * Note for implementors:
     * If you are implementing this function, you should call the passed-in
     * lambda on your children, after doing any other manipulation (casting,
     * etc.) necessary.
     * </p>
     * <p>
     * Note for Callers:
     * If you are attempting to call this method, and you have an
     * {@link Expression} and a {@link Layout},
     * you likely want to call {@link EvalMapper#toEvaluator}
     * instead.  On the other hand, if you already have something that
     * looks like the parameter for this method, you should call this method
     * with that function.
     * </p>
     * <p>
     * Build an {@link ExpressionEvaluator.Factory} for the tree of
     * expressions rooted at this node. This is only guaranteed to return
     * a sensible evaluator if this node has a valid type. If this node
     * is a subclass of {@link Expression} then "valid type" means that
     * {@link Expression#typeResolved} returns a non-error resolution.
     * If {@linkplain Expression#typeResolved} returns an error then
     * this method may throw. Or return an evaluator that produces
     * garbage. Or return an evaluator that throws when run.
     * </p>
     */
    ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator);

    /**
     * Fold using {@link #toEvaluator} so you don't need a "by hand"
     * implementation of {@link Expression#fold}.
     */
    default Object fold(Source source, FoldContext ctx) {
        /*
         * OK! So! We're going to build a bunch of *stuff* that so that we can
         * call toEvaluator and use it without standing up an entire compute
         * engine.
         *
         * Step 1 is creation of a `toEvaluator` which we'll soon use to turn
         * the *children* of this Expression into ExpressionEvaluators. They
         * have to be foldable or else we wouldn't have ended up here. So!
         * We just call `fold` on them and turn the result of that into a
         * Block.
         *
         * If the tree of expressions is pretty deep that `fold` call will
         * likely end up being implemented by calling this method for the
         * child. That's fine. Recursion is how you process trees.
         */
        ToEvaluator foldChildren = new ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return driverContext -> new ExpressionEvaluator() {
                    @Override
                    public Block eval(Page page) {
                        return fromArrayRow(driverContext.blockFactory(), expression.fold(ctx))[0];
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public FoldContext foldCtx() {
                return ctx;
            }
        };

        /*
         * Step 2 is to create a DriverContext that we can pass to the above.
         * This DriverContext is mostly about delegating to the FoldContext.
         * That'll cause us to break if we attempt to allocate a huge amount
         * of memory. Neat.
         *
         * Specifically, we make a CircuitBreaker view of the FoldContext, then
         * we wrap it in a CircuitBreakerService so we can feed it to a BigArray
         * so we can feed *that* into a DriverContext. It's a bit hacky, but
         * that's what's going on here.
         */
        CircuitBreaker breaker = ctx.circuitBreakerView(source);
        BigArrays bigArrays = new BigArrays(null, new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                if (name.equals(CircuitBreaker.REQUEST) == false) {
                    throw new UnsupportedOperationException();
                }
                return breaker;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                throw new UnsupportedOperationException();
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                throw new UnsupportedOperationException();
            }
        }, CircuitBreaker.REQUEST).withCircuitBreaking();
        DriverContext driverCtx = new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));

        /*
         * Finally we can call toEvaluator on ourselves! It'll fold our children,
         * convert the result into Blocks, and then we'll run that with the memory
         * breaking DriverContext.
         *
         * Then, finally finally, we turn the result into a java object to be compatible
         * with the signature of `fold`.
         */
        Block block = toEvaluator(foldChildren).get(driverCtx).eval(new Page(1));
        if (block.getPositionCount() != 1) {
            throw new IllegalStateException("generated odd block from fold [" + block + "]");
        }
        return toJavaObject(block, 0);
    }
}
