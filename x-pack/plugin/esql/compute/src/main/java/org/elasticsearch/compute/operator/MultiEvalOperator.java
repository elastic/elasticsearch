/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Like {@link EvalOperator}, but evaluates several mutually independent expressions against the same input
 * {@link Page} and appends all of their result blocks to the page in a single step.
 * <p>
 *     {@link EvalOperator} appends one block at a time, and {@link Page#appendBlock(Block)} copies the whole
 *     block-reference array on every call. A chain of N such operators therefore copies about N&sup2;/2 block
 *     references per page, and those copies are not tracked by any circuit breaker. A query like
 *     {@code FILLNULL <value> ON *} expands to one such {@link EvalOperator} per column, so the cost grows
 *     quadratically with the number of columns. Batching the independent expressions of a single {@code EVAL}
 *     into one operator turns that into a single {@link Page#appendBlocks(Block[])} per batch.
 * </p>
 * <p>
 *     Only expressions that do not reference each other may share a batch; the caller
 *     ({@code LocalExecutionPlanner.planEval}) groups them accordingly.
 * </p>
 */
public class MultiEvalOperator extends AbstractPageMappingOperator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MultiEvalOperator.class);

    public record MultiEvalOperatorFactory(List<ExpressionEvaluator.Factory> evaluators) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            List<ExpressionEvaluator> built = new ArrayList<>(evaluators.size());
            boolean success = false;
            try {
                for (ExpressionEvaluator.Factory factory : evaluators) {
                    built.add(factory.get(driverContext));
                }
                MultiEvalOperator operator = new MultiEvalOperator(driverContext, built);
                success = true;
                return operator;
            } finally {
                if (success == false) {
                    Releasables.closeWhileHandlingException(built.toArray(ExpressionEvaluator[]::new));
                }
            }
        }

        @Override
        public String describe() {
            return "MultiEvalOperator[evaluators=" + evaluators + "]";
        }
    }

    private final DriverContext ctx;
    private final ExpressionEvaluator[] evaluators;
    private final long evaluatorsRamBytesUsed;

    public MultiEvalOperator(DriverContext ctx, List<ExpressionEvaluator> evaluators) {
        this.ctx = ctx;
        this.evaluators = evaluators.toArray(ExpressionEvaluator[]::new);
        long ramBytesUsed = 0;
        for (ExpressionEvaluator evaluator : this.evaluators) {
            ramBytesUsed += evaluator.baseRamBytesUsed();
        }
        this.evaluatorsRamBytesUsed = ramBytesUsed;
        ctx.breaker().addEstimateBytesAndMaybeBreak(BASE_RAM_BYTES_USED + evaluatorsRamBytesUsed, "ESQL");
    }

    @Override
    protected Page process(Page page) {
        Block[] blocks = new Block[evaluators.length];
        boolean success = false;
        try {
            for (int i = 0; i < evaluators.length; i++) {
                blocks[i] = evaluators[i].eval(page);
            }
            Page result = page.appendBlocks(blocks);
            success = true;
            return result;
        } finally {
            if (success == false) {
                // The evaluated blocks were not handed off to a Page; release the ones already produced.
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[evaluators=" + List.of(evaluators) + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            () -> Releasables.closeExpectNoException(evaluators),
            () -> ctx.breaker().addWithoutBreaking(-BASE_RAM_BYTES_USED - evaluatorsRamBytesUsed),
            super::close
        );
    }
}
