/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

// begin generated imports
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.util.List;
import java.util.stream.IntStream;
// end generated imports

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Coalesce}.
 * This class is generated. Edit {@code X-InEvaluator.java.st} instead.
 */
abstract sealed class CoalesceIntEvaluator implements EvalOperator.ExpressionEvaluator permits
    CoalesceIntEvaluator.CoalesceIntEagerEvaluator, //
    CoalesceIntEvaluator.CoalesceIntLazyEvaluator {

    static ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator, List<Expression> children) {
        List<ExpressionEvaluator.Factory> childEvaluators = children.stream().map(toEvaluator::apply).toList();
        if (childEvaluators.stream().allMatch(ExpressionEvaluator.Factory::eagerEvalSafeInLazy)) {
            return new ExpressionEvaluator.Factory() {
                @Override
                public ExpressionEvaluator get(DriverContext context) {
                    return new CoalesceIntEagerEvaluator(context, childEvaluators.stream().map(x -> x.get(context)).toList());
                }

                @Override
                public String toString() {
                    return "CoalesceIntEagerEvaluator[values=" + childEvaluators + ']';
                }
            };
        }
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new CoalesceIntLazyEvaluator(context, childEvaluators.stream().map(x -> x.get(context)).toList());
            }

            @Override
            public String toString() {
                return "CoalesceIntLazyEvaluator[values=" + childEvaluators + ']';
            }
        };
    }

    protected final DriverContext driverContext;
    protected final List<EvalOperator.ExpressionEvaluator> evaluators;

    protected CoalesceIntEvaluator(DriverContext driverContext, List<EvalOperator.ExpressionEvaluator> evaluators) {
        this.driverContext = driverContext;
        this.evaluators = evaluators;
    }

    @Override
    public final IntBlock eval(Page page) {
        return entireBlock(page);
    }

    /**
     * Evaluate COALESCE for an entire {@link Block} for as long as we can, then shift to
     * {@link #perPosition} evaluation.
     * <p>
     * Entire Block evaluation is the "normal" way to run the compute engine,
     * just calling {@link EvalOperator.ExpressionEvaluator#eval}. It's much faster so we try
     * that first. For each evaluator, we {@linkplain EvalOperator.ExpressionEvaluator#eval} and:
     * </p>
     * <ul>
     *     <li>If the {@linkplain Block} doesn't have any nulls we return it. COALESCE done.</li>
     *     <li>If the {@linkplain Block} is only nulls we skip it and try the next evaluator.</li>
     *     <li>If this is the last evaluator we just return it. COALESCE done.</li>
     *     <li>
     *         Otherwise, the {@linkplain Block} has mixed nulls and non-nulls so we drop
     *         into a per position evaluator.
     *     </li>
     * </ul>
     */
    private IntBlock entireBlock(Page page) {
        int lastFullBlockIdx = 0;
        while (true) {
            IntBlock lastFullBlock = (IntBlock) evaluators.get(lastFullBlockIdx++).eval(page);
            if (lastFullBlockIdx == evaluators.size() || lastFullBlock.asVector() != null) {
                return lastFullBlock;
            }
            if (lastFullBlock.areAllValuesNull()) {
                // Result is all nulls and isn't the last result so we don't need any of it.
                lastFullBlock.close();
                continue;
            }
            // The result has some nulls and some non-nulls.
            return perPosition(page, lastFullBlock, lastFullBlockIdx);
        }
    }

    /**
     * Evaluate each position of the incoming {@link Page} for COALESCE
     * independently. Our attempt to evaluate entire blocks has yielded
     * a block that contains some nulls and some non-nulls and we have
     * to fill in the nulls with the results of calling the remaining
     * evaluators.
     * <p>
     * This <strong>must not</strong> return warnings caused by
     * evaluating positions for which a previous evaluator returned
     * non-null. These are positions that, at least from the perspective
     * of a compute engine user, don't <strong>have</strong> to be
     * evaluated. Put another way, this must function as though
     * {@code COALESCE} were per-position lazy. It can manage that
     * any way it likes.
     * </p>
     */
    protected abstract IntBlock perPosition(Page page, IntBlock lastFullBlock, int firstToEvaluate);

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[values=" + evaluators + ']';
    }

    @Override
    public final void close() {
        Releasables.closeExpectNoException(() -> Releasables.close(evaluators));
    }

    /**
     * Evaluates {@code COALESCE} eagerly per position if entire-block evaluation fails.
     * First we evaluate all remaining evaluators, and then we pluck the first non-null
     * value from each one. This is <strong>much</strong> faster than
     * {@link CoalesceIntLazyEvaluator} but will include spurious warnings if any of the
     * evaluators make them so we only use it for evaluators that are
     * {@link Factory#eagerEvalSafeInLazy safe} to evaluate eagerly
     * in a lazy environment.
     */
    static final class CoalesceIntEagerEvaluator extends CoalesceIntEvaluator {
        CoalesceIntEagerEvaluator(DriverContext driverContext, List<EvalOperator.ExpressionEvaluator> evaluators) {
            super(driverContext, evaluators);
        }

        @Override
        protected IntBlock perPosition(Page page, IntBlock lastFullBlock, int firstToEvaluate) {
            int positionCount = page.getPositionCount();
            IntBlock[] flatten = new IntBlock[evaluators.size() - firstToEvaluate + 1];
            try {
                flatten[0] = lastFullBlock;
                for (int f = 1; f < flatten.length; f++) {
                    flatten[f] = (IntBlock) evaluators.get(firstToEvaluate + f - 1).eval(page);
                }
                try (IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
                    position: for (int p = 0; p < positionCount; p++) {
                        for (IntBlock f : flatten) {
                            if (false == f.isNull(p)) {
                                result.copyFrom(f, p);
                                continue position;
                            }
                        }
                        result.appendNull();
                    }
                    return result.build();
                }
            } finally {
                Releasables.close(flatten);
            }
        }
    }

    /**
     * Evaluates {@code COALESCE} lazily per position if entire-block evaluation fails.
     * For each position we either:
     * <ul>
     *     <li>Take the non-null values from the {@code lastFullBlock}</li>
     *     <li>
     *         Evaluator the remaining evaluators one at a time, keeping
     *         the first non-null value.
     *     </li>
     * </ul>
     */
    static final class CoalesceIntLazyEvaluator extends CoalesceIntEvaluator {
        CoalesceIntLazyEvaluator(DriverContext driverContext, List<EvalOperator.ExpressionEvaluator> evaluators) {
            super(driverContext, evaluators);
        }

        @Override
        protected IntBlock perPosition(Page page, IntBlock lastFullBlock, int firstToEvaluate) {
            int positionCount = page.getPositionCount();
            try (IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
                position: for (int p = 0; p < positionCount; p++) {
                    if (lastFullBlock.isNull(p) == false) {
                        result.copyFrom(lastFullBlock, p, p + 1);
                        continue;
                    }
                    int[] positions = new int[] { p };
                    Page limited = new Page(
                        1,
                        IntStream.range(0, page.getBlockCount()).mapToObj(b -> page.getBlock(b).filter(positions)).toArray(Block[]::new)
                    );
                    try (Releasable ignored = limited::releaseBlocks) {
                        for (int e = firstToEvaluate; e < evaluators.size(); e++) {
                            try (IntBlock block = (IntBlock) evaluators.get(e).eval(limited)) {
                                if (false == block.isNull(0)) {
                                    result.copyFrom(block, 0);
                                    continue position;
                                }
                            }
                        }
                        result.appendNull();
                    }
                }
                return result.build();
            } finally {
                lastFullBlock.close();
            }
        }
    }
}
