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

/**
 * Evaluates functions for every position in the block, resulting in a
 * new block which is appended to the page.
 * {@snippet lang="txt" :
 * ┌──────┬──────────┬────────────┐    ┌──────┬──────────┬────────────┬─────────────┐
 * │  ref │ class    │ discovered │    │  ref │ class    │ discovered │ day_of_week │
 * ├──────┼──────────┼────────────┤    ├──────┼──────────┼────────────┼─────────────┤
 * │  173 │ Euclid   │ 1993-01-01 │    │  173 │ Euclid   │ 1993-01-01 │ Friday      │
 * │ 2317 │ Keter    │ 1922-01-01 │    │ 2317 │ Keter    │ 1922-01-01 │ Sunday      │
 * │ 2639 │ Euclid   │ 2010-01-01 │ -> │ 2639 │ Euclid   │ 2010-01-01 │ Friday      │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    │ 3000 │ Thaumiel │ 1971-01-01 │ Friday      │
 * │ 3001 │ Euclid   │ 2000-01-02 │    │ 3001 │ Euclid   │ 2000-01-02 │ Sunday      │
 * │ 5000 │ Safe     │ 2020-12-04 │    │ 5000 │ Safe     │ 2020-12-04 │ Friday      │
 * └──────┴──────────┴────────────┘    └──────┴──────────┴────────────┴─────────────┘
 * }
 * <p>
 *     {@link ExpressionEvaluator}s are the things that actually evaluate the function.
 *     They form a tree. {@code ADD(LENGTH(class) + DATE_EXTRACT("year", discovered))}
 *     looks like:
 * </p>
 * {@snippet lang="txt" :
 *                     ┌─────┐
 *                     │ ADD │
 *                     └──┬──┘
 *        ┌───────────────┴───────────────┐
 *        ▼                               ▼
 *   ┌────────┐                    ┌──────────────┐
 *   │ LENGTH │                    │ DATE_EXTRACT │
 *   └────┬───┘                    └──────┬───────┘
 *        │                    ┌──────────┴──────────┐
 *        ▼                    ▼                     ▼
 * ┌─────────────┐    ┌─────────────────┐  ┌──────────────────┐
 * │ LOAD(class) │    │ LITERAL("year") │  │ LOAD(discovered) │
 * └─────────────┘    └─────────────────┘  └──────────────────┘
 * }
 * <p>
 *     And it evaluates like:
 * </p>
 * {@snippet lang="txt" :
 * ┌──────┬──────────┬────────────┐    ┌──────┬──────────┬────────────┬────────┐
 * │  ref │ class    │ discovered │    │  ref │ class    │ discovered │ result │
 * ├──────┼──────────┼────────────┤    ├──────┼──────────┼────────────┼────────┤
 * │  173 │ Euclid   │ 1993-01-01 │    │  173 │ Euclid   │ 1993-01-01 │   1999 │
 * │ 2317 │ Keter    │ 1922-01-01 │    │ 2317 │ Keter    │ 1922-01-01 │   1927 │
 * │ 2639 │ Euclid   │ 2010-01-01 │ -> │ 2639 │ Euclid   │ 2010-01-01 │   2016 │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    │ 3000 │ Thaumiel │ 1971-01-01 │   1979 │
 * │ 3001 │ Euclid   │ 2000-01-02 │    │ 3001 │ Euclid   │ 2000-01-02 │   2006 │
 * │ 5000 │ Safe     │ 2020-12-04 │    │ 5000 │ Safe     │ 2020-12-04 │   2024 │
 * └──────┴──────────┴────────────┘    └──────┴──────────┴────────────┴────────┘
 * }
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
    /**
     * Cached {@link #toString()} representation. The evaluator tree is immutable after construction,
     * so its string form is deterministic. {@link Driver} reads this on every status update; caching
     * avoids walking the evaluator tree per call.
     */
    private final String description;

    public EvalOperator(DriverContext ctx, ExpressionEvaluator evaluator) {
        this.ctx = ctx;
        this.evaluator = evaluator;
        this.description = getClass().getSimpleName() + "[evaluator=" + evaluator + "]";
        ctx.breaker().addEstimateBytesAndMaybeBreak(BASE_RAM_BYTES_USED + evaluator.baseRamBytesUsed(), "ESQL");
    }

    @Override
    protected Page process(Page page) {
        Block block = evaluator.eval(page);
        return page.appendBlock(block);
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            evaluator,
            () -> ctx.breaker().addWithoutBreaking(-BASE_RAM_BYTES_USED - evaluator.baseRamBytesUsed()),
            super::close
        );
    }
}
