/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Filters rows from input {@link Page}s. Keeps things that evaluate to {@code true}
 * and discards things that return {@code false} or {@code null}.
 * {@snippet lang="txt" :
 * ┌──────┬──────────┬────────────┐    ┌──────┬──────────┬────────────┐
 * │  ref │ class    │ discovered │    │  ref │ class    │ discovered │
 * ├──────┼──────────┼────────────┤    ├──────┼──────────┼────────────┤
 * │  173 │ Euclid   │ 1993-01-01 │    │  173 │ Euclid   │ 1993-01-01 │
 * │ 2317 │ Keter    │ 1922-01-01 │ -> │ 2639 │ Euclid   │ 2010-01-01 │
 * │ 2639 │ Euclid   │ 2010-01-01 │    │ 3001 │ Euclid   │ 2000-01-02 │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    └──────┴──────────┴────────────┘
 * │ 3001 │ Euclid   │ 2000-01-02 │
 * │ 5000 │ Safe     │ 2020-12-04 │
 * └──────┴──────────┴────────────┘
 * }
 * <p>
 *     {@link ExpressionEvaluator}s are the things that actually evaluate the filter.
 *     They form a tree. {@code GREATER_THAN(LENGTH(class), DATE_EXTRACT("month", discovered))}
 *     looks like:
 * </p>
 * {@snippet lang="txt" :
 *                  ┌──────────────┐
 *                  │ GREATER_THAN │
 *                  └──────┬───────┘
 *        ┌────────────────┴────────────────┐
 *        ▼                                 ▼
 *   ┌────────┐                      ┌──────────────┐
 *   │ LENGTH │                      │ DATE_EXTRACT │
 *   └────┬───┘                      └──────┬───────┘
 *        │                      ┌──────────┴──────────┐
 *        ▼                      ▼                     ▼
 * ┌─────────────┐     ┌──────────────────┐  ┌──────────────────┐
 * │ LOAD(class) │     │ LITERAL("month") │  │ LOAD(discovered) │
 * └─────────────┘     └──────────────────┘  └──────────────────┘
 * }
 * <p>
 *     Which evaluates to like:00b
 * </p>
 * {@snippet lang="txt" :
 * ┌──────┬──────────┬────────────┐    ┌──────┬──────────┬────────────┐
 * │  ref │ class    │ discovered │    │  ref │ class    │ discovered │
 * ├──────┼──────────┼────────────┤    ├──────┼──────────┼────────────┤
 * │  173 │ Euclid   │ 1993-01-01 │    │  173 │ Euclid   │ 1993-01-01 │
 * │ 2317 │ Keter    │ 1922-01-01 │    │ 2317 │ Keter    │ 1922-01-01 │
 * │ 2639 │ Euclid   │ 2010-01-01 │ -> │ 2639 │ Euclid   │ 2010-01-01 │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    │ 3000 │ Thaumiel │ 1971-01-01 │
 * │ 3001 │ Euclid   │ 2000-01-02 │    │ 3001 │ Euclid   │ 2000-01-02 │
 * │ 5000 │ Safe     │ 2020-12-04 │    └──────┴──────────┴────────────┘
 * └──────┴──────────┴────────────┘
 * }
 */
public class FilterOperator extends AbstractPageMappingOperator {

    private final ExpressionEvaluator evaluator;
    /**
     * Cached {@link #toString()} representation. The evaluator tree is immutable after construction,
     * so its string form is deterministic. {@link Driver} reads this on every status update; for
     * deep predicates (e.g. nested {@code LIKE} / boolean logic chains) recomputing it walks the
     * whole evaluator tree and can dominate CPU.
     */
    private final String description;

    public record FilterOperatorFactory(ExpressionEvaluator.Factory evaluatorSupplier) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new FilterOperator(evaluatorSupplier.get(driverContext));
        }

        @Override
        public String describe() {
            return "FilterOperator[evaluator=" + evaluatorSupplier + "]";
        }
    }

    public FilterOperator(ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
        this.description = "FilterOperator[evaluator=" + evaluator + ']';
    }

    @Override
    protected Page process(Page page) {
        int rowCount = 0;
        int[] positions = new int[page.getPositionCount()];

        try (BooleanBlock test = (BooleanBlock) evaluator.eval(page)) {
            if (test.areAllValuesNull()) {
                // All results are null which is like false. No values selected.
                return null;
            }
            // TODO we can detect constant true or false from the type
            // TODO or we could make a new method in bool-valued evaluators that returns a list of numbers
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (test.isNull(p) || test.getValueCount(p) != 1) {
                    // Null is like false
                    // And, for now, multivalued results are like false too
                    continue;
                }
                if (test.getBoolean(test.getFirstValueIndex(p))) {
                    positions[rowCount++] = p;
                }
            }

            if (rowCount == 0) {
                return null;
            }
            if (rowCount == page.getPositionCount()) {
                return page.shallowCopy();
            }
            positions = Arrays.copyOf(positions, rowCount);

            return page.filter(false, positions);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(evaluator, super::close);
    }
}
