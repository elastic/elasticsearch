/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;

/**
 * SPI interface for filter pushdown support in external data sources.
 * <p>
 * Inspired by Lucene's {@code translatable()} pattern and Spark's {@code SupportsPushDownFilters},
 * this interface allows data sources to indicate which ESQL filter expressions they can handle
 * natively, enabling efficient predicate pushdown.
 * <p>
 * The pushdown flow works as follows:
 * <ol>
 *   <li>The optimizer calls {@link #pushFilters(List)} with AND-separated filter expressions</li>
 *   <li>The implementation converts supported expressions to source-specific filters</li>
 *   <li>The result contains an opaque pushed filter and any remainder expressions</li>
 *   <li>The pushed filter is stored in the physical plan node (opaque to core)</li>
 *   <li>During execution, the operator factory retrieves and applies the pushed filter</li>
 * </ol>
 * <p>
 * Since external sources execute on the coordinator only ({@code ExecutesOn.Coordinator}),
 * the pushed filter is never serialized - it's created during local physical optimization
 * and consumed immediately by the operator factory in the same JVM.
 */
public interface FilterPushdownSupport {

    /**
     * Attempt to push filters to the source.
     * <p>
     * The implementation should examine each expression and determine if it can be
     * converted to a source-specific filter. Expressions that can be fully pushed
     * should be converted; those that cannot should be returned as remainder.
     *
     * @param filters ESQL filter expressions (AND-separated)
     * @return result containing the pushed filter (opaque) and remainder expressions
     */
    PushdownResult pushFilters(List<Expression> filters);

    /**
     * Check if a single expression can be pushed to the source.
     * <p>
     * Similar to Lucene's {@code translatable()} returning YES/NO/RECHECK, this method
     * allows fine-grained control over which expressions are pushable.
     *
     * @param expr the expression to check
     * @return the pushability status
     */
    default Pushability canPush(Expression expr) {
        return Pushability.NO;
    }

    /**
     * Indicates whether an expression can be pushed to the data source.
     */
    enum Pushability {
        /**
         * The expression can be fully pushed to the source and removed from FilterExec.
         * The source guarantees correct evaluation.
         */
        YES,

        /**
         * The expression cannot be pushed to the source and must remain in FilterExec.
         */
        NO,

        /**
         * The expression can be pushed for efficiency (e.g., partition pruning),
         * but must also remain in FilterExec for correctness.
         * <p>
         * This is useful when the source can use the filter for optimization
         * (like skipping files) but cannot guarantee exact semantics.
         */
        RECHECK
    }

    /**
     * Result of attempting to push filters to a data source.
     *
     * @param pushedFilter source-specific filter object (opaque to core), or null if nothing was pushed
     * @param remainder expressions that could not be pushed and must remain in FilterExec
     */
    record PushdownResult(Object pushedFilter, List<Expression> remainder) {

        /**
         * Creates a result indicating no filters could be pushed.
         *
         * @param all the original filter expressions
         * @return a PushdownResult with null pushed filter and all expressions as remainder
         */
        public static PushdownResult none(List<Expression> all) {
            return new PushdownResult(null, all);
        }

        /**
         * Creates a result indicating all filters were pushed.
         *
         * @param pushedFilter the source-specific filter object
         * @return a PushdownResult with the pushed filter and empty remainder
         */
        public static PushdownResult all(Object pushedFilter) {
            return new PushdownResult(pushedFilter, List.of());
        }

        /**
         * Returns true if any filters were successfully pushed.
         */
        public boolean hasPushedFilter() {
            return pushedFilter != null;
        }

        /**
         * Returns true if there are remaining filters that couldn't be pushed.
         */
        public boolean hasRemainder() {
            return remainder != null && remainder.isEmpty() == false;
        }
    }
}
