/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFunctionComparisonRewriter;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Listing-only copy of {@link Filter} conjuncts with all-literal {@code date_extract} /
 * {@code date_trunc} calls folded to {@link Literal}s so {@link PartitionFilterHintExtractor}
 * can see attribute-vs-literal comparisons. Does not mutate the session plan; the analyzer
 * still folds the original unresolved tree later.
 */
public final class FoldDateFunctionFiltersForListing {

    private FoldDateFunctionFiltersForListing() {}

    /**
     * Returns a plan whose {@link Filter} conditions have foldable date-function sides replaced.
     * Unchanged filters (and the original plan, when nothing folds) are reused by identity.
     * {@code transformUp} folds nested calls inside-out so {@code DATE_EXTRACT(..., DATE_TRUNC(...))}
     * can become a literal before the comparison is seen.
     */
    public static LogicalPlan fold(LogicalPlan plan, Configuration configuration, EsqlFunctionRegistry functionRegistry) {
        return plan.transformUp(Filter.class, filter -> {
            Expression folded = foldFilterCondition(filter.condition(), configuration, functionRegistry);
            return folded == filter.condition() ? filter : filter.with(folded);
        });
    }

    private static Expression foldFilterCondition(
        Expression condition,
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry
    ) {
        return condition.transformUp(expr -> {
            if (expr instanceof UnresolvedFunction uf) {
                return DateFunctionComparisonRewriter.tryFoldCall(uf, configuration, functionRegistry);
            }
            return expr;
        });
    }
}
