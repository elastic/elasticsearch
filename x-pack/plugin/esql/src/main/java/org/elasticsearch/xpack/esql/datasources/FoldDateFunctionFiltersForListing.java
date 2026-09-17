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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;

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
            if (expr instanceof EsqlBinaryComparison comparison) {
                Expression left = foldCall(comparison.left(), configuration, functionRegistry);
                Expression right = foldCall(comparison.right(), configuration, functionRegistry);
                if (left != comparison.left() || right != comparison.right()) {
                    return comparison.replaceChildren(List.of(left, right));
                }
                return comparison;
            }
            if (expr instanceof In in) {
                Expression value = foldCall(in.value(), configuration, functionRegistry);
                boolean changed = value != in.value();
                List<Expression> list = new ArrayList<>(in.list().size());
                for (Expression item : in.list()) {
                    Expression folded = foldCall(item, configuration, functionRegistry);
                    changed |= folded != item;
                    list.add(folded);
                }
                return changed ? new In(in.source(), value, List.copyOf(list)) : in;
            }
            return expr;
        });
    }

    private static Expression foldCall(Expression expr, Configuration configuration, EsqlFunctionRegistry functionRegistry) {
        if (expr instanceof UnresolvedFunction uf) {
            return DateFunctionComparisonRewriter.tryFoldCall(uf, configuration, functionRegistry);
        }
        return expr;
    }
}
