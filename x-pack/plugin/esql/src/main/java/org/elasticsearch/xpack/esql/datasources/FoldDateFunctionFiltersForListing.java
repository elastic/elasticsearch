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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFunctionComparisonRewriter;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Listing-only copy of {@link Filter} conjuncts with all-literal {@code date_extract} /
 * {@code date_trunc} / {@code year} / {@code month} / {@code day} / {@code hour} /
 * {@code to_datetime} calls folded to {@link Literal}s so {@link PartitionFilterHintExtractor}
 * can see attribute-vs-literal comparisons. Does not mutate the session plan; the analyzer
 * still folds the original unresolved tree later.
 * <p>
 * The fold itself is {@link DateFunctionComparisonRewriter#foldConstants}, shared with the time range that index
 * resolution reads from a query. It also folds the other shapes of a constant date: {@code now()},
 * {@code to_date_nanos}, {@code date_parse}, and a time span added to or subtracted from a date.
 * <p>
 * {@code to_datetime} is folded in both of the spellings a query can use. A named call arrives
 * unresolved and is folded by name; an inline {@code ::datetime} cast is built by the parser as a
 * {@link ToDatetime} node, so it is matched on the node instead. Folding only the named form left a
 * cast-written filter with no hint and the whole glob listed.
 * <p>
 * A range such as {@code year > YEAR("2024-01-01")} folds to the same {@code GREATER_THAN}
 * hint as {@code year > 2024}. That does not rewrite the listing glob
 * ({@code canRewriteGlob} is {@code EQUALS}/{@code IN} only); range filtering is
 * split-discovery.
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
            Expression folded = DateFunctionComparisonRewriter.foldConstants(filter.condition(), configuration, functionRegistry);
            return folded == filter.condition() ? filter : filter.with(folded);
        });
    }
}
