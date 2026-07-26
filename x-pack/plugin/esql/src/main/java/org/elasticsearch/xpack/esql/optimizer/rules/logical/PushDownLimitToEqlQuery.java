/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.eql.EqlQuery;

/**
 * Pushes an ES|QL {@code LIMIT} that sits <b>directly</b> above an {@link EqlQuery} source onto that source, where it is
 * later forwarded to the EQL request as {@code size}. This lets the EQL endpoint stop early instead of always returning
 * its default {@code size} (10) events/sequences.
 * <p>
 * The match is intentionally restricted to the <b>immediate</b> child so we never push a limit through a
 * cardinality- or order-changing command (e.g. {@code WHERE}, {@code STATS}, {@code SORT}→{@code TopN}, {@code MV_EXPAND}):
 * for those the pushed {@code size} could drop rows that the query actually needs. The {@link Limit} node is kept in place
 * so the downstream {@code Limit} operator still enforces the exact ES|QL row count; the pushed value is only an upper
 * bound. For sequence queries EQL {@code size} counts sequences (each expands to several rows), so pushing the row limit
 * as {@code size} never under-fetches rows.
 * <p>
 * A folded limit of {@code 0} (or less) is left untouched so that {@code SkipQueryOnLimitZero} can short-circuit the whole
 * plan; we never send {@code size <= 0} to EQL.
 * <p>
 * {@link EqlQuery} is coordinator-only and never appears in a data-node fragment, so this rule is
 * {@link OptimizerRules.CoordinatorOnly} (skipped by the local logical optimizer).
 */
public final class PushDownLimitToEqlQuery extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext>
    implements
        OptimizerRules.CoordinatorOnly {

    public PushDownLimitToEqlQuery() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof EqlQuery eqlQuery && limit.limit().foldable()) {
            int limitValue = (int) limit.limit().fold(ctx.foldCtx());
            if (limitValue <= 0) {
                // Leave LIMIT 0 for SkipQueryOnLimitZero; do not push a non-positive size to EQL.
                return limit;
            }
            int newLimit = eqlQuery.limit() == null ? limitValue : Math.min(eqlQuery.limit(), limitValue);
            if (eqlQuery.limit() != null && eqlQuery.limit() == newLimit) {
                // Already pushed (fixed point) - avoid rebuilding an equal plan.
                return limit;
            }
            return limit.replaceChild(eqlQuery.withLimit(newLimit));
        }
        return limit;
    }
}
