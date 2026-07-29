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
 * Pushes a {@code LIMIT} sitting <b>directly</b> above an {@link EqlQuery} onto the source, forwarded to the EQL request
 * as {@code size} so the endpoint stops early instead of returning its default of 10. The match is restricted to the
 * immediate child so a limit never crosses a cardinality/order-changing command; the {@link Limit} node stays in place
 * (the pushed value is only an upper bound). For sequence queries EQL {@code size} counts sequences, so this never
 * under-fetches rows. A folded limit of {@code 0} is left for {@code SkipQueryOnLimitZero}.
 * <p>
 * {@link EqlQuery} is coordinator-only, so this rule is {@link OptimizerRules.CoordinatorOnly}.
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
