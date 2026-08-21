/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Folds a row {@code LIMIT} sitting directly above an {@link EqlRelation} into the relation's request {@code size},
 * so {@code EQL idx "process where true" | LIMIT n} fetches ~n matches instead of the EQL engine default. Mirrors
 * {@link PushLimitToKnn}: the value is folded into the leaf but the {@link Limit} node is kept (the operator still
 * trims, and keeping it is harmless if the response over-returns).
 *
 * <p>Applies in every mode. The request {@code size} bounds the number of matches — events, sequences or samples —
 * and each match unnests to at least one row, so pushing {@code size = n} yields at least n rows and the kept Limit
 * still trims to n rows: it can never under-fetch. In ES|QL {@code LIMIT} bounds rows (so it may split a sequence
 * mid-match); {@code WITH {"size"}} is the way to bound whole matches. A limit that does not sit directly above the
 * relation (e.g. {@code | WHERE … | LIMIT n}) is not pushed: the source must over-scan so the downstream filter still
 * sees enough rows. When no limit is pushed the request falls back to the ES|QL result-truncation cap (see
 * {@code LocalExecutionPlanner#planEqlSource}).
 */
public class PushLimitIntoEqlRelation extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushLimitIntoEqlRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof EqlRelation relation && limit.limit().foldable()) {
            int value = (Integer) limit.limit().fold(ctx.foldCtx());
            // Keep the smallest limit; return the same instance when nothing changes so the fixed-point batch stops.
            if (relation.pushedLimit() == null || value < relation.pushedLimit()) {
                return limit.replaceChild(relation.withPushedLimit(value));
            }
        }
        return limit;
    }
}
