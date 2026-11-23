/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.eql.EqlRequests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Lets a {@code LIMIT}, including the implicit one, directly above an {@link EqlRelation} lower the EQL request {@code size}, as a pure
 * optimization: only for an event query with {@code result_position: head} and no EQL {@code head}/{@code tail} pipe. Each match
 * unnests to at least one row and the {@link Limit} is kept, so a pushed size never under-fetches. A limit not directly above the
 * relation (e.g. {@code | WHERE … | LIMIT n}) is not pushed: the source must over-scan so the downstream filter still sees enough rows.
 */
public class PushLimitIntoEqlRelation extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushLimitIntoEqlRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof EqlRelation relation && limit.limit().foldable() && canPush(relation)) {
            int value = (Integer) limit.limit().fold(ctx.foldCtx());
            // Keep the smallest limit; return the same instance when nothing changes so the fixed-point batch stops.
            if (relation.pushedLimit() == null || value < relation.pushedLimit()) {
                return limit.replaceChild(relation.withPushedLimit(value));
            }
        }
        return limit;
    }

    private static boolean canPush(EqlRelation relation) {
        return switch (relation.mode()) {
            case EVENT -> relation.hasExplicitLimit() == false && EqlRequests.readsFromHead(relation.options());
            // A smaller size can change which sequences come first: EQL stops at the first `size` completed ones, and an event with
            // multi-valued join keys completes several sequences at once, in an order that differs from the order they are returned in.
            case SEQUENCE -> false;
            case SAMPLE -> false;
        };
    }
}
