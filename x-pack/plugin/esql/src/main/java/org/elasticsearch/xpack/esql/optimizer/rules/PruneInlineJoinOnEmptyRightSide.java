/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

public final class PruneInlineJoinOnEmptyRightSide extends OptimizerRules.OptimizerRule<InlineJoin> {

    @Override
    protected LogicalPlan rule(InlineJoin plan) {
        return plan.right() instanceof LocalRelation lr ? InlineJoin.inlineData(plan, lr) : plan;
    }
}
