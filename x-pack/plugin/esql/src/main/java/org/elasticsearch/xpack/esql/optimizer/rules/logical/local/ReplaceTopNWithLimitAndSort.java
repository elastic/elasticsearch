/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

/**
 * Break TopN back into Limit + OrderBy to allow the order rules to kick in.
 */
public class ReplaceTopNWithLimitAndSort extends OptimizerRules.OptimizerRule<TopN> {
    public ReplaceTopNWithLimitAndSort() {
        super(UP);
    }

    @Override
    protected LogicalPlan rule(TopN plan) {
        return new Limit(plan.source(), plan.limit(), new OrderBy(plan.source(), plan.child(), plan.order()));
    }
}
