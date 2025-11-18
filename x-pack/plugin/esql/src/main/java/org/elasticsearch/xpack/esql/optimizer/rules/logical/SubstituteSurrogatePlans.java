/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;

public final class SubstituteSurrogatePlans extends OptimizerRules.OptimizerRule<LogicalPlan> {

    public SubstituteSurrogatePlans() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        if (plan instanceof SurrogateLogicalPlan surrogate) {
            plan = surrogate.surrogate();
        }
        return plan;
    }
}
