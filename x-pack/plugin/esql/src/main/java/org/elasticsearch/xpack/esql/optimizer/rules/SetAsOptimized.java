/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class SetAsOptimized extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        plan.forEachUp(SetAsOptimized::rule);
        return plan;
    }

    private static void rule(LogicalPlan plan) {
        if (plan.optimized() == false) {
            plan.setOptimized();
        }
    }
}
