/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

public final class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), EmptyLocalSupplier.EMPTY);
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        return plan.output().isEmpty() ? skipPlan(plan) : plan;
    }
}
