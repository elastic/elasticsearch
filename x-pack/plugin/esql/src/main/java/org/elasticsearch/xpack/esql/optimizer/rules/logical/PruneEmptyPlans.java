/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

public final class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        // this basically is a decision to create a different shortcut for an empty plan for the right hand side of an InlineJoin
        // where the StubRelation is the EsRelation/LocalRelation equivalent
        return hasStubRelation(plan)
            ? new StubRelation(plan.source(), plan.output())
            : new LocalRelation(plan.source(), plan.output(), EmptyLocalSupplier.EMPTY);
    }

    private static boolean hasStubRelation(LogicalPlan plan) {
        for (var child : plan.children()) {
            if (child instanceof StubRelation) {
                return true;
            } else if (child instanceof InlineJoin) {
                // we don't want to find the StubRelation of a different InlineJoin
                return false;
            } else {
                return hasStubRelation(child);
            }
        }
        return false;
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        return plan.output().isEmpty() ? skipPlan(plan) : plan;
    }
}
