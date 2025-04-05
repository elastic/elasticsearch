/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;

public final class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        var rootEmptyOutput = plan.output().isEmpty();
        if (rootEmptyOutput == false) {
            if (plan.anyMatch(intermediary -> intermediary.output().isEmpty())) {
                return plan.transformUp(LogicalPlan.class, p -> {
                    if (p instanceof EsRelation es && es.indexMode() != IndexMode.LOOKUP) {// a lookup should return all fields
                        return new EsRelation(es.source(), es.indexPattern(), es.indexMode(), es.indexNameWithModes(), NO_FIELDS);
                    } else if (p instanceof UnaryPlan up && up.output().isEmpty()) {
                        return up.child();
                    } else {
                        return p;
                    }
                });
            }
        } else {
            return skipPlan(plan);
        }
        return plan;
    }
}
