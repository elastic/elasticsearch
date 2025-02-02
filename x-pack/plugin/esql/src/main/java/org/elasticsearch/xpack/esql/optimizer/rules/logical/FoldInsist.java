/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

// FIXME(gal, do-not-merge!) document
public final class FoldInsist extends OptimizerRules.OptimizerRule<Insist> {
    public FoldInsist() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Insist insist) {
        assert insist.child() instanceof EsRelation : "INSIST should be on top of a relation (see LogicalPlanBuilder)";
        var relation = (EsRelation) insist.child();
        return relation.withAttributes(insist.output());
    }
}
