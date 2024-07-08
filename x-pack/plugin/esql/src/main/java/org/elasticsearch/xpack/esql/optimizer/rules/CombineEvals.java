/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Combine multiple Evals into one in order to reduce the number of nodes in a plan.
 * TODO: eliminate unnecessary fields inside the eval as well
 */
public final class CombineEvals extends OptimizerRules.OptimizerRule<Eval> {

    public CombineEvals() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Eval eval) {
        LogicalPlan plan = eval;
        if (eval.child() instanceof Eval subEval) {
            plan = new Eval(eval.source(), subEval.child(), CollectionUtils.combine(subEval.fields(), eval.fields()));
        }
        return plan;
    }
}
