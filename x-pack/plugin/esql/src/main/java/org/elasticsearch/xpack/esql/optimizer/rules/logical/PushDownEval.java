/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class PushDownEval extends OptimizerRules.OptimizerRule<Eval> {
    @Override
    protected LogicalPlan rule(Eval eval) {
        // Merge with the Eval below (as CombineEvals would) so the pushed-down Eval, which the transform revisits, keeps moving past
        // the next Project/OrderBy in this pass. Otherwise a chain of Evals interleaved with Projects needs one pass per Project.
        //
        // This is here and not in PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy because only Evals can be merged: the other
        // callers (Dissect, Grok, Enrich, inference and compound-output plans) have no combine rule, so it would be dead code for them.
        //
        // It duplicates CombineEvals because the two Evals only become adjacent while this rule is pushing, after CombineEvals has
        // already run in the current pass, and the merge must happen before the transform revisits the pushed-down Eval.
        while (eval.child() instanceof Eval subEval) {
            eval = new Eval(eval.source(), subEval.child(), CollectionUtils.combine(subEval.fields(), eval.fields()));
        }
        return PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(eval);
    }
}
