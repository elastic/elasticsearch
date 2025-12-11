/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

/**
 * Combines a Limit immediately followed by a TopN into a single TopN.
 * This is needed because {@link HoistRemoteEnrichTopN} can create new TopN nodes that are not covered by the previous rules.
 */
public final class CombineLimitTopN extends OptimizerRules.OptimizerRule<Limit> {

    public CombineLimitTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit) {
        if (limit.child() instanceof TopN topn) {
            int thisLimitValue = Foldables.limitValue(limit.limit(), limit.sourceText());
            int topNValue = Foldables.limitValue(topn.limit(), topn.sourceText());
            if (topNValue <= thisLimitValue) {
                return topn;
            } else {
                return new TopN(topn.source(), topn.child(), topn.order(), limit.limit(), topn.local());
            }
        }
        if (limit.child() instanceof Project proj) {
            // It is possible that Project is sitting on top on TopN. Swap limit and project then.
            return proj.replaceChild(limit.replaceChild(proj.child()));
        }
        return limit;
    }
}
