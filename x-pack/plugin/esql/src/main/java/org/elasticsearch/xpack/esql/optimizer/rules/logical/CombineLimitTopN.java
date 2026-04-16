/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.listSemanticEqualsIgnoreOrder;

/**
 * Combines a Limit immediately followed by a TopN into a single TopN.
 * Combines a LimitBy immediately followed by a TopNBy into a single TopNBy.
 * This is needed because {@link HoistRemoteEnrichTopN} can create new TopN nodes that are not covered by the previous rules.
 */
public final class CombineLimitTopN extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public CombineLimitTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(UnaryPlan plan) {
        if (plan instanceof Limit == false && plan instanceof LimitBy == false) {
            return plan;
        }
        if (plan instanceof Limit limit && limit.child() instanceof TopN topn) {
            int thisLimitValue = Foldables.limitValue(limit.limit(), limit.sourceText());
            int topNValue = Foldables.limitValue(topn.limit(), topn.sourceText());
            if (topNValue <= thisLimitValue) {
                return topn;
            } else {
                return new TopN(topn.source(), topn.child(), topn.order(), limit.limit(), topn.local());
            }
        }
        if (plan instanceof LimitBy limitBy
            && limitBy.child() instanceof TopNBy topnBy
            && listSemanticEqualsIgnoreOrder(limitBy.groupings(), topnBy.groupings())) {
            int thisLimitValue = Foldables.limitValue(limitBy.limitPerGroup(), limitBy.sourceText());
            int topNValue = Foldables.limitValue(topnBy.limitPerGroup(), topnBy.sourceText());
            if (topNValue <= thisLimitValue) {
                return topnBy;
            } else {
                return new TopNBy(topnBy.source(), topnBy.child(), topnBy.order(), limitBy.limitPerGroup(), topnBy.groupings());
            }
        }
        return plan;
    }
}
