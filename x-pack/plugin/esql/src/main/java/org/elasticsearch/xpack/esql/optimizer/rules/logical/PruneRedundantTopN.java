/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

/**
 * Removes redundant TopN operations from the logical plan to improve execution efficiency.
 * <p>
 * Multiple TopN nodes may appear in a query plan—particularly after optimization passes—
 * and some of them can be safely removed if they share the same sort order and are not separated
 * by operations that disrupt sorting semantics.
 * <p>
 * For instance:
 * <pre>
 * from test | sort x | limit 100 | sort x | limit 10
 * </pre>
 * Both <code>sort x | limit 100</code> and <code>sort x | limit 10</code> will be transformed into TopN nodes.
 * Since they sort by the same key, the two TopNs are merged into one that keeps the lower of the two limits
 * (see {@link TopN#mergeWithChildTopN}). The merged TopN is only local if both inputs were, so that a pipeline-breaking
 * (non-local) TopN is never replaced by a local one.
 */
public class PruneRedundantTopN extends OptimizerRules.ParameterizedOptimizerRule<TopN, LogicalOptimizerContext> {

    public PruneRedundantTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(TopN plan, LogicalOptimizerContext ctx) {
        TopN merged = plan.mergeWithChildTopN(ctx.foldCtx());
        while (merged != plan) {
            plan = merged;
            merged = plan.mergeWithChildTopN(ctx.foldCtx());
        }
        return plan;
    }
}
