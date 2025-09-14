/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

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
 * Since they sort by the same key and the latter applies a stricter (or equal) limit,
 * the first TopN becomes redundant and can be pruned.
 */
public class PruneRedundantTopN extends OptimizerRules.ParameterizedOptimizerRule<TopN, LogicalOptimizerContext> {

    public PruneRedundantTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(TopN plan, LogicalOptimizerContext ctx) {
        Set<LogicalPlan> redundant = findRedundantSort(plan, ctx);
        if (redundant.isEmpty()) {
            return plan;
        }
        return plan.transformDown(p -> redundant.contains(p) ? ((UnaryPlan) p).child() : p);
    }

    /**
     * breadth-first recursion to find redundant TopNs in the children tree.
     * Returns an identity set (we need to compare and prune the exact instances)
     */
    private Set<LogicalPlan> findRedundantSort(LogicalPlan plan, LogicalOptimizerContext ctx) {
        Set<LogicalPlan> result = Collections.newSetFromMap(new IdentityHashMap<>());

        Deque<LogicalPlan> toCheck = new ArrayDeque<>();
        toCheck.push(((UnaryPlan) plan).child());

        while (toCheck.isEmpty() == false) {
            LogicalPlan p = toCheck.pop();
            if (p instanceof TopN childTopN && plan instanceof TopN parentTopN) {
                // Check if a child TopN is redundant compared to a parent TopN.
                // A child TopN is redundant if it matches the parent's sort order and has a greater or equal limit.
                // Although `PushDownAndCombineLimits` is expected to have propagated the stricter (lower) limit,
                // we still compare limit values here to ensure correctness and avoid relying solely on prior optimizations.
                // This limit check should always pass, but we validate it explicitly for robustness.
                if (childTopN.order().equals(parentTopN.order())
                    && (int) parentTopN.limit().fold(ctx.foldCtx()) <= (int) childTopN.limit().fold(ctx.foldCtx())) {
                    result.add(childTopN);
                    toCheck.push(childTopN.child());
                }
            } else if (p instanceof SortAgnostic) {
                for (LogicalPlan child : p.children()) {
                    toCheck.push(child);
                }
            }
        }
        return result;
    }
}
