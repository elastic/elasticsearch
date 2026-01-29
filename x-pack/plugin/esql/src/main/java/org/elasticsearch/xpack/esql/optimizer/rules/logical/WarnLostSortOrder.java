/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.Rule;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Warns when a SORT or TopN is followed by a LOOKUP JOIN which does not preserve order,
 * and there is no subsequent SORT to restore the order.
 * <p>
 * For example:
 * <pre>
 * FROM test | SORT x | LOOKUP JOIN lookup ON key | LIMIT 10
 * </pre>
 * will produce a warning because the SORT order is lost by the LOOKUP JOIN.
 * <p>
 * But:
 * <pre>
 * FROM test | SORT x | LOOKUP JOIN lookup ON key | SORT y | LIMIT 10
 * </pre>
 * will NOT produce a warning because the final SORT restores a defined order.
 */
public final class WarnLostSortOrder extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // runs only once on the top node
        // does not do anything per node so no need to implement rule method
        warnLostSorts(plan, false);
        return plan;
    }

    private void warnLostSorts(LogicalPlan plan, boolean seenJoin) {
        if (plan instanceof OrderBy || plan instanceof TopN) {
            if (seenJoin) {
                Source source = plan.source();
                addWarning(
                    "Line {}:{}: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                        + "add another SORT after the LOOKUP JOIN if order is required",
                    source.source().getLineNumber(),
                    source.source().getColumnNumber()
                );
            }
            // Stop traversing this branch - we found a sort.
            // If we there is a lookup join under we are fine because this sort is above it
            // and we already printed a warning if there was lookup join above this node
            // so we can stop traversing the tree here
            return;
        }

        // Only warn for Join that doesn't preserve sort order
        // We do not warn for SortPreserving joins such as InlineStats
        boolean newSeenJoin = seenJoin || (plan instanceof Join && plan instanceof SortPreserving == false);

        for (LogicalPlan child : plan.children()) {
            warnLostSorts(child, newSeenJoin);
        }
    }
}
