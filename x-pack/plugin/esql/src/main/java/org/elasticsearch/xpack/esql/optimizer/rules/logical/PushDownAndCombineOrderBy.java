/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

public final class PushDownAndCombineOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {
    @Override
    protected LogicalPlan rule(OrderBy orderBy) {
        LogicalPlan child = orderBy.child();

        if (child instanceof OrderBy childOrder) {
            // combine orders
            return new OrderBy(orderBy.source(), childOrder.child(), orderBy.order());
        } else if (child instanceof Project) {
            return PushDownUtils.pushDownPastProject(orderBy);
        } else if (child instanceof Highlight highlight
            // HIGHLIGHT only appends the generated highlight_<field> columns, so a sort that does not read them is unaffected by
            // its position. Pushing it below the highlight (paired with PushDownAndCombineLimits) lets the sort and limit combine
            // into a TopN that runs before highlighting. A sort on a generated column has to stay above the highlight.
            && highlight.generatedAttributes().stream().noneMatch(orderBy.references()::contains)) {
                return highlight.replaceChild(orderBy.replaceChild(highlight.child()));
            }

        return orderBy;
    }
}
