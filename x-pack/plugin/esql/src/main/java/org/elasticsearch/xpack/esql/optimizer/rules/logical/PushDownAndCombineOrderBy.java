/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.HashSet;
import java.util.Set;

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
            } else if (child instanceof Eval eval
                && eval.child() instanceof Highlight highlight
                && highlight.generatedAttributes().stream().noneMatch(orderBy.references()::contains)
                && canRunBefore(eval, highlight)) {
                    // Move the EVAL below the HIGHLIGHT so the sort can follow it. HIGHLIGHT appends its columns after the
                    // EVAL's, so the project restores the column order.
                    LogicalPlan pushed = highlight.replaceChild(orderBy.replaceChild(eval.replaceChild(highlight.child())));
                    return new Project(eval.source(), pushed, eval.output());
                }

        return orderBy;
    }

    /**
     * Whether the EVAL is independent of the HIGHLIGHT: it reads no generated column and defines no name that would shadow a
     * generated column or a column the HIGHLIGHT reads.
     */
    private static boolean canRunBefore(Eval eval, Highlight highlight) {
        if (highlight.generatedAttributes().stream().anyMatch(eval.references()::contains)) {
            return false;
        }
        Set<String> taken = new HashSet<>(highlight.references().names());
        highlight.generatedAttributes().forEach(a -> taken.add(a.name()));
        if (highlight.query() != null) {
            taken.addAll(highlight.query().references().names());
        }
        return eval.fields().stream().map(Alias::name).noneMatch(taken::contains);
    }
}
