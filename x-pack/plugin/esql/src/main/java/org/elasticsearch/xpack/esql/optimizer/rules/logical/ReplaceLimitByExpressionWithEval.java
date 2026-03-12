/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Two-phase rewrite of {@link Limit} groupings (LIMIT BY):
 * <ol>
 *   <li><b>Prune foldable groupings.</b> A foldable expression evaluates to the same constant for every row, so it has
 *       no grouping effect. If all groupings are foldable the LIMIT BY degenerates to a plain LIMIT.</li>
 *   <li><b>Extract non-attribute expressions into a synthetic {@link Eval}.</b>
 *       {@code SORT salary | LIMIT N BY languages * 2}
 *       becomes
 *       {@code SORT salary | EVAL $$limit_by_0 = languages * 2 | LIMIT N BY $$limit_by_0}
 *       The eval is inserted below the {@link OrderBy} to preserve the {@code Limit -> OrderBy} structure
 *       needed by {@link ReplaceLimitAndSortAsTopN}.</li>
 * </ol>
 */
public final class ReplaceLimitByExpressionWithEval extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit limit) {
        if (limit.groupings().isEmpty()) {
            return limit;
        }

        // Phase 1: prune foldable groupings -- they evaluate to a constant and have no grouping effect.
        // Groupings arrive from the parser as either raw Attributes (e.g. languages) or Alias nodes wrapping
        // the expression (e.g. Alias("languages * 2", Mul(...))). Alias.foldable() is always false, so we
        // unwrap to check the child.
        List<Expression> newGroupings = new ArrayList<>();
        for (Expression g : limit.groupings()) {
            Expression toCheck = g instanceof Alias as ? as.child() : g;
            if (toCheck.foldable() == false) {
                newGroupings.add(g);
            }
        }
        if (newGroupings.isEmpty()) {
            // All groupings were foldable -- degenerate to plain LIMIT
            return new Limit(limit.source(), limit.limit(), limit.child(), List.of(), limit.duplicated(), limit.local());
        }

        // Phase 2: extract non-attribute grouping expressions into a synthetic Eval.
        // Groupings that are already Attributes are left as-is. Alias nodes are moved directly into
        // the eval list (mirroring ReplaceAggregateNestedExpressionWithEval) rather than wrapped in a
        // second Alias.
        int counter = 0;
        int size = newGroupings.size();
        List<Alias> evals = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as) {
                // Move the existing alias into the eval and replace the grouping with its attribute
                evals.add(as);
                newGroupings.set(i, as.toAttribute());
            } else if (g instanceof Attribute == false) {
                var name = rawTemporaryName("LIMIT BY", String.valueOf(i), String.valueOf(counter++));
                var alias = new Alias(g.source(), name, g, null, true);
                evals.add(alias);
                newGroupings.set(i, alias.toAttribute());
            }
            // else: g is already an Attribute, leave it as-is
        }

        if (evals.isEmpty()) {
            // Groupings changed (foldables pruned) but all remaining are already attributes -- update the Limit
            if (newGroupings.size() != limit.groupings().size()) {
                return new Limit(limit.source(), limit.limit(), limit.child(), newGroupings, limit.duplicated(), limit.local());
            }
            return limit;
        }

        // Insert Eval below OrderBy to preserve Limit -> OrderBy structure for ReplaceLimitAndSortAsTopN
        var originalOutput = limit.output();
        var child = limit.child();
        LogicalPlan evalChild;
        if (child instanceof OrderBy orderBy) {
            evalChild = new OrderBy(orderBy.source(), new Eval(orderBy.source(), orderBy.child(), evals), orderBy.order());
        } else {
            evalChild = new Eval(limit.source(), child, evals);
        }

        var newLimit = new Limit(limit.source(), limit.limit(), evalChild, newGroupings, limit.duplicated(), limit.local());
        return new Project(limit.source(), newLimit, originalOutput);
    }
}
