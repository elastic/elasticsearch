/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Simplifies a {@link TopN} by removing sort keys that are provably no-ops, since sorting by a
 * constant does not affect the result order. If all keys are no-ops the {@link TopN} is replaced
 * entirely by a {@link Limit}. If only some keys are no-ops the {@link TopN} is rebuilt with just
 * the remaining keys.
 *
 * <p>A sort key is considered a no-op if:
 * <ul>
 *   <li>it resolves to a foldable (constant) expression via alias substitution, or</li>
 *   <li>it is a {@link FieldAttribute} whose underlying field is a {@link MissingEsField} —
 *       meaning the field is absent from every shard in the queried index (established at analysis
 *       time via field-caps) and will therefore be {@code null} for every document.</li>
 * </ul>
 *
 * <p>The rule intentionally skips {@link TopN} nodes that have been pushed down into {@link Fork}
 * branches by {@link PushDownLimitAndOrderByIntoFork}. Those inner TopNs carry sort keys that look
 * constant within one branch (e.g. {@code _fork = "fork1"}) but are required to preserve the sort
 * order expected by the outer coordinator-level TopN for the k-way merge.
 *
 * <p>When collecting foldable references from the subtree, traversal stops at both {@link Fork} and
 * {@link org.elasticsearch.xpack.esql.plan.logical.Aggregate} boundaries. {@code STATS BY} re-uses
 * the same attribute IDs for its grouping outputs as the input, so a foldable alias defined below
 * the aggregate (e.g. {@code color = ["blue","pink","yellow"]} in a {@code ROW}) must not be
 * mistaken for a constant post-aggregation value.
 */
public final class PruneConstantSortKeysFromTopN extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        return applyRecursive(plan, ctx);
    }

    // transformDown cannot stop its own recursion, so we use a manual traversal.
    // We stop at Fork nodes: TopNs pushed down into branches by PushDownLimitAndOrderByIntoFork
    // carry sort keys that are constant only within that branch (e.g. _fork = "fork1") but are
    // needed for the outer coordinator merge sort, so we must not prune them.
    // If ES|QL ever supports nested Fork, this is the place to handle TopNs that sit above
    // a nested inner Fork inside a branch.
    private static LogicalPlan applyRecursive(LogicalPlan plan, LogicalOptimizerContext ctx) {
        if (plan instanceof Fork) {
            return plan;
        }
        LogicalPlan result = plan instanceof TopN topN ? simplifyTopN(topN, ctx) : plan;
        List<LogicalPlan> newChildren = new ArrayList<>(result.children().size());
        boolean changed = false;
        for (LogicalPlan child : result.children()) {
            LogicalPlan newChild = applyRecursive(child, ctx);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? result.replaceChildren(newChildren) : result;
    }

    private static LogicalPlan simplifyTopN(TopN topN, LogicalOptimizerContext ctx) {
        AttributeMap<Expression> foldables = RuleUtils.foldableReferences(
            topN.child(),
            ctx,
            p -> p instanceof Fork || p instanceof Aggregate
        );
        List<Order> nonConstant = topN.order().stream().filter(o -> {
            Expression key = foldables.resolve(o.child(), o.child());
            return key.foldable() == false && isMissingField(key) == false;
        }).toList();
        if (nonConstant.isEmpty()) {
            return new Limit(topN.source(), topN.limit(), topN.child(), false, topN.local());
        }
        if (nonConstant.size() < topN.order().size()) {
            return new TopN(topN.source(), topN.child(), nonConstant, topN.limit(), topN.local());
        }
        return topN;
    }

    private static boolean isMissingField(Expression expr) {
        return expr instanceof FieldAttribute fa && fa.field() instanceof MissingEsField;
    }
}
