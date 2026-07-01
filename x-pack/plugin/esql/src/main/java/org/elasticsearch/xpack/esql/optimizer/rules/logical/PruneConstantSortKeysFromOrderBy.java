/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
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
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/**
 * Removes sort keys that are provably constant — foldable, or a field missing from every shard — since
 * sorting by a constant conveys no order. Emptying the sort lets the surrounding {@link Limit}/{@link
 * LimitBy} stay a plain limit instead of becoming a {@code TopN}, which is what lets it push down to Lucene.
 */
public final class PruneConstantSortKeysFromOrderBy extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        return plan.transformDownSkipBranch((p, skipBranch) -> {
            // Pushed-down per-branch sorts keep _fork keys the coordinator merge needs; don't touch them.
            if (p instanceof Fork) {
                skipBranch.set(true);
                return p;
            }
            // Only prune a limit-bounded sort (Limit/LimitBy over OrderBy).
            if (p instanceof Limit || p instanceof LimitBy) {
                UnaryPlan limit = (UnaryPlan) p;
                if (limit.child() instanceof OrderBy orderBy) {
                    LogicalPlan simplified = simplifyOrderBy(orderBy, ctx);
                    return simplified == orderBy ? p : limit.replaceChild(simplified);
                }
            }
            return p;
        });
    }

    private static LogicalPlan simplifyOrderBy(OrderBy orderBy, LogicalOptimizerContext ctx) {
        // Aggregate/InlineJoin reuse input attribute ids, so a foldable below the grouping isn't constant above it.
        AttributeMap<Expression> foldables = RuleUtils.foldableReferencesSkipMVGroupings(
            orderBy.child(),
            ctx,
            p -> p instanceof Fork || p instanceof Aggregate || p instanceof InlineJoin
        );
        List<Order> keep = orderBy.order().stream().filter(o -> {
            // Never prune the Fork branch identifier. Within a per-branch fragment seen at the
            // local stage, `_fork` is a literal (e.g. "fork2"), but the coordinator's k-way merge
            // across branches depends on it remaining in the sort.
            if (o.child() instanceof Attribute attr && Fork.FORK_FIELD.equals(attr.name())) {
                return true;
            }
            Expression key = foldables.resolve(o.child(), o.child());
            return key.foldable() == false && isMissingField(key) == false;
        }).toList();
        if (keep.isEmpty()) {
            return orderBy.child();
        }
        if (keep.size() < orderBy.order().size()) {
            return new OrderBy(orderBy.source(), orderBy.child(), keep);
        }
        return orderBy;
    }

    private static boolean isMissingField(Expression expr) {
        return expr instanceof FieldAttribute fa && fa.field() instanceof MissingEsField;
    }
}
