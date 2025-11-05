/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameUtils.locallyUniqueTemporaryName;

/**
 * Pulls "up" an {@link OrderBy} node that is not preceded by a "sort breaker" (like LIMIT), but is preceded by an {@link InlineJoin}. The
 * {@code INLINE STATS} is sort agnostic, so the {@link OrderBy} can be pulled up without affecting the semantics of this type of join.
 * This is needed since otherwise the {@link OrderBy} would remain to be executed unbounded, which isn't supported.
 * Specifically, if it's preceded by a {@link Limit}, it will be merged into a {@link org.elasticsearch.xpack.esql.plan.logical.TopN} later
 * in the "cleanup" optimization stage.
 * <p>
 * See also {@link PruneRedundantOrderBy}.
 */
public final class PullUpOrderByBeforeInlineJoin extends OptimizerRules.OptimizerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformUp(LogicalPlan.class, PullUpOrderByBeforeInlineJoin::pullUpOrderByBeforeInlineJoin);
    }

    private static LogicalPlan pullUpOrderByBeforeInlineJoin(LogicalPlan plan) {
        if (plan instanceof InlineJoin inlineJoin) {
            Holder<OrderBy> orderByHolder = new Holder<>();
            inlineJoin.forEachDownMayReturnEarly((node, breakEarly) -> {
                if (node instanceof OrderBy orderBy) {
                    orderByHolder.set(orderBy);
                    breakEarly.set(true);
                } else {
                    breakEarly.set(isSortBreaker(node));
                }
            });

            OrderBy orderBy = orderByHolder.get();
            plan = orderBy == null ? plan : pullUpOrderByBeforeInlineJoin(inlineJoin, orderBy);
        }
        return plan;
    }

    private static LogicalPlan pullUpOrderByBeforeInlineJoin(InlineJoin inlineJoin, OrderBy orderBy) {
        List<Alias> evalAliases = new ArrayList<>();
        AttributeMap.Builder<Attribute> orderByAttrMapBuilder = AttributeMap.builder();
        // Collect all attributes referenced by the OrderBy that are no longer present in the InlineJoin input; i.e., that have been
        // dropped or overwritten before it.
        for (var order : orderBy.order()) {
            var orderExpression = order.child();
            var orderAttribute = Expressions.attribute(orderExpression); // TODO: can it be something else than an attribute?
            if (inlineJoin.inputSet().contains(orderAttribute) == false) { // attribute got dropped or overwritten
                var orderAlias = new Alias(orderBy.source(), locallyUniqueTemporaryName(orderAttribute.name()), orderExpression);
                evalAliases.add(orderAlias);
                orderByAttrMapBuilder.put(orderAttribute, Expressions.attribute(orderAlias));
            }
        }

        return evalAliases.isEmpty()
            ? orderBy.replaceChild(inlineJoin.transformUp(OrderBy.class, ob -> ob == orderBy ? orderBy.child() : ob))
            : pullUpRewritingMidProjections(inlineJoin, orderBy, evalAliases, orderByAttrMapBuilder.build());
    }

    /**
     * Pulls up the {@code OrderBy}, but leaves an {@code Eval} of temporary attribute(s) pointing to the field(s) in it in its place,
     * updates all the {@code Project}s in-between to include the temporary attributes and then adds another top projection,
     * that drops the temporary attributes.
     */
    private static LogicalPlan pullUpRewritingMidProjections(
        InlineJoin inlineJoin,
        OrderBy orderBy,
        List<Alias> evalAliases,
        AttributeMap<Attribute> attrToTempAliasMap
    ) {
        var eval = new Eval(orderBy.source(), orderBy.child(), evalAliases);
        var newInlineJoin = inlineJoin.transformUp(OrderBy.class, ob -> ob == orderBy ? eval : ob);
        var attrsKeySet = attrToTempAliasMap.keySet();
        newInlineJoin = newInlineJoin.transformUp(Project.class, p -> {
            List<NamedExpression> newProjections = new ArrayList<>(p.projections().size() + attrsKeySet.size());
            newProjections.addAll(p.projections());
            for (var attr : attrsKeySet) {
                if (p.inputSet().contains(attr) && p.outputSet().contains(attr) == false) {
                    newProjections.add(attrToTempAliasMap.resolve(attr));
                }
            }
            return p.withProjections(newProjections);
        });
        var newOrderBy = orderBy.replaceChild(newInlineJoin);
        newOrderBy = (OrderBy) newOrderBy.transformExpressionsOnly(Attribute.class, a -> attrToTempAliasMap.resolve(a, a));
        return new Project(orderBy.source(), newOrderBy, inlineJoin.output());
    }

    /**
     * Returns `true` if the {@code plan}'s position cannot be swapped with a SORT, `false` otherwise.
     */
    private static boolean isSortBreaker(LogicalPlan plan) {
        return switch (plan) {
            case Aggregate a -> true; // reducing node (SORT should be dropped by the `PruneRedundantOrderBy` rule)
            case Fork f -> true;
            case InlineJoin i -> false; // Note: this and the Join case need to keep their relative order!
            case Join j -> true; // LOOKUP JOIN, which can be generative, is surrogate'd with a "plain" Join node
            case MvExpand m -> true; // generative node, can destabilize the order
            case Limit l -> true;
            case TopN t -> true;
            default -> false;
        };
    }
}
