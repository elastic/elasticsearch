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
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator.locallyUniqueTemporaryName;

/**
 * Pulls "up" an {@link OrderBy} node that is not preceded by a "sort breaker" (like LIMIT), but is preceded by an {@link InlineJoin}. The
 * {@code INLINE STATS} is sort agnostic, so the {@link OrderBy} can be pulled up without affecting the semantics of this type of join.
 * This is needed since otherwise the {@link OrderBy} would remain to be executed unbounded, which isn't supported.
 * Specifically, if it's preceded by a {@link Limit}, it will be merged into a {@link org.elasticsearch.xpack.esql.plan.logical.TopN} later
 * in the "cleanup" optimization stage.
 * <p>
 * See also {@link PruneRedundantOrderBy}.
 */
public final class HoistOrderByBeforeInlineJoin extends OptimizerRules.OptimizerRule<LogicalPlan>
    implements
        OptimizerRules.CoordinatorOnly {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformUp(InlineJoin.class, HoistOrderByBeforeInlineJoin::hoistOrderByBeforeInlineJoin);
    }

    private static LogicalPlan hoistOrderByBeforeInlineJoin(InlineJoin inlineJoin) {
        Holder<OrderBy> orderByHolder = new Holder<>();
        inlineJoin.forEachDownMayReturnEarly((node, breakEarly) -> {
            if (node instanceof OrderBy orderBy) {
                orderByHolder.set(orderBy);
                breakEarly.set(true);
            } else {
                breakEarly.set(node instanceof SortPreserving == false);
            }
        });

        OrderBy orderBy = orderByHolder.get();
        return orderBy == null ? inlineJoin : hoistOrderByBeforeInlineJoin(inlineJoin, orderBy);
    }

    private static LogicalPlan hoistOrderByBeforeInlineJoin(InlineJoin inlineJoin, OrderBy orderBy) {
        List<Alias> evalAliases = new ArrayList<>();
        AttributeMap.Builder<Attribute> orderByAttrMapBuilder = AttributeMap.builder();
        // Collect all attributes referenced by the OrderBy that are no longer present in the InlineJoin input; i.e., that have been
        // dropped or overwritten before it.
        for (var order : orderBy.order()) {
            var orderExpression = order.child();
            var orderAttribute = Expressions.attribute(orderExpression); // TODO: can it be something else than an attribute?
            // attribute got dropped or overwritten before the InlineJoin, or _by_ it
            if (inlineJoin.inputSet().contains(orderAttribute) == false || inlineJoin.outputSet().contains(orderAttribute) == false) {
                var orderAlias = new Alias(orderBy.source(), locallyUniqueTemporaryName(orderAttribute.name()), orderExpression);
                evalAliases.add(orderAlias);
                orderByAttrMapBuilder.put(orderAttribute, Expressions.attribute(orderAlias));
            }
        }

        return evalAliases.isEmpty()
            ? orderBy.replaceChild(inlineJoin.transformUp(OrderBy.class, ob -> ob == orderBy ? orderBy.child() : ob))
            : hoistRewritingMidProjections(inlineJoin, orderBy, evalAliases, orderByAttrMapBuilder.build());
    }

    /**
     * Pulls up the {@code OrderBy}, but leaves an {@code Eval} of temporary attribute(s) pointing to the field(s) in it in its place,
     * updates all the {@code Project}s in-between to include the temporary attributes and then adds another top projection,
     * that drops the temporary attributes.
     */
    private static LogicalPlan hoistRewritingMidProjections(
        InlineJoin inlineJoin,
        OrderBy orderBy,
        List<Alias> evalAliases,
        AttributeMap<Attribute> attrToTempAliasMap
    ) {
        var eval = new Eval(orderBy.source(), orderBy.child(), evalAliases);
        var newInlineJoin = inlineJoin.transformUp(OrderBy.class, ob -> ob == orderBy ? eval : ob);
        var newOrderBy = orderBy.replaceChild(rewriteMidProjections(newInlineJoin, eval, attrToTempAliasMap));
        newOrderBy = (OrderBy) newOrderBy.transformExpressionsOnly(Attribute.class, a -> attrToTempAliasMap.resolve(a, a));
        return new Project(orderBy.source(), newOrderBy, inlineJoin.output());
    }

    private static LogicalPlan rewriteMidProjections(LogicalPlan plan, Eval eval, AttributeMap<Attribute> attrToTempAliasMap) {
        Holder<Boolean> evalVisited = new Holder<>(false);
        return plan.transformUp(lp -> {
            if (lp == eval) {
                evalVisited.set(true);
            } else if (lp instanceof Project project && evalVisited.get()) {
                List<NamedExpression> newProjections = new ArrayList<>(project.projections());
                for (var attrSet : attrToTempAliasMap.entrySet()) {
                    // check if the Project's input contains either the original attribute, as it might be on a subtree not containing the
                    // Eval, or the alias attribute itself, as current Project might be a DROP added by other rules (like PushDownEnrich)
                    if ((project.inputSet().contains(attrSet.getKey()) || project.inputSet().contains(attrSet.getValue()))
                        && project.projections().contains(attrSet.getValue()) == false) {
                        newProjections.add(attrSet.getValue());
                    }
                }
                lp = project.withProjections(newProjections);
            }
            return lp;
        });
    }
}
