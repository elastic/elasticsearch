/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class AbstractAggregateDeduplicator extends OptimizerRules.OptimizerRule<Aggregate> {
    protected AbstractAggregateDeduplicator() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes = new HashMap<>(
            aggregate.groupings().size()
        );
        AttributeMap<Expression> aliases = buildAliases(aggregate, nonEvalGroupingAttributes);
        // break down each aggregate into AggregateFunction and/or grouping key
        // preserve the projection at the end
        List<? extends NamedExpression> aggs = aggregate.aggregates();

        // root/naked aggs
        Map<AggregateFunction, Alias> rootAggs = Maps.newLinkedHashMapWithExpectedSize(aggs.size());
        // evals (original expression relying on multiple aggs)
        List<Alias> newEvals = new ArrayList<>();
        List<NamedExpression> newProjections = new ArrayList<>();
        // track the aggregate aggs (including grouping which is not an AggregateFunction)
        List<NamedExpression> newAggs = new ArrayList<>();

        Holder<Boolean> changed = new Holder<>(false);
        int[] counter = new int[] { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias as) {
                processAlias(as, aliases, rootAggs, newAggs, newProjections, newEvals, changed, counter, nonEvalGroupingAttributes);
            } else {
                newAggs.add(agg);
                newProjections.add(agg.toAttribute());
            }
        }
        if (!changed.get()) {
            return aggregate;
        }
        LogicalPlan plan = aggregate;
        if (changed.get()) {
            Source source = aggregate.source();
            plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
            if (newEvals.size() > 0) {
                plan = new Eval(source, plan, newEvals);
            }
            // preserve initial projection
            plan = new Project(source, plan, newProjections);
        }

        return plan;
    }

    /** Build alias map — subclasses can override to handle grouping functions differently */
    protected abstract AttributeMap<Expression> buildAliases(
        Aggregate aggregate,
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes
    );

    /** Process each alias — subclasses can override to add Eval logic or grouping replacements */
    protected abstract void processAlias(
        Alias as,
        AttributeMap<Expression> aliases,
        Map<AggregateFunction, Alias> rootAggs,
        List<NamedExpression> newAggs,
        List<NamedExpression> newProjections,
        List<Alias> newEvals,
        Holder<Boolean> changed,
        int[] counter,
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes
    );
}
