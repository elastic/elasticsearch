/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;

public final class UseSinglePassTimeSeriesAggregate extends OptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregate,
    LogicalOptimizerContext> {

    public UseSinglePassTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
        if (aggregate.singlePass() == false
            && context.configuration().pragmas().timeSeriesSinglePass()
            && context.minimumVersion().supports(TimeSeriesAggregate.TIME_SERIES_SINGLE_PASS)
            && canExecuteSinglePass(aggregate)) {
            return aggregate.withSinglePass();
        }
        return aggregate;
    }

    private static boolean canExecuteSinglePass(TimeSeriesAggregate aggregate) {
        List<EsRelation> relations = aggregate.collect(EsRelation.class);
        if (relations.size() != 1) {
            return false;
        }
        var concreteIndices = relations.getFirst().concreteIndices().values();
        if (Iterables.size(concreteIndices) != 1 || Iterables.get(concreteIndices, 0).size() != 1) {
            return false;
        }
        boolean requiredSorted = false;
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (af.hasWindow()) {
                    return false;
                }
                requiredSorted |= (af instanceof TimeSeriesAggregateFunction ts && ts.requiredTimeSeriesSource());
            }
        }
        return requiredSorted;
    }
}
