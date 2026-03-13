/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.WindowFilter;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ApplyWindowFilter extends OptimizerRules.ParameterizedOptimizerRule<TimeSeriesAggregate, LogicalOptimizerContext> {

    public ApplyWindowFilter() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
        List<NamedExpression> aggs = new ArrayList<>();
        for (var agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af && af.hasWindow()) {
                if (af.window().foldable() && af.window().fold(FoldContext.small()) instanceof Duration windowDuration) {
                    Expression bucket = aggregate.timeBucket().buckets();
                    if (bucket != null
                        && bucket.foldable()
                        && bucket.fold(FoldContext.small()) instanceof Duration bucketDuration
                        && bucketDuration.compareTo(windowDuration) > 0) {
                        AggregateFunction newAggregateFunction;
                        if (af.hasFilter()) {
                            newAggregateFunction = af.withFilter(
                                Predicates.combineAnd(
                                    List.of(
                                        af.filter(),
                                        new WindowFilter(af.source(), af.window(), aggregate.timeBucket(), aggregate.timestamp())
                                    )
                                )
                            );
                        } else {
                            newAggregateFunction = af.withFilter(
                                new WindowFilter(af.source(), af.window(), aggregate.timeBucket(), aggregate.timestamp())
                            );
                        }
                        aggs.add(
                            new Alias(alias.source(), alias.name(), newAggregateFunction.withWindow(AggregateFunction.NO_WINDOW), agg.id())
                        );
                        continue;
                    }
                }
            }
            aggs.add(agg);
        }
        return aggregate.with(aggregate.child(), aggregate.groupings(), aggs);
    }
}
