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
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
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
        boolean modified = false;
        for (var agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af && af.hasWindow()) {
                if (af.window().foldable() && af.window().fold(FoldContext.small()) instanceof Duration windowDuration) {
                    Expression bucket = aggregate.timeBucket().buckets();
                    if (bucket != null
                        && bucket.foldable()
                        && bucket.fold(FoldContext.small()) instanceof Duration bucketDuration
                        && bucketDuration.compareTo(windowDuration) > 0) {
                        aggs.add(
                            new Alias(
                                alias.source(),
                                alias.name(),
                                replaceWindowWithFilter(af, aggregate.timeBucket(), aggregate.timestamp()),
                                agg.id()
                            )
                        );
                        modified = true;
                        continue;
                    }
                }
            }
            aggs.add(agg);
        }
        return modified ? aggregate.with(aggregate.child(), aggregate.groupings(), aggs) : aggregate;
    }

    private static AggregateFunction replaceWindowWithFilter(AggregateFunction af, Bucket bucket, Expression timestamp) {
        AggregateFunction newAggregateFunction;
        if (af.hasFilter()) {
            newAggregateFunction = af.withFilter(
                Predicates.combineAnd(List.of(af.filter(), new WindowFilter(af.source(), af.window(), bucket, timestamp)))
            );
        } else {
            newAggregateFunction = af.withFilter(new WindowFilter(af.source(), af.window(), bucket, timestamp));
        }
        return newAggregateFunction.withWindow(AggregateFunction.NO_WINDOW);
    }
}
