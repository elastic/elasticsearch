/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.WindowFilter;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites an aggregate whose window is smaller than the time bucket into a row-level {@link WindowFilter} keeping
 * only the trailing {@code W} of each bucket. The window itself is dropped, except for {@code rate()} and
 * {@code increase()}, which keep it so the final phase extrapolates over the window's range rather than the
 * bucket's.
 * <p>
 * Windows of at least one bucket are left untouched here: the final aggregation phase merges the buckets the window
 * covers, and a window that is not an exact multiple of the bucket is decomposed into a full and a partial aggregate
 * by {@code InsertPartialWindowAggregates} during physical planning. Windows or buckets that do not fold to a fixed
 * duration (for example calendar intervals) are also left untouched and handled by the range-driven merge at the
 * final phase.
 */
public class ApplyWindowFilter extends AnalyzerRules.AnalyzerRule<TimeSeriesAggregate> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate) {
        Duration bucketDuration = aggregate.timeBucket() == null ? null : foldToPositiveDuration(aggregate.timeBucket().buckets());
        if (bucketDuration == null) {
            return aggregate;
        }
        List<NamedExpression> aggs = new ArrayList<>(aggregate.aggregates().size());
        boolean modified = false;
        for (var agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af && af.hasWindow()) {
                Duration windowDuration = foldToPositiveDuration(af.window());
                if (windowDuration != null && windowDuration.compareTo(bucketDuration) < 0) {
                    aggs.add(new Alias(alias.source(), alias.name(), replaceWindowWithFilter(af, aggregate), agg.id()));
                    modified = true;
                    continue;
                }
            }
            aggs.add(agg);
        }
        return modified ? aggregate.with(aggregate.child(), aggregate.groupings(), aggs) : aggregate;
    }

    private static AggregateFunction replaceWindowWithFilter(AggregateFunction af, TimeSeriesAggregate aggregate) {
        WindowFilter filter = new WindowFilter(af.source(), af.window(), aggregate.timeBucket(), aggregate.timestamp());
        AggregateFunction filtered = af.hasFilter()
            ? af.withFilter(Predicates.combineAnd(List.of(af.filter(), filter)))
            : af.withFilter(filter);
        // Do not clear the function's window.
        // rate()/increase() rely on group start/end timestamps (by default, bucket) for extrapolation;
        // for windows different from bucket, clearing it leads to incorrect results.
        if (filtered instanceof Rate || filtered instanceof Increase) {
            return filtered;
        }
        return filtered.withWindow(AggregateFunction.NO_WINDOW);
    }

    private static Duration foldToPositiveDuration(Expression expression) {
        if (expression != null
            && expression.foldable()
            && expression.fold(FoldContext.small()) instanceof Duration duration
            && duration.isPositive()) {
            return duration;
        }
        return null;
    }
}
