/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.WindowFilter;
import org.elasticsearch.xpack.esql.expression.function.WindowWithPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts the declarative window on each aggregate of a {@link TimeSeriesAggregate} into its executable form,
 * dispatching per aggregate on the relationship between the window {@code W} and the time bucket {@code B}:
 * <ul>
 *     <li>{@code W < B}: rewritten to a row-level {@link WindowFilter} keeping only the trailing {@code W} of each
 *     bucket; the window is dropped, except for {@code rate()}/{@code increase()}, which keep it so the final phase
 *     extrapolates over the window's range rather than the bucket's.</li>
 *     <li>{@code W == k * B} (including {@code W == B}): the plain window duration is kept - the final aggregation
 *     phase merges the {@code k} buckets covered by the window, short-circuiting to the bare per-bucket value when
 *     the window covers exactly the bucket.</li>
 *     <li>{@code W == k * B + r} with {@code r > 0}: rewritten to {@link WindowWithPartial}, decomposing the window
 *     into a full per-bucket channel plus a partial channel restricted to the trailing {@code r} of each bucket via
 *     {@link WindowFilter}; the final phase merges {@code k} full buckets and the boundary bucket's partial state.</li>
 * </ul>
 * The dispatch is strictly per aggregate: windows on different aggregates in the same STATS never interact, so any
 * combination of the cases above is supported. Windows or buckets that do not fold to a fixed duration (for example
 * calendar intervals or a bucket derived from a target count) are left untouched and handled by the range-driven
 * merge at the final phase, matching the behavior before this rule existed.
 */
public class ApplyWindowSemantics extends AnalyzerRules.ParameterizedAnalyzerRule<TimeSeriesAggregate, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, AnalyzerContext context) {
        Bucket timeBucket = aggregate.timeBucket();
        if (timeBucket == null) {
            return aggregate;
        }
        Duration bucketDuration = foldToPositiveDuration(timeBucket.buckets());
        if (bucketDuration == null) {
            return aggregate;
        }
        List<NamedExpression> aggs = new ArrayList<>(aggregate.aggregates().size());
        boolean modified = false;
        for (var agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af && af.hasWindow()) {
                Duration windowDuration = foldToPositiveDuration(af.window());
                if (windowDuration != null) {
                    AggregateFunction dispatched = dispatch(af, windowDuration, bucketDuration, aggregate, context);
                    if (dispatched != af) {
                        aggs.add(new Alias(alias.source(), alias.name(), dispatched, agg.id()));
                        modified = true;
                        continue;
                    }
                }
            }
            aggs.add(agg);
        }
        return modified ? aggregate.with(aggregate.child(), aggregate.groupings(), aggs) : aggregate;
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

    private static AggregateFunction dispatch(
        AggregateFunction af,
        Duration window,
        Duration bucket,
        TimeSeriesAggregate aggregate,
        AnalyzerContext context
    ) {
        long remainderMillis = window.toMillis() % bucket.toMillis();
        if (window.compareTo(bucket) < 0) {
            // aggregate only the trailing W of each bucket via a row filter
            WindowFilter filter = new WindowFilter(af.source(), af.window(), aggregate.timeBucket(), aggregate.timestamp());
            AggregateFunction filtered = withAndFilter(af, filter);
            // Do not clear the function's window.
            // rate()/increase() rely on group start/end timestamps (by default, bucket) for extrapolation;
            // for windows different from bucket, clearing it leads to incorrect results.
            if (filtered instanceof Rate || filtered instanceof Increase) {
                return filtered;
            }
            return filtered.withWindow(AggregateFunction.NO_WINDOW);
        }
        if (remainderMillis == 0) {
            // exact multiple; the final phase merges the covered buckets
            return af;
        }
        if (context.minimumVersion().supports(WindowWithPartial.ESQL_PER_AGGREGATE_WINDOW) == false) {
            throw new IllegalArgumentException(
                "cannot use window ["
                    + af.window().sourceText()
                    + "] that is not a multiple of the time bucket ["
                    + aggregate.timeBucket().buckets().sourceText()
                    + "] until all nodes in the cluster have been upgraded"
            );
        }
        Literal remainder = Literal.timeDuration(af.window().source(), Duration.ofMillis(remainderMillis));
        WindowFilter partialFilter = new WindowFilter(af.source(), remainder, aggregate.timeBucket(), aggregate.timestamp());
        return af.withWindow(new WindowWithPartial(af.window().source(), af.window(), partialFilter));
    }

    private static AggregateFunction withAndFilter(AggregateFunction af, WindowFilter filter) {
        return af.hasFilter() ? af.withFilter(Predicates.combineAnd(List.of(af.filter(), filter))) : af.withFilter(filter);
    }
}
