/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.WindowFilter;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Decomposes a time-series aggregate whose window is larger than, but not an exact multiple of, the time bucket
 * ({@code W = k * B + r} with {@code k >= 1} and {@code 0 < r < B}) into two ordinary aggregates:
 * <ul>
 *     <li>the original <em>full</em> aggregate, keeping its plain window {@code W}, whose per-bucket state covers
 *     every row of a bucket, and</li>
 *     <li>a synthetic <em>partial</em> sibling with no window whose input is filtered (via {@link WindowFilter})
 *     to the trailing {@code r} of each bucket.</li>
 * </ul>
 * Both execute as ordinary aggregates in the initial and intermediate phases; only the coordinator's final phase
 * pairs them, merging the {@code k} fully covered buckets from the full state with the boundary bucket's partial
 * state (see {@code WindowGroupingAggregatorFunction}). Since the shipped plan fragment contains nothing but plain
 * per-aggregate filters and windows, data nodes and remote clusters on older versions execute it unchanged.
 * <p>
 * The rule runs on the coordinator's physical plan, matching the exchange sandwich produced by the {@code Mapper}
 * for the first-pass aggregation - {@code TimeSeriesAggregateExec[FINAL]} over {@link ExchangeExec} over
 * {@link FragmentExec} - and rewrites all three consistently: the fragment's logical aggregate (what data nodes
 * execute), the exchange output, and the final exec's aggregates and intermediate attributes. Planting the sibling
 * after logical optimization keeps it out of reach of the generic logical rules ({@code PruneColumns} would remove
 * an aggregate nothing references, {@code DeduplicateAggs} and projection folding would break the pairing) and
 * means surrogate substitution has already happened, so the sibling is a clone of the final form of the function.
 * <p>
 * Siblings are shared: aggregates whose windows leave the same remainder over the same input reuse one sibling, and
 * an existing structurally identical aggregate (for example a window smaller than the bucket, which the analyzer
 * rewrites to exactly this filtered shape) is reused instead of planting a duplicate. The final-phase wiring in
 * {@code AbstractPhysicalOperationProviders} locates the sibling with the same structural signature via
 * {@link #findPartialSibling}.
 */
public class InsertPartialWindowAggregates extends PhysicalOptimizerRules.OptimizerRule<TimeSeriesAggregateExec> {

    @Override
    protected PhysicalPlan rule(TimeSeriesAggregateExec aggExec) {
        if (aggExec.getMode() != AggregatorMode.FINAL) {
            return aggExec;
        }
        if (aggExec.child() instanceof ExchangeExec exchange
            && exchange.child() instanceof FragmentExec fragment
            && fragment.fragment() instanceof TimeSeriesAggregate aggregate
            && aggregate.timeBucket() != null) {
            return insertPartialAggregates(aggExec, exchange, fragment, aggregate);
        }
        return aggExec;
    }

    private static PhysicalPlan insertPartialAggregates(
        TimeSeriesAggregateExec aggExec,
        ExchangeExec exchange,
        FragmentExec fragment,
        TimeSeriesAggregate aggregate
    ) {
        FoldContext foldContext = FoldContext.small();
        List<NamedExpression> newAggs = new ArrayList<>(aggregate.aggregates().size() + 1);
        List<NamedExpression> planted = new ArrayList<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            newAggs.add(agg);
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                Duration remainder = windowRemainder(af, aggregate.timeBucket(), foldContext);
                if (remainder == null
                    || findPartialSibling(aggregate.aggregates(), af, remainder, foldContext) != null
                    || findPartialSibling(planted, af, remainder, foldContext) != null) {
                    continue;
                }
                if (aggregate.timestamp() == null) {
                    throw new EsqlIllegalArgumentException(
                        "time-series aggregation with a windowed aggregate [{}] has no timestamp",
                        af.sourceText()
                    );
                }
                AggregateFunction sibling = partialSibling(af, aggregate, remainder);
                Alias siblingAlias = new Alias(alias.source(), Attribute.rawTemporaryName(alias.name(), "partial"), sibling, null, true);
                planted.add(siblingAlias);
                newAggs.add(siblingAlias);
            }
        }
        if (planted.isEmpty()) {
            return aggExec;
        }
        TimeSeriesAggregate newAggregate = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
        List<Attribute> intermediateAttributes = AbstractPhysicalOperationProviders.intermediateAttributes(
            newAggs,
            newAggregate.groupings()
        );
        ExchangeExec newExchange = new ExchangeExec(
            exchange.source(),
            intermediateAttributes,
            exchange.inBetweenAggs(),
            fragment.withFragment(newAggregate)
        );
        return new TimeSeriesAggregateExec(
            aggExec.source(),
            newExchange,
            aggExec.groupings(),
            newAggs,
            AggregatorMode.FINAL,
            intermediateAttributes,
            aggExec.estimatedRowSize(),
            aggExec.timeBucket()
        );
    }

    /**
     * The remainder {@code r = W mod B} of the aggregate's window over the time bucket, or {@code null} when no
     * partial sibling applies: no window, a window not exceeding the bucket (handled by the analyzer as a row
     * filter), an exact multiple (the final phase merges whole buckets only), or a window or bucket that does not
     * fold to a fixed duration (handled by the range-driven merge at the final phase).
     */
    @Nullable
    public static Duration windowRemainder(AggregateFunction af, @Nullable Bucket timeBucket, FoldContext foldContext) {
        if (af.hasWindow() == false || timeBucket == null) {
            return null;
        }
        Duration window = foldToPositiveDuration(af.window(), foldContext);
        Duration bucket = foldToPositiveDuration(timeBucket.buckets(), foldContext);
        if (window == null || bucket == null || window.compareTo(bucket) <= 0) {
            return null;
        }
        long remainderMillis = window.toMillis() % bucket.toMillis();
        return remainderMillis == 0 ? null : Duration.ofMillis(remainderMillis);
    }

    /**
     * Finds the partial sibling of the given windowed aggregate in the aggregates list, or {@code null} if there is
     * none. The match is structural, so it finds the planted sibling as well as a reused pre-existing aggregate.
     */
    @Nullable
    public static AggregateFunction findPartialSibling(
        List<? extends NamedExpression> aggregates,
        AggregateFunction af,
        Duration remainder,
        FoldContext foldContext
    ) {
        for (NamedExpression ne : aggregates) {
            if (ne instanceof Alias alias
                && alias.child() instanceof AggregateFunction candidate
                && isPartialSibling(candidate, af, remainder, foldContext)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Whether {@code candidate} is the partial sibling of the windowed aggregate {@code af}: the same function over
     * the same input, with no window, and with the aggregate's filter extended by exactly one {@link WindowFilter}
     * over the remainder.
     */
    private static boolean isPartialSibling(
        AggregateFunction candidate,
        AggregateFunction af,
        Duration remainder,
        FoldContext foldContext
    ) {
        if (candidate.getClass() != af.getClass()
            || candidate.hasWindow()
            || candidate.field().equals(af.field()) == false
            || candidate.parameters().equals(af.parameters()) == false) {
            return false;
        }
        List<Expression> conjuncts = new ArrayList<>(Predicates.splitAnd(candidate.filter()));
        List<Expression> expected = af.hasFilter() ? Predicates.splitAnd(af.filter()) : List.of();
        if (conjuncts.size() != expected.size() + 1) {
            return false;
        }
        boolean removed = false;
        // an aggregate carries at most one WindowFilter today; if more than one matching conjunct ever appears,
        // removing just the first makes the remaining-conjuncts comparison below fail rather than mismatch silently
        for (Iterator<Expression> it = conjuncts.iterator(); it.hasNext();) {
            if (it.next() instanceof WindowFilter windowFilter
                && windowFilter.window().foldable()
                && remainder.equals(windowFilter.window().fold(foldContext))) {
                it.remove();
                removed = true;
                break;
            }
        }
        return removed && conjuncts.equals(expected);
    }

    private static AggregateFunction partialSibling(AggregateFunction af, TimeSeriesAggregate aggregate, Duration remainder) {
        Literal remainderLiteral = Literal.timeDuration(af.window().source(), remainder);
        WindowFilter windowFilter = new WindowFilter(af.source(), remainderLiteral, aggregate.timeBucket(), aggregate.timestamp());
        Expression filter = af.hasFilter() ? Predicates.combineAnd(List.of(af.filter(), windowFilter)) : windowFilter;
        return af.withFilter(filter).withWindow(AggregateFunction.NO_WINDOW);
    }

    private static Duration foldToPositiveDuration(Expression expression, FoldContext foldContext) {
        if (expression != null
            && expression.foldable()
            && expression.fold(foldContext) instanceof Duration duration
            && duration.isPositive()) {
            return duration;
        }
        return null;
    }
}
