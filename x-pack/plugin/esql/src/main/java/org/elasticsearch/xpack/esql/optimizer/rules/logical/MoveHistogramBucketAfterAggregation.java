/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMerge;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Moves histogram bucketing after a histogram merge when the merge is the aggregate's only aggregation:
 * <pre>
 *     STATS merged = HISTOGRAM_MERGE(h) BY bucket = BUCKET(h, size), other_groups
 *     | EVAL count = HISTOGRAM_FRACTION(merged, bucket)
 *     →
 *     EVAL empty_or_null = HISTOGRAM_COUNT(h) == 0 OR HISTOGRAM_COUNT(h) IS NULL
 *     | STATS merged = HISTOGRAM_MERGE(h) BY other_groups, empty_or_null
 *     | EVAL bucket = BUCKET(merged, size)
 *     | MV_EXPAND bucket
 *     | DROP empty_or_null
 *     | EVAL count = HISTOGRAM_FRACTION(merged, bucket)
 * </pre>
 *
 * This avoids merging every input histogram once per populated bucket. The rewrite is only safe when the merged
 * histogram is not exposed or consumed by anything other than {@link HistogramFraction}.
 */
public final class MoveHistogramBucketAfterAggregation extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        Set<Aggregate> candidates = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachDown(Aggregate.class, aggregate -> {
            Match match = match(aggregate);
            if (match != null && usedOnlyByHistogramFraction(plan, match.histogramMergeAlias().toAttribute())) {
                candidates.add(aggregate);
            }
        });
        if (candidates.isEmpty()) {
            return plan;
        }
        return plan.transformUp(Aggregate.class, aggregate -> {
            if (candidates.contains(aggregate) == false) {
                return aggregate;
            }
            Match match = match(aggregate);
            assert match != null;
            return rewrite(aggregate, match);
        });
    }

    private static Match match(Aggregate aggregate) {
        Alias histogramMergeAlias = null;
        int aggregateFunctionCount = 0;
        for (NamedExpression output : aggregate.aggregates()) {
            List<AggregateFunction> functions = new ArrayList<>();
            output.forEachDown(AggregateFunction.class, functions::add);
            aggregateFunctionCount += functions.size();
            if (output instanceof Alias alias && alias.child() instanceof HistogramMerge histogramMerge) {
                histogramMergeAlias = alias;
                // filters are not allowed as they are evaluated after BUCKET() is computed in the unoptimized plan
                if (histogramMerge.hasFilter()) {
                    return null;
                }
            }
        }
        if (aggregateFunctionCount != 1 || histogramMergeAlias == null) {
            return null;
        }

        HistogramMerge histogramMerge = (HistogramMerge) histogramMergeAlias.child();
        if (aggregate.child() instanceof Eval == false) {
            return null;
        }
        Eval eval = (Eval) aggregate.child();
        Match match = null;
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof Attribute == false) {
                continue;
            }
            Attribute groupingAttribute = (Attribute) grouping;
            Alias bucketAlias = eval.fields()
                .stream()
                .filter(alias -> alias.toAttribute().semanticEquals(groupingAttribute) && alias.child() instanceof Bucket)
                .findFirst()
                .orElse(null);
            if (bucketAlias == null) {
                continue;
            }
            Bucket bucket = (Bucket) bucketAlias.child();
            if (bucket.field().semanticEquals(histogramMerge.field()) == false) {
                continue;
            }
            if (match != null) {
                return null;
            }
            match = new Match(histogramMergeAlias, groupingAttribute, bucketAlias);
        }
        return match;
    }

    private static boolean usedOnlyByHistogramFraction(LogicalPlan plan, Attribute histogram) {
        int[] uses = new int[1];
        plan.forEachExpressionDown(Attribute.class, attribute -> {
            if (attribute.semanticEquals(histogram)) {
                uses[0]++;
            }
        });

        int[] histogramFractionUses = new int[1];
        plan.forEachExpressionDown(HistogramFraction.class, fraction -> {
            Expression input = fraction.children().getFirst();
            if (input instanceof Attribute attribute && attribute.semanticEquals(histogram)) {
                histogramFractionUses[0]++;
            }
        });
        return histogramFractionUses[0] > 0 && uses[0] == histogramFractionUses[0];
    }

    private static LogicalPlan rewrite(Aggregate aggregate, Match match) {
        HistogramMerge histogramMerge = (HistogramMerge) match.histogramMergeAlias().child();
        // Keep empty and null histograms in a separate group so they still produce the null bucket that existed before the rewrite.
        // The synthetic grouping also prevents an empty input from becoming an ungrouped aggregation that emits a single null row.
        Expression histogramCount = ExtractHistogramComponent.create(
            aggregate.source(),
            histogramMerge.field(),
            HistogramBlock.Component.COUNT
        );
        Alias emptyHistogram = new Alias(
            aggregate.source(),
            Attribute.rawTemporaryName("is_histogram_empty"),
            new Or(
                aggregate.source(),
                new Equals(aggregate.source(), histogramCount, new Literal(aggregate.source(), 0.0, DataType.DOUBLE)),
                new IsNull(aggregate.source(), histogramMerge.field())
            ),
            null,
            true
        );
        Attribute emptyHistogramAttribute = emptyHistogram.toAttribute();

        List<Expression> newGroupings = new ArrayList<>(
            aggregate.groupings().stream().filter(grouping -> grouping.semanticEquals(match.bucketGrouping()) == false).toList()
        );
        newGroupings.add(emptyHistogramAttribute);
        List<NamedExpression> newAggregates = new ArrayList<>(
            aggregate.aggregates()
                .stream()
                .filter(output -> output.toAttribute().semanticEquals(match.bucketGrouping()) == false)
                .map(NamedExpression.class::cast)
                .toList()
        );
        newAggregates.add(emptyHistogramAttribute);

        Eval eval = (Eval) aggregate.child();
        List<Alias> remainingFields = new ArrayList<>(eval.fields().stream().filter(alias -> alias != match.bucketAlias()).toList());
        remainingFields.add(emptyHistogram);
        LogicalPlan aggregateChild = new Eval(eval.source(), eval.child(), remainingFields);
        LogicalPlan plan = aggregate.with(aggregateChild, newGroupings, newAggregates);

        Bucket bucket = (Bucket) match.bucketAlias().child();
        List<Expression> bucketChildren = new ArrayList<>(bucket.children());
        bucketChildren.set(0, match.histogramMergeAlias().toAttribute());
        Alias postAggregateBucket = match.bucketAlias().replaceChild(bucket.replaceChildren(bucketChildren));
        plan = new Eval(aggregate.source(), plan, List.of(postAggregateBucket));

        Attribute bucketAttribute = postAggregateBucket.toAttribute();
        plan = new MvExpand(aggregate.source(), plan, bucketAttribute, bucketAttribute);
        return new Project(aggregate.source(), plan, aggregate.output());
    }

    private record Match(Alias histogramMergeAlias, Attribute bucketGrouping, Alias bucketAlias) {}
}
