/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.HistogramFunctionCall.LE_LABEL;

/**
 * Translates the classic histogram functions ({@code histogram_quantile}, {@code histogram_fraction}) over a label
 * container: the buckets of each histogram group by their labels but {@code le}, reading each bucket's bound off it.
 */
final class HistogramFunctionCallV2 {

    /**
     * Labels a classic histogram function leaves out of its input's label container: {@code le}, which it collapses, and the
     * metric name, which it drops like Prometheus does. Within one metric bucket rows differ by {@code le} alone.
     */
    private static final Set<String> COLLAPSED_LABELS = Set.of(LE_LABEL, LabelMatcher.NAME);

    private HistogramFunctionCallV2() {}

    static IntermediateResult translate(HistogramFunctionCall histogram, TranslationContextV2 context) {
        IntermediateResult result = context.translate(histogram.child());
        if (result.kind().constant) {
            return result;
        }

        // Native histograms - distinguishable only by the value type - are regular value transformations. They carry no
        // `le`, so the inferred identity holds for them too.
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            return PromqlFunctionCallV2.apply(histogram, context, result);
        }

        // Classic counter-backed histograms evaluate their buckets by `le`: the series collapse as they are, then group by
        // their labels but `le`, reading each bucket's bound off it. Prometheus drops the metric name from the result.
        // An input without `le` has a null bound and evaluates to nothing, with a warning like Prometheus.
        LogicalPlan buckets = histogram.child();
        if (selectsMappedMetric(buckets) && inputLabel(context.cmd(), buckets, LE_LABEL) == null) {
            HeaderWarning.addWarning(histogram.functionName() + ": input vector has no le label; no buckets to evaluate");
        }
        if (result.kind().afterInitialAggregation == false) {
            result = context.emitInitialAggregate(result, result.value());
        }
        // Bucket counts are consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
        Expression count = new ToDouble(histogram.source(), result.value());
        Expression labels = result.labels();
        Expression le = labels == null
            ? new Literal(histogram.source(), null, DataType.KEYWORD)
            : new TimeSeriesMetadataPath(labels.source(), labels, Literal.keyword(labels.source(), LE_LABEL));
        IntermediateResult quantiles = context.emitAggregate(
            result,
            labels == null
                ? null
                : new TimeSeriesMetadataFilter(labels.source(), labels, List.of(Literal.keyword(labels.source(), LE_LABEL)), true),
            histogram.buildAggregateFunction(count, le)
        );
        Expression keys = quantiles.labels();
        return keys == null
            ? quantiles
            : quantiles.withLabels(
                new TimeSeriesMetadataFilter(keys.source(), keys, PromqlLabels.labelLiterals(keys.source(), COLLAPSED_LABELS), true)
            );
    }

    private static boolean selectsMappedMetric(LogicalPlan input) {
        return input.anyMatch(
            node -> node instanceof Selector selector && selector.series().resolved() && selector.series().dataType() != DataType.NULL
        );
    }

    /** The label {@code name} of the series of {@code input}: declared by it, or read from the source it leaves it in. */
    private static @Nullable Attribute inputLabel(PromqlCommand cmd, LogicalPlan input, String name) {
        Attribute declared = PromqlLabels.getLabel(
            input.output().stream().filter(attribute -> PromqlLabels.isTagsLabel(attribute) == false).toList(),
            name
        );
        if (declared != null) {
            return declared;
        }
        Set<String> excluded = PromqlLabels.excluded(input);
        if (excluded == null || excluded.contains(name) || PromqlLogicalPlanBuilder.tryFoldRelation(cmd, cmd.child()) != null) {
            return null;
        }
        // the source relation, which declares every field; the command input may project some away
        return cmd.child()
            .collect(EsRelation.class)
            .stream()
            .map(relation -> PromqlLabels.getLabel(relation, name))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }
}
