/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for PromQL histogram functions that evaluate classic histogram buckets grouped by their {@code le} label.
 */
public abstract sealed class HistogramFunctionCall extends PromqlFunctionCall permits HistogramFraction, HistogramQuantile {
    public static final String LE_LABEL = "le";
    /**
     * Labels a classic histogram function leaves out of its input's metadata: {@code le}, which it collapses, and the metric
     * name, which it drops like Prometheus does. Within one metric bucket rows differ by {@code le} alone.
     */
    private static final Set<String> COLLAPSED_LABELS = Set.of(LE_LABEL, LabelMatcher.NAME);

    private List<Attribute> output;

    protected HistogramFunctionCall(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    /**
     * Builds the aggregate expression for this function call:
     * The aggregate expression will be invoked with the bucket counts and their upper bounds (the le labels).
     */
    public abstract Expression buildAggregateFunction(Expression count, Expression upperBound);

    @Override
    public final List<Attribute> output() {
        if (output == null) {
            var keys = new ArrayList<Attribute>();
            for (Attribute attr : child().output()) {
                if (attr instanceof UnresolvedPromqlLabelRef metadata && metadata.isMetadata()) {
                    var excluded = new LinkedHashSet<>(metadata.unset());
                    excluded.addAll(COLLAPSED_LABELS);
                    keys.add(UnresolvedPromqlLabelRef.metadata(source(), excluded));
                } else if (MetadataAttribute.isTimeSeriesAttributeName(attr.name())
                    || LE_LABEL.equals(PromqlLabels.labelName(attr)) == false) {
                        keys.add(attr);
                    }
            }
            output = List.copyOf(keys);
        }
        return output;
    }

    /**
     * The labels the bucket rows of a raw child key by: every label of the child but {@code le} as one view, and
     * {@code le} itself, so each row is one bucket series.
     */
    private List<Attribute> bucketKeys(TranslationContext context) {
        var keys = new ArrayList<Attribute>();
        if (PromqlLabels.metadataLabel(child().output()) != null) {
            keys.add(UnresolvedPromqlLabelRef.metadata(source(), COLLAPSED_LABELS));
        }
        keys.add(UnresolvedPromqlLabelRef.label(source(), LE_LABEL));
        return keys;
    }

    @Override
    public final FunctionType functionType() {
        return FunctionType.HISTOGRAM;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Reshapes labels (drops `le`) but is not a grouping boundary for relabel placement: a relabel below it still
        // feeds the enclosing aggregation.
        return true;
    }

    @Override
    public IntermediateResult translate(TranslationContext context) {
        // Classic histogram functions collapse the `le` bucket dimension like a `without (le)` would (see output()), and
        // read the bucket bound off the `le` column itself (see bucketKeys).
        IntermediateResult result = context.translate(child());
        if (result.kind().constant) {
            return result;
        }

        // Native histograms - distinguishable only by the value type - are regular value transformations. They carry no
        // `le`, so the inferred identity holds for them too.
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            return applyValueFunction(context, result);
        }

        // Classic counter-backed histograms evaluate their buckets by `le`. An input without it has a null `le` and evaluates
        // to nothing; the verifier warns, like Prometheus.
        if (result.kind().afterInitialAggregation == false) {
            result = context.emitInitialAggregate(result, bucketKeys(context), result.value());
        }

        // Bucket counts are consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
        Expression count = new ToDouble(source(), result.value());
        Attribute le = UnresolvedPromqlLabelRef.label(source(), LE_LABEL);
        return context.emitAggregate(
            result,
            UnresolvedPromqlLabelRef.tsIdentity(source(), output()),
            true,
            buildAggregateFunction(count, le)
        );
    }
}
