/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * Base class for PromQL histogram functions that evaluate classic histogram buckets grouped by their {@code le} label.
 */
public abstract sealed class HistogramFunctionCall extends PromqlFunctionCall permits HistogramFraction, HistogramQuantile {
    public static final String LE_LABEL = "le";

    private volatile List<Attribute> output;

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
            output = child().output()
                .stream()
                .filter(attr -> MetadataAttribute.isTimeSeriesAttributeName(attr.name()) || LE_LABEL.equals(labelName(attr)) == false)
                .toList();
        }
        return output;
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

    private static String labelName(Attribute attribute) {
        String fieldName;
        if (attribute instanceof FieldAttribute fieldAttribute) {
            fieldName = fieldAttribute.fieldName().string();
        } else {
            fieldName = attribute.name();
        }
        if (fieldName.startsWith(PROMETHEUS_LABELS_PREFIX)) {
            return fieldName.substring(PROMETHEUS_LABELS_PREFIX.length());
        }
        return fieldName;
    }

    @Override
    public IntermediateResult translate(TranslationContext context) {
        IntermediateResult result = context.translate(child());
        if (result.kind().constant) {
            return result;
        }

        // native histograms - distinguishable only at this point in planning are regular value transformations.
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            return new ValueTransformationFunction(source(), child(), definition(), parameters()).translate(context);
        }

        // Classic counter-backed histograms evaluate their buckets by `le`: the series collapse as they are, then group by
        // their labels but `le`, reading each bucket's bound off it. Prometheus drops the metric name from the result.
        // An input without `le` has a null bound and evaluates to nothing, with a warning like Prometheus.
        if (hasLe(context) == false) {
            HeaderWarning.addWarning(functionName() + ": input vector has no le label; no buckets to evaluate");
        }
        if (result.kind().afterInitialAggregation == false) {
            result = context.emitInitialAggregate(result, result.value());
        }
        // Bucket counts are consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
        Expression count = new ToDouble(source(), result.value());
        Expression labels = result.labels();
        Expression le = labels == null ? new Literal(source(), null, DataType.KEYWORD) : PromqlLabels.read(labels, LE_LABEL);
        IntermediateResult quantiles = context.emitAggregate(
            result,
            PromqlLabels.without(labels, List.of(LE_LABEL)),
            buildAggregateFunction(count, le)
        );
        return quantiles.withLabels(context.withoutMetricName(quantiles.labels()));
    }

    /** Whether the series of the input may carry {@code le}: one of the labels the input keeps, which the source maps a field for. */
    private boolean hasLe(TranslationContext context) {
        return carries(child(), LE_LABEL, context);
    }

    /** Whether the series of {@code node} may carry the label {@code label}. */
    private static boolean carries(LogicalPlan node, String label, TranslationContext context) {
        return switch (node) {
            case CountValuesAggregate countValues -> labelName(countValues.valueLabel()).equals(label)
                || countValues.grouping() != AcrossSeriesAggregate.Grouping.NONE
                    && carries((AcrossSeriesAggregate) countValues, label, context);
            case AcrossSeriesAggregate aggregate -> carries(aggregate, label, context);
            case MetadataManipulationFunction relabel -> labelName(relabel.destination()).equals(label)
                || carries(relabel.child(), label, context);
            case LiteralSelector ignored -> false;
            case Selector ignored -> context.cmd().child().output().stream().anyMatch(field -> labelName(field).equals(label));
            default -> node.children().stream().anyMatch(child -> carries(child, label, context));
        };
    }

    private static boolean carries(AcrossSeriesAggregate aggregate, String label, TranslationContext context) {
        boolean listed = aggregate.groupings().stream().anyMatch(grouping -> labelName(grouping).equals(label));
        return switch (aggregate.grouping()) {
            case BY -> listed;
            case WITHOUT -> listed == false && carries(aggregate.child(), label, context);
            case NONE -> false;
        };
    }
}
