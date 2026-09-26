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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;

/**
 * Base class for PromQL histogram functions that evaluate classic histogram buckets grouped by their {@code le} label.
 */
public abstract sealed class HistogramFunctionCall extends PromqlFunctionCall permits HistogramFraction, HistogramQuantile {
    public static final String LE_LABEL = "le";

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
            output = child().output()
                .stream()
                .filter(attr -> MetadataAttribute.isTimeSeriesAttributeName(attr.name()) || LE_LABEL.equals(labelName(attr)) == false)
                .toList();
        }
        return output;
    }

    /**
     * Classic histogram functions collapse the {@code le} bucket dimension like a {@code without (le)} would, dropping the
     * metric name with it, and read the bucket bound off the {@code le} column itself, so the child must also expose it by
     * name.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        TranslationConstraint required = translation.required();
        List<String> le = List.of(LE_LABEL);
        List<String> dropped = List.of(LE_LABEL, LabelMatcher.NAME);
        // IN: any + required - le - `__name__`, plus le itself as a column to read the bound from
        TranslationConstraint below = union(sub(union(any(), required), of(dropped)), of(le));
        TranslationResult result = translation.translate(child(), below);
        if (result.kind().constant) {
            return result;
        }

        // native histograms - distinguishable only at this point in planning - are regular value transformations
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            return new ValueTransformationFunction(source(), child(), definition(), parameters()).translate(translation);
        }

        // Classic counter-backed histograms need the special treatment below.
        Attribute upperBound = result.label(LE_LABEL);
        if (upperBound == null) {
            // like prometheus, return warning and drop series w/o `le`
            HeaderWarning.addWarning(functionName() + ": input vector has no le label; no buckets to evaluate");
            var skipAllFilter = new Filter(source(), result.plan(), Literal.FALSE);
            var nullGrouping = new Values(source(), new Literal(source(), null, DataType.DOUBLE));
            return translation.aggregate(result.with(skipAllFilter, result.value()), nullGrouping);
        }
        if (result.kind().afterInitialAggregation == false) {
            result = translation.aggregate(result, result.value());
            upperBound = result.label(LE_LABEL);
            assert upperBound != null : "[INVARIANT]: [" + LE_LABEL + "] must be delivered by the child";
        }

        // OUT: child's labels - le - `__name__`; bucket counts read as doubles
        TranslationConstraint keys = sub(result.shape(), of(dropped));
        Expression count = new ToDouble(source(), result.value());
        return translation.aggregate(result, keys, buildAggregateFunction(count, upperBound), true);
    }

    @Override
    public final FunctionType functionType() {
        return FunctionType.HISTOGRAM;
    }

    @Override
    public boolean dropsMetricName() {
        // Classic histograms collapse buckets like `without (le)`: the metric name goes with `le`.
        return true;
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
}
