/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Represents a PromQL function call that performs element-wise transformations on an instant vector.
 * <p>
 * These functions transform each sample in the input instant vector independently, preserving non-name labels.
 * Like Prometheus, they drop {@code __name__}. The result is an instant vector with the same cardinality as the
 * input, containing the transformed values. This corresponds to PromQL syntax:
 * <pre>
 * function_name(instant_vector)
 * </pre>
 *
 * Examples:
 * <pre>
 * abs(http_requests_total)
 * ceil(cpu_usage)
 * sqrt(variance)
 * </pre>
 */
public final class ValueTransformationFunction extends PromqlFunctionCall {

    private List<Attribute> output;

    public ValueTransformationFunction(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, ValueTransformationFunction::new, child(), definition(), parameters());
    }

    @Override
    public ValueTransformationFunction replaceChild(LogicalPlan newChild) {
        return new ValueTransformationFunction(source(), newChild, definition(), parameters());
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            // Prometheus drops the metric name from value transforms (ceil, abs, …).
            output = withoutMetricName(source(), child().output());
        }
        return output;
    }

    /** Child tsIdentity with {@code __name__} left out of metadata (and dropped if promoted). */
    static List<Attribute> withoutMetricName(Source source, List<Attribute> tsIdentity) {
        var result = new ArrayList<Attribute>(tsIdentity.size());
        for (Attribute attr : tsIdentity) {
            if (PromqlLabels.isMetadataLabel(attr)) {
                UnresolvedPromqlAttribute timeSeriesMetadata = (UnresolvedPromqlAttribute) attr;
                var unset = new LinkedHashSet<>(timeSeriesMetadata.unset());
                unset.add(LabelMatcher.NAME);
                result.add(UnresolvedPromqlAttribute.metadata(source, unset));
            } else if (LabelMatcher.NAME.equals(PromqlLabels.labelName(attr)) == false) {
                result.add(attr);
            }
        }
        return List.copyOf(result);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.VALUE_TRANSFORMATION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Drops __name__ but is not a grouping boundary for relabel placement.
        return true;
    }

    @Override
    public IntermediateResult translate(TranslationContext context) {
        // Name drop is declared in {@link #output()}; union branches project that unset onto a shared
        // {@code _timeseries} alias so PathTrace omits {@code __name__} without an Eval statement that would
        // break rate/sum aggregate fusion.
        return translateValueFunction(context);
    }
}
