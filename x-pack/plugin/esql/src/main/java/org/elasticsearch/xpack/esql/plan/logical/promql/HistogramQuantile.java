/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated logical node for PromQL {@code histogram_quantile()} over classic histograms.
 * The function consumes the cumulative bucket counts identified by the {@code le} label and
 * produces the same label set as its child except for {@code le}.
 */
public final class HistogramQuantile extends PromqlFunctionCall {
    public static final String LE_LABEL = "le";
    private static final String PROMETHEUS_LABELS_PREFIX = "labels.";

    private final Expression quantile;
    private List<Attribute> output;

    public HistogramQuantile(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
        this.quantile = parameters.getFirst();
    }

    public Expression quantile() {
        return quantile;
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, HistogramQuantile::new, child(), definition(), parameters());
    }

    @Override
    public HistogramQuantile replaceChild(LogicalPlan newChild) {
        return new HistogramQuantile(source(), newChild, definition(), parameters());
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = child().output()
                .stream()
                .filter(attr -> MetadataAttribute.isTimeSeriesAttributeName(attr.name()) || LE_LABEL.equals(labelName(attr)) == false)
                .toList();
            if (output.isEmpty() && child().output().stream().anyMatch(attr -> MetadataAttribute.isTimeSeriesAttributeName(attr.name()))) {
                List<Attribute> relationLabels = relationLabelsWithoutLe();
                output = relationLabels.isEmpty() ? List.of(FieldAttribute.timeSeriesAttribute(source())) : relationLabels;
            }
        }
        return output;
    }

    private List<Attribute> relationLabelsWithoutLe() {
        List<Attribute> labels = new ArrayList<>();
        boolean[] hasLe = new boolean[1];
        child().forEachDown(EsRelation.class, relation -> {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute fieldAttribute && fieldAttribute.isDimension()) {
                    String labelName = labelName(attribute);
                    if (LE_LABEL.equals(labelName)) {
                        hasLe[0] = true;
                    } else {
                        labels.add(attribute.withName(labelName));
                    }
                }
            }
        });
        return hasLe[0] ? labels : List.of();
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.HISTOGRAM;
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
