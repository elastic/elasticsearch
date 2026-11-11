/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Resolves the labels a translated PromQL plan names against the fields of its source, right after {@link PushDownPromqlLabels}.
 * <p>
 * The source's label container holds its dimensions by field path ({@code attributes.cpu}), while the mapping resolves a PromQL
 * label name: a passthrough alias ({@code cpu}) names the field of the highest priority, which only a shard knows, and a field that
 * is not a dimension is a label too. A source container a label operation names such a label of reads it from its field instead:
 * the source leaves the label out, the shard resolving its name, and the container writes the field's value under the label name.
 */
public final class ResolvePromqlSourceLabels extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (PromqlCommand.usesLabelContainer(context) == false) {
            // The keyword containers left after label pushdown name no labels
            return plan;
        }
        Set<String> named = new TreeSet<>();
        plan.forEachExpressionDown(AbstractTimeSeriesMetadataFunction.class, function -> {
            for (Expression label : function.labels()) {
                named.add(BytesRefs.toString(((Literal) label).value()));
            }
        });
        if (named.isEmpty()) {
            return plan;
        }
        return plan.transformUp(node -> {
            List<EsRelation> relations = node.collect(EsRelation.class);
            if (relations.isEmpty()) {
                return node;
            }
            List<FieldAttribute> fields = fields(relations.getFirst(), named);
            if (fields.isEmpty()) {
                return node;
            }
            return node.transformExpressionsOnlyUp(
                ToTimeSeriesMetadata.class,
                container -> container.field() instanceof TimeSeriesWithout without ? resolve(container, without, fields) : container
            );
        });
    }

    /** The source container reading {@code fields} from the fields, under their label names. */
    private static Expression resolve(ToTimeSeriesMetadata container, TimeSeriesWithout without, List<FieldAttribute> fields) {
        var excluded = new ArrayList<Expression>(without.children());
        fields.forEach(field -> {
            if (excluded.contains(field) == false) {
                excluded.add(field);
            }
        });
        Expression resolved = new ToTimeSeriesMetadata(container.source(), new TimeSeriesWithout(without.source(), excluded));
        for (FieldAttribute field : fields) {
            resolved = new TimeSeriesMetadataSet(
                container.source(),
                resolved,
                Literal.keyword(container.source(), PromqlLabels.labelName(field)),
                field
            );
        }
        return resolved;
    }

    /**
     * The string fields of {@code relation} holding a label of {@code named} the container does not hold by its name: one that is not
     * a dimension, or a passthrough alias of a dimension under another path.
     */
    private static List<FieldAttribute> fields(EsRelation relation, Set<String> named) {
        var fields = new ArrayList<FieldAttribute>();
        for (String label : named) {
            if (PromqlLabels.getField(relation, label) instanceof FieldAttribute field
                && DataType.isString(field.dataType())
                && (field.isDimension() == false || aliased(relation, field))) {
                fields.add(field);
            }
        }
        return fields;
    }

    /**
     * Whether another dimension of {@code relation} ends with the path of {@code field}, as the field a passthrough alias names, and
     * the container holds it under another name: it holds a {@code labels.} field under its label name already.
     */
    private static boolean aliased(EsRelation relation, FieldAttribute field) {
        String label = field.fieldName().string();
        String suffix = "." + label;
        for (Attribute attribute : relation.output()) {
            if (attribute instanceof FieldAttribute other
                && other.isDimension()
                && other.fieldName().string().endsWith(suffix)
                && PromqlLabels.labelName(other).equals(label) == false) {
                return true;
            }
        }
        return false;
    }
}
