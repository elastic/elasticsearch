/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.LabelTranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Translates a {@link PromqlCommand} over a label container: the query translated by {@link LabelTranslationContext}, projected
 * to the command's declared output, each declared label read off the result's label container.
 */
public final class PromqlCommandLabelTranslation {

    // An unbounded range query has no start or end; its steps take the widest representable range.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    private PromqlCommandLabelTranslation() {}

    /** The command's ES|QL plan. */
    public static LogicalPlan translate(PromqlCommand cmd, AnalyzerContext analyzer) {
        var context = new LabelTranslationContext(cmd, analyzer);
        IntermediateResult result = context.translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
        boolean constant = result.kind().constant;
        return finish(cmd, emitFinalProjection(cmd, result.plan(), result.labels(), constant, context), constant);
    }

    /** Keeps only steps within the query range; steps are anchored at {@code start} and offset-independent. */
    private static LogicalPlan emitByStepFilter(PromqlCommand cmd, LogicalPlan plan) {
        var source = cmd.source();
        var step = cmd.stepAttribute();
        var start = cmd.start();
        var end = cmd.end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    /** Every command drops missing samples and bounds its evaluation steps after exposing the public columns. */
    private static LogicalPlan finish(PromqlCommand cmd, LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), plan, cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(cmd, plan);
    }

    /**
     * Projects the command output. Value and step bind to the finished table's columns by name. Every label the command
     * declares reads the result's label container {@code labels}, under its declared name, id and type, and the declared
     * {@code _timeseries} is the container without them. A constant table carries no labels. Typed {@code _timeseries}
     * stays internal: it binds under an id of its own and the command exposes its keyword form.
     */
    private static LogicalPlan emitFinalProjection(
        PromqlCommand cmd,
        LogicalPlan plan,
        @Nullable Expression labels,
        boolean constant,
        LabelTranslationContext context
    ) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(attr.name(), attr);
        }
        var projected = new ArrayList<NamedExpression>(cmd.output());
        var evals = new ArrayList<Alias>();
        List<Attribute> declared = cmd.promqlPlan().output();
        // A column exposed only to order the result is read from the container but stays in _timeseries too
        Set<NameId> orderingOnly = new HashSet<>();
        cmd.promqlPlan().forEachDown(LogicalPlan.class, p -> {
            if (p instanceof ResultOrderingFunction ordering) {
                ordering.orderingOnlyColumns().forEach(column -> orderingOnly.add(column.id()));
            }
        });
        List<String> named = PromqlLabels.labelNames(
            declared.stream().filter(a -> PromqlLabels.isTagsLabel(a) == false && orderingOnly.contains(a.id()) == false).toList()
        );
        Source source = cmd.promqlPlan().source();
        Alias rendered = null;
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i).toAttribute();
            // the value and step lead the declared output, then the result's labels
            if (i >= 2 && constant == false) {
                Attribute label = declared.get(i - 2);
                if (PromqlLabels.isTagsLabel(label)) {
                    Expression rest = labels == null
                        ? LabelTranslationContext.emptyLabels(source)
                        : new TimeSeriesMetadataFilter(labels.source(), labels, PromqlLabels.labelLiterals(labels.source(), named), true);
                    Alias labelContainer = new Alias(source, attr.name(), rest);
                    evals.add(labelContainer);
                    rendered = new Alias(
                        source,
                        attr.name(),
                        new ToString(source, labelContainer.toAttribute(), context.configuration()),
                        attr.id()
                    );
                    projected.set(i, labelContainer.toAttribute());
                    continue;
                }
                Expression value = labels == null
                    ? new Literal(source, null, attr.dataType())
                    : asType(
                        new TimeSeriesMetadataPath(
                            labels.source(),
                            labels,
                            Literal.keyword(labels.source(), PromqlLabels.labelName(label))
                        ),
                        attr.dataType(),
                        context
                    );
                Alias alias = new Alias(source, attr.name(), value, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
                continue;
            }
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                if (lookupAttr.resolved()) {
                    // Remap the finished table's column onto the command's id via Eval, then project the attribute.
                    evals.add(alias);
                    projected.set(i, alias.toAttribute());
                } else {
                    // Union branches often expose an unresolved value/step until Bind resolves aggregates under them.
                    // Wrapping that in Eval makes Alias.toAttribute() an UnresolvedAttribute that shadows the real
                    // column by name once the child resolves (Unknown column [value]/result]). Keep the Alias in
                    // the Project so ResolveRefs rebinds it after the union output is refreshed.
                    projected.set(i, alias);
                }
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        plan = new Project(cmd.source(), plan, projected);
        if (rendered == null) {
            return plan;
        }
        plan = new Eval(cmd.source(), plan, List.of(rendered));
        // in the declared order: the Eval appends the rendered column
        var exposed = new ArrayList<NamedExpression>(projected.size());
        for (NamedExpression projection : projected) {
            exposed.add(projection.name().equals(rendered.name()) ? rendered.toAttribute() : projection.toAttribute());
        }
        return new Project(cmd.source(), plan, exposed);
    }

    /** A label's text as the declared label's type: a label read from the container is text, a dimension may not be. */
    private static Expression asType(Expression text, DataType type, LabelTranslationContext context) {
        if (type == DataType.KEYWORD || type == DataType.NULL) {
            return text;
        }
        return EsqlDataTypeConverter.converterFunctionFactory(type).apply(text.source(), text, context.configuration());
    }

}
