/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.FloatToLabelValue;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The PromQL {@code count_values(label, v)} aggregation: counts the elements sharing each distinct sample value and exposes
 * that value as the label {@code label}. The value label replaces a grouping label of the same name, as in Prometheus.
 */
public final class CountValuesAggregate extends AcrossSeriesAggregate {

    private final Attribute valueLabel;

    public CountValuesAggregate(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Grouping grouping,
        List<Attribute> groupings
    ) {
        this(source, child, definition, parameters, grouping, groupings, mintValueLabel(source, parameters));
    }

    /**
     * Rebuilds a node keeping an already minted {@link #valueLabel()}, so the label keeps its identity across rewrites.
     */
    public CountValuesAggregate(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Grouping grouping,
        List<Attribute> groupings,
        Attribute valueLabel
    ) {
        super(source, child, definition, parameters, grouping, groupings);
        this.valueLabel = valueLabel;
    }

    private static Attribute mintValueLabel(Source source, List<Expression> parameters) {
        String name = BytesRefs.toString(((Literal) parameters.getFirst()).value());
        return new ReferenceAttribute(source, null, name, DataType.KEYWORD);
    }

    /**
     * The label {@code count_values} derives from each sample value. Its {@code NameId} is minted once, so the analyzer,
     * the output contract and the translated column share one identity, like a stored label.
     */
    public Attribute valueLabel() {
        return valueLabel;
    }

    /** Whether {@code plan} contains a {@code count_values} aggregation. */
    public static boolean usesCountValues(LogicalPlan plan) {
        return plan.anyMatch(CountValuesAggregate.class::isInstance);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, CountValuesAggregate::new, child(), definition(), parameters(), grouping(), groupings(), valueLabel());
    }

    @Override
    public CountValuesAggregate replaceChild(LogicalPlan newChild) {
        return new CountValuesAggregate(source(), newChild, definition(), parameters(), grouping(), groupings(), valueLabel());
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(valueLabel, ((CountValuesAggregate) o).valueLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueLabel);
    }

    /**
     * The aggregate's labels plus the value label, which replaces a grouping label of the same name as it does in
     * Prometheus.
     */
    @Override
    public List<Attribute> output() {
        List<Attribute> labels = super.output();
        if (grouping() == Grouping.WITHOUT) {
            return labels;
        }
        String valueLabelName = PromqlLabels.labelName(valueLabel);
        List<Attribute> output = new ArrayList<>(labels.size() + 1);
        for (Attribute label : labels) {
            if (PromqlLabels.labelName(label).equals(valueLabelName) == false) {
                output.add(label);
            }
        }
        output.add(valueLabel);
        return output;
    }

    /**
     * {@code count_values} is two aggregation levels: the child collapses to one value per series (an aggregated child
     * already is one), then an aggregate keyed by the labels of each group with the value label written counts the series
     * sharing each value. The value label is the text Prometheus gives the value, so values of one text count together.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        IntermediateResult ir = context.translate(child());
        Expression value = ir.value();
        if (value.resolved() && value.dataType().isHistogram()) {
            throw new ParsingException(source(), "{} function is not yet supported on native histograms", functionName());
        }
        if (ir.kind().constant) {
            // A constant child is a single element per step, so it counts 1 under its own value.
            Expression one = new Case(
                source(),
                new IsNull(source(), ir.value()),
                List.of(Literal.NULL, new Literal(source(), 1.0, DataType.DOUBLE))
            );
            IntermediateResult counted = context.eval(ir, one);
            Expression text = new FloatToLabelValue(source(), groupingKey(asDouble(ir.value())));
            Expression group = groupLabels(context, ir.labels());
            return counted.withLabels(valueLabels(context, group == null ? TranslationContext.emptyLabels(source()) : group, text));
        }
        IntermediateResult table = ir.kind().afterInitialAggregation ? ir : context.emitInitialAggregate(ir, ir.value());
        Expression group = groupLabels(context, table.labels());
        // the text of each series' value under a name of its own, which the label written into the group's labels reads
        Alias text = new Alias(
            source(),
            TemporaryNameGenerator.locallyUniqueTemporaryName(PromqlLabels.labelName(valueLabel)),
            new FloatToLabelValue(source(), groupingKey(table.valueColumn()))
        );
        table = table.with(new Eval(context.cmd().source(), table.plan(), List.of(text)), table.value());
        Expression labels = valueLabels(context, group == null ? TranslationContext.emptyLabels(source()) : group, text.toAttribute());
        IntermediateResult counted = context.emitAggregate(table, labels, new Count(source(), table.valueColumn()));
        return context.eval(counted, TranslationContext.zeroCountAsNull(counted.valueColumn()));
    }

    /** {@code labels} with the value label written as {@code text}, which replaces a label of the same name. */
    private Expression valueLabels(TranslationContext context, Expression labels, Expression text) {
        Source source = source();
        String name = PromqlLabels.labelName(valueLabel);
        return new TimeSeriesMetadataSet(source, labels, Literal.keyword(source, name), text, context.labelPath(name));
    }

    private Expression asDouble(Expression value) {
        return value.dataType() == DataType.DOUBLE ? value : new ToDouble(source(), value);
    }

    /**
     * The value as a double grouping key. Packed multi-key hashing compares a double's raw bits, so a value that can be
     * NaN maps every NaN payload to the one canonical NaN; a whole number never converts to NaN.
     */
    private Expression groupingKey(Expression value) {
        Expression asDouble = asDouble(value);
        DataType type = value.dataType();
        if (type.noCounter().isWholeNumber()) {
            return asDouble;
        }
        Source source = source();
        return new Case(
            source,
            new NotEquals(source, asDouble, asDouble),
            List.of(new Literal(source, Double.NaN, DataType.DOUBLE), asDouble)
        );
    }
}
