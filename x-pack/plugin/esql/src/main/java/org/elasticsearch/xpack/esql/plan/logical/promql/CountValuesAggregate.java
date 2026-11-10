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
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.FloatToLabelValue;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

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
     * already is one), then a regroup by the grouping keys plus the sample value counts the series sharing each value.
     * The value groups as a double so equal values group together whatever their text, and is formatted to the label
     * text once per output group rather than once per series.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        String label = TranslationContext.mapFinite(valueLabel);
        List<String> keys = TranslationContext.mapFinite(output()).stream().filter(k -> k.equals(label) == false).toList();
        IntermediateResult ir = context.withRequired(union(open(), finite(keys))).translate(child());
        Expression value = ir.value();
        if (value.resolved() && value.dataType().isHistogram()) {
            throw new ParsingException(
                source(),
                "Error building ESQL function for [{}]: {}() is not supported yet on native histograms",
                functionName(),
                functionName()
            );
        }
        if (ir.kind().constant) {
            return translateCountValuesConstant(context, ir, keys);
        }
        IntermediateResult table = ir.kind().afterInitialAggregation
            ? ir
            : context.collapse(ir, union(ir.schema(), finite(keys)), ir.value());

        Source source = source();
        Alias key = new Alias(source, label, groupingKey(table.valueColumn()));
        LogicalPlan keyed = new Eval(source, table.plan(), List.of(key));
        // the key replaces a stored label of the same name, which the regroup would otherwise prefer as its backing field
        List<Attribute> shadowed = keyed.output()
            .stream()
            .filter(a -> a.id().equals(key.id()) == false && TranslationContext.mapFinite(a).equals(label))
            .toList();
        if (shadowed.isEmpty() == false) {
            keyed = new Project(source, keyed, keyed.output().stream().filter(a -> shadowed.contains(a) == false).toList());
        }

        List<String> regroupKeys = new ArrayList<>(keys);
        regroupKeys.add(label);
        IntermediateResult counted = context.regroup(
            table.with(keyed, union(table.schema(), finite(List.of(label))), table.value()),
            finite(regroupKeys),
            false,
            new Count(source, key.toAttribute())
        );
        counted = context.eval(counted, TranslationContext.zeroCountAsNull(counted.valueColumn()));
        Alias formatted = new Alias(source, valueLabel.name(), new FloatToLabelValue(source, counted.label(label)), valueLabel.id());
        return counted.with(new Eval(source, counted.plan(), List.of(formatted)), counted.schema(), counted.value());
    }

    /**
     * A constant child is a single element per step, so it counts 1 under its own value. The grouping keys it lacks are
     * null, and the columns outside the result's label set are projected away so no enclosing node reads them.
     */
    private IntermediateResult translateCountValuesConstant(TranslationContext context, IntermediateResult ir, List<String> keys) {
        Source source = source();
        String label = TranslationContext.mapFinite(valueLabel);
        List<Alias> fields = new ArrayList<>();
        for (String key : keys) {
            if (ir.label(key) == null) {
                fields.add(TranslationContext.emitNullExpression(TranslationContext.mapToRef(key)));
            }
        }
        Alias formatted = new Alias(source, valueLabel.name(), new FloatToLabelValue(source, asDouble(ir.value())), valueLabel.id());
        fields.add(formatted);
        List<String> labels = new ArrayList<>(keys);
        labels.add(label);
        TranslationSchema schema = finite(labels);
        Expression one = new Case(source, new IsNull(source, ir.value()), List.of(Literal.NULL, new Literal(source, 1.0, DataType.DOUBLE)));
        IntermediateResult counted = context.eval(ir.with(new Eval(source, ir.plan(), fields), schema, ir.value()), one);
        List<Attribute> projections = new ArrayList<>(List.of(counted.valueColumn(), counted.step()));
        for (String key : keys) {
            projections.add(counted.label(key));
        }
        projections.add(formatted.toAttribute());
        return counted.with(new Project(source, counted.plan(), projections), schema, counted.value());
    }

    private Expression asDouble(Expression value) {
        return value.dataType() == DataType.DOUBLE ? value : new ToDouble(source(), value);
    }

    /**
     * The value as a double grouping key. Packed multi-key hashing compares a double's raw bits, so a value that can be
     * NaN maps every NaN payload to the one canonical NaN; a whole number never converts to NaN.
     */
    private Expression groupingKey(Attribute value) {
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
