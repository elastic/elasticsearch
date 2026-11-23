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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated logical node for the PromQL label-manipulation functions {@code label_replace} and {@code label_join}.
 * <p>
 * Both operate purely on a series' labels/identity - never on its sample values - deriving a destination label from one
 * or more source labels and returning an instant vector with the same cardinality as the input. Unlike the other PromQL
 * function families, these are not lowered through the generic function builder: the derivation is wired directly during
 * translation (see {@link #translate}). The {@code parameters()} carried by this node are the
 * keyword-literal arguments after the child instant vector:
 * <ul>
 *     <li>{@code label_replace}: {@code [dst_label, replacement, src_label, regex]}</li>
 *     <li>{@code label_join}: {@code [dst_label, separator, src_label_1, ... src_label_N]}</li>
 * </ul>
 * <p>
 * The destination label may be a new label or may overwrite a stored label (a dimension or {@code __name__}). To let an
 * enclosing {@code by(dst)} bind to the derived value exactly like a stored label, the node mints a stable
 * {@link #destination()} attribute once (following the {@code Grok}/{@code Dissect} generated-attribute pattern) and
 * threads it unchanged through the node's lifecycle. Analysis adds that attribute to the PromQL resolution scope of
 * enclosing nodes, shadowing a stored label of the same name for references that consume the relabeled vector (see
 * {@code Analyzer#resolvePromql} for the positional, subtree-scoped shadowing rules), so the derived identity flows
 * through the aggregate output and the command output contract, and translation materializes the derived column under
 * that same id.
 */
public final class MetadataManipulationFunction extends PromqlFunctionCall {

    private final Attribute destination;

    public MetadataManipulationFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters
    ) {
        this(source, child, definition, parameters, new ReferenceAttribute(source, null, destinationName(parameters), DataType.KEYWORD));
    }

    private MetadataManipulationFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Attribute destination
    ) {
        super(source, child, definition, parameters);
        this.destination = destination;
    }

    /**
     * The derived destination label as a stable attribute. Its {@code NameId} is minted once and preserved across the
     * node's lifecycle, so the analyzer, the aggregate/command output contract, and the translated column all refer to
     * the one identity - the derived label thus behaves exactly like a stored label end to end.
     */
    public Attribute destination() {
        return destination;
    }

    /**
     * The labels the derivation reads, which the operand must expose as columns: {@code label_replace}'s source label,
     * {@code label_join}'s source labels, plus the destination itself - {@code label_replace} falls back to its existing
     * value on no-match, and exposing it lets the derived column shadow a stored label of the same name.
     */
    public List<String> sourceLabels() {
        List<Expression> params = parameters();
        List<String> labels = new ArrayList<>();
        if (definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE) {
            labels.add(literalString(params.get(2)));
        } else {
            for (int i = 2; i < params.size(); i++) {
                labels.add(literalString(params.get(i)));
            }
        }
        labels.add(PromqlLabels.labelName(destination));
        // An empty label name names no label: its value reads as the empty string.
        labels.removeIf(String::isEmpty);
        return labels;
    }

    /** The destination label name, taken from the first keyword-literal argument ({@code dst}). */
    private static String destinationName(List<Expression> parameters) {
        return literalString(parameters.getFirst());
    }

    /** The string value of a keyword-literal argument. */
    private static String literalString(Expression literal) {
        return BytesRefs.toString(((Literal) literal).value());
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, MetadataManipulationFunction::new, child(), definition(), parameters(), destination);
    }

    @Override
    public MetadataManipulationFunction replaceChild(LogicalPlan newChild) {
        return new MetadataManipulationFunction(source(), newChild, definition(), parameters(), destination);
    }

    @Override
    public List<Attribute> output() {
        // The destination label is materialized during translation as a derived column, so the label set is the child's
        // with the destination shadowing a same-named label or appended as a new one. An enclosing by(...) aggregation
        // ignores this and exposes its own groupings; it matters for a consumer that keeps the child's identity - a
        // reduction, or the command's own output contract when the relabel is the outermost node.
        List<Attribute> childOutput = child().output();
        List<Attribute> labels = new ArrayList<>(childOutput.size() + 1);
        boolean shadowed = false;
        // Shadowing compares PromQL label names so a `labels.`-prefixed destination shadows the bare label and vice versa.
        String destinationLabel = PromqlLabels.labelName(destination);
        for (Attribute label : childOutput) {
            if (PromqlLabels.labelName(label).equals(destinationLabel)) {
                labels.add(destination);
                shadowed = true;
            } else {
                labels.add(label);
            }
        }
        if (shadowed == false) {
            labels.add(destination);
        }
        return labels;
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.METADATA_MANIPULATION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // A relabel passes series identity through unchanged, so a nested relabel is consumed by the same enclosing
        // consumer as this one.
        return true;
    }

    /**
     * Translates a {@code label_replace}/{@code label_join}: the destination label of each series is written into its label
     * container. The child is first collapsed to one row per series (forcing the initial aggregate when it has not happened
     * yet), so each series' labels are read once.
     * <p>
     * Because ES|QL treats {@code null} and {@code ""} as distinct values while Prometheus treats an absent label and an empty
     * label value alike, an absent source label reads as {@code ""}, and an empty destination value removes the label.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        IntermediateResult child = context.translate(child());
        if (child.kind().constant) {
            return child;
        }
        IntermediateResult aggregated = child.kind().afterInitialAggregation ? child : context.emitInitialAggregate(child, child.value());
        Source source = source();
        Expression labels = aggregated.labels() != null ? aggregated.labels() : TranslationContext.emptyLabels(source);
        String name = PromqlLabels.labelName(destination());
        // The value once, under a column of its own the destination reads: an empty value removes the label, as in
        // Prometheus, so the destination reads as absent, like a label never set.
        Alias computed = new Alias(
            source,
            TemporaryNameGenerator.locallyUniqueTemporaryName(name),
            definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
                ? labelReplaceValue(source, labels, name)
                : labelJoinValue(source, labels)
        );
        Attribute value = computed.toAttribute();
        Expression empty = new Equals(source, value, Literal.keyword(source, ""));
        Alias derived = new Alias(
            source,
            TemporaryNameGenerator.locallyUniqueTemporaryName(name),
            new Case(source, empty, List.of(new Literal(source, null, DataType.KEYWORD), value))
        );
        LogicalPlan plan = new Eval(context.cmd().source(), aggregated.plan(), List.of(computed, derived));
        Expression relabeled = new TimeSeriesMetadataSet(
            source,
            labels,
            Literal.keyword(source, name),
            derived.toAttribute(),
            context.labelPath(name)
        );
        if (overwrites(context, name)) {
            // Overwriting a label can leave two series with one label set, which Prometheus fails.
            Alias container = new Alias(source, TemporaryNameGenerator.locallyUniqueTemporaryName(MetadataAttribute.TIMESERIES), relabeled);
            plan = new Eval(context.cmd().source(), plan, List.of(container));
            plan = TranslationContext.uniqueSeries(context.cmd().source(), plan, aggregated.step(), container.toAttribute());
            relabeled = container.toAttribute();
        }
        return aggregated.with(plan, aggregated.value()).withLabels(relabeled);
    }

    /**
     * Whether the destination may name a label the input series carry, so that overwriting it can leave two series with one
     * label set: the input declares it, or its series keep their source labels and the source maps a field for it.
     */
    private boolean overwrites(TranslationContext context, String name) {
        boolean concrete = child().output().stream().noneMatch(a -> MetadataAttribute.isTimeSeriesAttributeName(a.name()));
        return concrete
            ? child().output().stream().anyMatch(label -> PromqlLabels.labelName(label).equals(name))
            : context.cmd().child().output().stream().anyMatch(field -> PromqlLabels.labelName(field).equals(name));
    }

    /**
     * The {@code label_replace} destination value: {@code COALESCE(RegexExpand(COALESCE(src, ""), regex, repl), dst)}. An
     * absent source matches as the empty string, like in Prometheus; a no-match ({@code null}) leaves the destination as it
     * is, and an empty expansion removes it.
     */
    private Expression labelReplaceValue(Source source, Expression labels, String destination) {
        List<Expression> params = parameters();
        Expression extracted = new RegexExpand(
            source,
            sourceLabelValue(source, labels, literalString(params.get(2))),
            params.get(3),
            params.get(1)
        );
        return new Coalesce(source, extracted, List.of(PromqlLabels.read(labels, destination)));
    }

    /**
     * The {@code label_join} destination value: the source label values, absent ones as {@code ""}, joined by the
     * separator. With no source labels the result is empty, which removes the destination like an empty
     * {@code label_replace}; a single source label is copied verbatim (no separator). With two or more source labels the
     * separator is inserted between every value, so even all-empty sources yield the separator run (for example a
     * {@code "-"} separator over two absent labels produces {@code "-"}), matching Prometheus.
     */
    private Expression labelJoinValue(Source source, Expression labels) {
        List<Expression> params = parameters();
        Literal separator = Literal.keyword(source, literalString(params.get(1)));
        List<Expression> parts = new ArrayList<>(2 * params.size() + 1);
        for (int i = 2; i < params.size(); i++) {
            if (parts.isEmpty() == false) {
                parts.add(separator);
            }
            parts.add(sourceLabelValue(source, labels, literalString(params.get(i))));
        }
        return switch (parts.size()) {
            case 0 -> Literal.keyword(source, "");
            case 1 -> parts.getFirst();
            default -> new Concat(source, parts.getFirst(), parts.subList(1, parts.size()));
        };
    }

    /** The value of a source label as a non-null string. No label has an empty name, so one named so reads as {@code ""}. */
    private static Expression sourceLabelValue(Source source, Expression labels, String labelName) {
        if (labelName.isEmpty()) {
            return Literal.keyword(source, "");
        }
        return new Coalesce(source, PromqlLabels.read(labels, labelName), List.of(Literal.keyword(source, "")));
    }

}
