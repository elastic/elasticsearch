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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.Header;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapFinite;

/**
 * Dedicated logical node for the PromQL label-manipulation functions {@code label_replace} and {@code label_join}.
 * <p>
 * Both operate purely on a series' labels/identity - never on its sample values - deriving a destination label from one
 * or more source labels and returning an instant vector with the same cardinality as the input. Unlike the other PromQL
 * function families, these are not lowered through the generic function builder: the derivation is wired directly during
 * translation (see {@code TranslatePromqlToEsqlPlan}). The {@code parameters()} carried by this node are the
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
        // The destination label is materialized during translation as a derived column exposed to the enclosing
        // aggregation, so at the logical level the label set is that of the child; the derived label is surfaced only
        // once translation runs.
        return child().output();
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
     * Translates a {@code label_replace}/{@code label_join} into a derived label column.
     * <p>
     * The child is first collapsed to one row per series (forcing the initial aggregate when it has not happened yet), so
     * the source labels are materialized as columns to derive from. The destination value is then computed with an
     * {@link Eval} under the destination's stable id and declared as a label of the result, so the enclosing
     * {@code by(...)} aggregation groups on it exactly as it would on a stored label. The derived label shadows a stored
     * label of the same name: any such stored column is projected away so a lookup by name binds to the derived one.
     * <p>
     * Because ES|QL treats {@code null} and {@code ""} as distinct grouping keys while Prometheus treats an absent label
     * and an empty label value alike, every "label absent" outcome is normalized to {@code ""}: an absent source is
     * coalesced to {@code ""} before matching, and {@code label_replace}'s no-match ({@code null}) is coalesced back to
     * {@code ""}. All such series therefore fall into the same group, matching Prometheus.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        // The child must expose the labels the derivation reads, on top of whatever the enclosing translation requires.
        Header childRequired = context.required().union(finite(sourceLabels()));
        IntermediateResult child = context.withRequired(childRequired).translate(child());
        if (child.kind().constant) {
            return child;
        }

        // Collapse to one row per series so the source labels exist as columns; this mirrors the seam in
        // translateIntermediate that forces the initial per-series aggregate for a not-yet-aggregated subtree.
        IntermediateResult aggregated = child.kind().afterInitialAggregation
            ? child
            : context.collapse(child, child.header(), child.value());

        Source source = source();
        Attribute destination = destination();
        Expression destinationValue = definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
            ? labelReplaceValue(context, source, aggregated)
            : labelJoinValue(context, source, aggregated);

        String name = mapFinite(destination);
        Alias derived = new Alias(source, destination.name(), destinationValue, destination.id());
        LogicalPlan plan = new Eval(context.cmd().source(), aggregated.plan(), List.of(derived));
        var unshadowed = new ArrayList<NamedExpression>();
        for (Attribute attribute : plan.output()) {
            if (attribute.id().equals(derived.id()) || mapFinite(attribute).equals(name) == false) {
                unshadowed.add(attribute);
            }
        }
        if (unshadowed.size() < plan.output().size()) {
            plan = new Project(context.cmd().source(), plan, unshadowed);
        }
        Header header = aggregated.header().union(finite(List.of(name)));
        return new IntermediateResult(
            plan,
            header,
            aggregated.value(),
            aggregated.step(),
            aggregated.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    /**
     * The {@code label_replace} destination value:
     * {@code COALESCE(RegexExpand(COALESCE(src, ""), regex, repl), existingDst)}.
     * The inner coalesce feeds the empty string when the source label is absent (so the regex matches against {@code ""}
     * like Prometheus). The outer coalesce implements Prometheus's no-match semantics: a no-match ({@code null}) leaves
     * the destination label unchanged, so it falls back to the destination's existing value - the stored label when the
     * destination overwrites one, or {@code ""} (the "absent" grouping key) when the destination is a new label. A match
     * with an empty expansion (the delete sentinel) resolves to {@code ""}, joining that same "absent" group.
     */
    private Expression labelReplaceValue(TranslationContext context, Source source, IntermediateResult table) {
        List<Expression> params = parameters();
        String srcLabel = literalString(params.get(2));
        Expression regex = params.get(3);
        Expression replacement = params.get(1);
        Expression src = sourceLabelValue(context, source, table, srcLabel);
        Expression extracted = new RegexExpand(source, src, regex, replacement);
        Expression existingDst = sourceLabelValue(context, source, table, mapFinite(destination()));
        return new Coalesce(source, extracted, List.of(existingDst));
    }

    /**
     * The {@code label_join} destination value: the source label values coalesced to {@code ""} and joined by the
     * separator. With no source labels the result is {@code ""} - the same "absent" grouping key produced by
     * {@code label_replace}; a single source label is copied verbatim (no separator). With two or more source labels the
     * separator is inserted between every value, so even all-empty sources yield the separator run (for example a
     * {@code "-"} separator over two absent labels produces {@code "-"}), matching Prometheus.
     */
    private Expression labelJoinValue(TranslationContext context, Source source, IntermediateResult table) {
        List<Expression> params = parameters();
        Literal separator = Literal.keyword(source, literalString(params.get(1)));

        List<Expression> parts = new ArrayList<>(2 * params.size() + 1);
        for (int i = 2; i < params.size(); i++) {
            if (parts.isEmpty() == false) {
                parts.add(separator);
            }
            parts.add(sourceLabelValue(context, source, table, literalString(params.get(i))));
        }

        return switch (parts.size()) {
            case 0 -> Literal.keyword(source, "");
            case 1 -> parts.getFirst();
            default -> new Concat(source, parts.getFirst(), parts.subList(1, parts.size()));
        };
    }

    /**
     * The value of a source label as a non-null string: {@code COALESCE(ToString(label), "")}, or {@code ""} if the
     * table does not carry the label. The lookup reads the table's plan, so it sees stored labels only: a destination an
     * enclosing {@code by(dst)} requires is a name in the header, never a column here, and cannot resolve to itself.
     */
    private Expression sourceLabelValue(TranslationContext context, Source source, IntermediateResult table, String labelName) {
        Attribute label = table.label(labelName);
        if (label == null) {
            return Literal.keyword(source, "");
        }
        Expression stringValue = DataType.isString(label.dataType()) ? label : new ToString(source, label, context.configuration());
        return new Coalesce(source, stringValue, List.of(Literal.keyword(source, "")));
    }

}
