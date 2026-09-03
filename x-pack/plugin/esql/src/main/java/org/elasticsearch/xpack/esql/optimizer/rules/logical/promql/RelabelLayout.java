/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.finite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.mapFinite;

/**
 * The plan block of one {@code label_replace}/{@code label_join}: the destination label derived as a column on top of the
 * operand, shadowing a stored label of the same name. The translator translates the operand and collapses it to one row
 * per series so the source labels exist as columns, then feeds it in; {@link #result()} derives the destination value,
 * defines it under the relabel's own attribute and projects away the stored label it overwrites.
 * <pre>
 * new RelabelLayout(relabel).command(cmd).configuration(cfg).input(collapsed).result()
 * </pre>
 * <p>
 * A label the derivation reads but the operand lacks is read as {@code ""}, and {@code label_replace}'s no-match is
 * coalesced back to the destination's existing value ({@code ""} for a new label), so such series all fall into the
 * same group - matching Prometheus, where a missing label and an empty label are the same thing.
 */
final class RelabelLayout {
    private final MetadataManipulationFunction relabel;

    private PromqlCommand cmd;
    private Configuration configuration;
    private IntermediateResult input;

    RelabelLayout(MetadataManipulationFunction relabel) {
        this.relabel = relabel;
    }

    RelabelLayout command(PromqlCommand cmd) {
        this.cmd = cmd;
        return this;
    }

    RelabelLayout configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    /** The relabel's operand, collapsed to one row per series so its source labels exist as columns. */
    RelabelLayout input(IntermediateResult input) {
        this.input = input;
        return this;
    }

    /** The operand's table with the destination label derived as a column, in place of a stored label of the same name. */
    IntermediateResult result() {
        assert cmd != null && configuration != null && input != null : "invariant: every input must be set before laying out a relabel";
        assert input.kind().afterInitialAggregation : "invariant: the operand must be collapsed to one row per series";
        Source source = relabel.source();
        Attribute destination = relabel.destination();
        Expression destinationValue = relabel.definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
            ? labelReplaceValue(source)
            : labelJoinValue(source);

        String name = mapFinite(destination);
        Alias derived = new Alias(source, destination.name(), destinationValue, destination.id());
        LogicalPlan plan = new Eval(cmd.source(), input.plan(), List.of(derived));
        var unshadowed = new ArrayList<NamedExpression>();
        for (Attribute attribute : plan.output()) {
            if (attribute.id().equals(derived.id()) || mapFinite(attribute).equals(name) == false) {
                unshadowed.add(attribute);
            }
        }
        if (unshadowed.size() < plan.output().size()) {
            plan = new Project(cmd.source(), plan, unshadowed);
        }
        Header header = input.header().union(finite(List.of(name)));
        return new IntermediateResult(plan, header, input.value(), input.step(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
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
    private Expression labelReplaceValue(Source source) {
        List<Expression> params = relabel.parameters();
        String srcLabel = literalString(params.get(2));
        Expression regex = params.get(3);
        Expression replacement = params.get(1);
        Expression src = sourceLabelValue(source, srcLabel);
        Expression extracted = new RegexExpand(source, src, regex, replacement);
        Expression existingDst = sourceLabelValue(source, mapFinite(relabel.destination()));
        return new Coalesce(source, extracted, List.of(existingDst));
    }

    /**
     * The {@code label_join} destination value: the source label values coalesced to {@code ""} and joined by the
     * separator. With no source labels the result is {@code ""} - the same "absent" grouping key produced by
     * {@code label_replace}; a single source label is copied verbatim (no separator). With two or more source labels the
     * separator is inserted between every value, so even all-empty sources yield the separator run (for example a
     * {@code "-"} separator over two absent labels produces {@code "-"}), matching Prometheus.
     */
    private Expression labelJoinValue(Source source) {
        List<Expression> params = relabel.parameters();
        Literal separator = Literal.keyword(source, literalString(params.get(1)));

        List<Expression> parts = new ArrayList<>(2 * params.size() + 1);
        for (int i = 2; i < params.size(); i++) {
            if (parts.isEmpty() == false) {
                parts.add(separator);
            }
            parts.add(sourceLabelValue(source, literalString(params.get(i))));
        }

        return switch (parts.size()) {
            case 0 -> Literal.keyword(source, "");
            case 1 -> parts.getFirst();
            default -> new Concat(source, parts.getFirst(), parts.subList(1, parts.size()));
        };
    }

    /**
     * The value of a source label as a non-null string: {@code COALESCE(ToString(label), "")}, or {@code ""} if the
     * operand does not carry the label. The lookup reads the operand's plan, so it sees stored labels only: a destination
     * an enclosing {@code by(dst)} requires is a name in the header, never a column here, and cannot resolve to itself.
     */
    private Expression sourceLabelValue(Source source, String labelName) {
        Attribute label = input.label(labelName);
        if (label == null) {
            return Literal.keyword(source, "");
        }
        Expression stringValue = DataType.isString(label.dataType()) ? label : new ToString(source, label, configuration);
        return new Coalesce(source, stringValue, List.of(Literal.keyword(source, "")));
    }

    /** The string value of a keyword-literal PromQL function argument. */
    private static String literalString(Expression literal) {
        return BytesRefs.toString(((Literal) literal).value());
    }
}
