/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates {@code label_replace} and {@code label_join} over a label container: the destination label of each series is
 * written into its container.
 * <p>
 * Because ES|QL treats {@code null} and {@code ""} as distinct values while Prometheus treats an absent label and an empty
 * label value alike, an absent source label reads as {@code ""}, and an empty destination value removes the label.
 */
final class MetadataManipulationFunctionV2 {

    private MetadataManipulationFunctionV2() {}

    static IntermediateResult translate(MetadataManipulationFunction relabel, TranslationContextV2 context) {
        IntermediateResult child = context.translate(relabel.child());
        if (child.kind().constant) {
            return child;
        }
        // The rewrite reads each series' labels, so the series collapse first; a vector without labels has an empty set.
        IntermediateResult aggregated = child.kind().afterInitialAggregation ? child : context.emitInitialAggregate(child, child.value());
        Expression labels = aggregated.labels() != null ? aggregated.labels() : TranslationContextV2.emptyLabels(relabel.source());
        Expression value = relabel.definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
            ? labelReplaceValue(relabel, labels)
            : labelJoinValue(relabel, labels);
        return aggregated.withLabels(
            new TimeSeriesMetadataSet(
                labels.source(),
                labels,
                Literal.keyword(labels.source(), PromqlLabels.labelName(relabel.destination())),
                value
            )
        );
    }

    /**
     * The {@code label_replace} destination value: {@code COALESCE(RegexExpand(COALESCE(src, ""), regex, repl), dst)}. An
     * absent source matches as the empty string, like in Prometheus; a no-match ({@code null}) leaves the destination as it
     * is, and an empty expansion removes it.
     */
    private static Expression labelReplaceValue(MetadataManipulationFunction relabel, Expression labels) {
        List<Expression> params = relabel.parameters();
        Source source = relabel.source();
        Expression extracted = new RegexExpand(
            source,
            sourceLabelValue(source, labels, literalString(params.get(2))),
            params.get(3),
            params.get(1)
        );
        Expression destination = new TimeSeriesMetadataPath(
            labels.source(),
            labels,
            Literal.keyword(labels.source(), PromqlLabels.labelName(relabel.destination()))
        );
        return new Coalesce(source, extracted, List.of(destination));
    }

    /**
     * The {@code label_join} destination value: the source label values, absent ones as {@code ""}, joined by the separator.
     * With no source labels the result is empty, which removes the destination like an empty {@code label_replace}.
     */
    private static Expression labelJoinValue(MetadataManipulationFunction relabel, Expression labels) {
        List<Expression> params = relabel.parameters();
        Source source = relabel.source();
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
        return new Coalesce(
            source,
            new TimeSeriesMetadataPath(labels.source(), labels, Literal.keyword(labels.source(), labelName)),
            List.of(Literal.keyword(source, ""))
        );
    }

    /** The string value of a keyword-literal argument. */
    private static String literalString(Expression literal) {
        return BytesRefs.toString(((Literal) literal).value());
    }
}
