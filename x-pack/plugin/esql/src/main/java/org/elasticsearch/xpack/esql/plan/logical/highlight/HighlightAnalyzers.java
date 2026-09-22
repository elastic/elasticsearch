/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders.DEFAULT_ANALYZER_NAME;

/**
 * Analyzer used to tokenize each HIGHLIGHT ON field.
 * WITH {@code analyzer} applies to every field. Otherwise a mapped text field uses {@link TextEsField#analyzerName},
 * a TO_TEXT column uses its declared analyzer, and anything else uses {@code standard}.
 */
public final class HighlightAnalyzers {

    private HighlightAnalyzers() {}

    /** Convenience overload for the local-planner path where warnings would be emitted twice. */
    public static Map<String, NamedAnalyzer> resolve(
        List<? extends NamedExpression> onFields,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        return resolve(onFields, commandAnalyzerName, analysisRegistry, w -> {});
    }

    /**
     * Map from each ON field name to the analyzer that tokenizes that field's values, in ON order.
     * A mapping analyzer that fails to resolve on this node falls back to {@code standard} and emits a
     * warning through {@code warnings}. Names typed by the user ({@code WITH}, {@code TO_TEXT}) still throw.
     */
    public static Map<String, NamedAnalyzer> resolve(
        List<? extends NamedExpression> onFields,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        NamedAnalyzer commandAnalyzer = PlannerUtils.resolveAnalyzer(commandAnalyzerName, analysisRegistry);
        Map<String, NamedAnalyzer> fieldAnalyzers = new LinkedHashMap<>();
        for (NamedExpression field : onFields) {
            fieldAnalyzers.put(field.name(), commandAnalyzer != null ? commandAnalyzer : analyzerOf(field, analysisRegistry, warnings));
        }
        return fieldAnalyzers;
    }

    private static NamedAnalyzer analyzerOf(NamedExpression field, @Nullable AnalysisRegistry analysisRegistry, Consumer<String> warnings) {
        // Known limitation: only a FieldAttribute still knows its mapping analyzer. RENAME and EVAL mint a
        // ReferenceAttribute, which carries a declared TO_TEXT analyzer but not a mapping one, so a renamed mapped
        // field drops to standard (highlight.csv-spec: highlightMappingAnalyzerRenamedFieldFallsBackToStandard and
        // highlightMappingAnalyzerLostByRenameUnderImplicitQuery). Forwarding it through Alias#toAttribute would need
        // the gap and the fail-open-on-unknown behaviour to ride along, since an unknown TO_TEXT analyzer is an error
        // while an unknown mapping analyzer is a warning, and both would arrive as the same string.
        if (field instanceof FieldAttribute fa && fa.field() instanceof TextEsField text) {
            return mappingAnalyzer(field.name(), text, analysisRegistry, warnings);
        }
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(field);
        return PlannerUtils.resolveAnalyzer(Objects.requireNonNullElse(declared, DEFAULT_ANALYZER_NAME), analysisRegistry);
    }

    /**
     * The mapping analyzer with its {@code position_increment_gap}, or {@code standard} plus a warning when this node
     * cannot rebuild it, because the highlight may then differ from what matched.
     */
    private static NamedAnalyzer mappingAnalyzer(
        String fieldName,
        TextEsField text,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        String fallbackReason = switch (text.unknownAnalyzer()) {
            case NONE -> null;
            case CONFLICT -> "the queried indices disagree on the analyzer for this field";
            case INDEX_LOCAL -> "its analyzer is defined in the index settings, which no node can rebuild by name";
        };
        if (text.analyzerName() != null) {
            try {
                NamedAnalyzer resolved = PlannerUtils.resolveAnalyzer(text.analyzerName(), analysisRegistry);
                int gap = text.positionIncrementGap();
                return resolved.getPositionIncrementGap(resolved.name()) == gap ? resolved : new NamedAnalyzer(resolved, gap);
            } catch (InvalidArgumentException e) {
                // index.analysis names arrive as INDEX_LOCAL, so this is a plugin analyzer this node did not load.
                // Fail open: the name came from the mapping, and a hard error would punish the user for the mapping.
                fallbackReason = "analyzer [" + text.analyzerName() + "] is not registered on this node";
            }
        }
        if (fallbackReason != null) {
            warnings.accept(
                "HIGHLIGHT on ["
                    + fieldName
                    + "] falls back to [standard]: "
                    + fallbackReason
                    + ". Highlights may differ from what matched; specify WITH {\"analyzer\": <registered analyzer>} to control this."
            );
        }
        return PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry);
    }
}
