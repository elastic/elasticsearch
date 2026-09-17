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
import org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Analyzer used to tokenize each HIGHLIGHT ON field.
 * WITH {@code analyzer} applies to every field. Otherwise a mapped text field uses {@link TextEsField#analyzerName},
 * a TO_TEXT column uses its declared analyzer, and anything else uses {@code standard}.
 */
public final class HighlightAnalyzers {

    private HighlightAnalyzers() {}

    /** Map from each ON field name to the analyzer that tokenizes that field's values, in ON order. */
    public static Map<String, NamedAnalyzer> resolve(
        List<? extends NamedExpression> onFields,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        Map<String, NamedAnalyzer> fieldAnalyzers = new LinkedHashMap<>();
        for (NamedExpression field : onFields) {
            fieldAnalyzers.put(field.name(), analyzerOf(field, commandAnalyzerName, analysisRegistry));
        }
        return fieldAnalyzers;
    }

    private static NamedAnalyzer analyzerOf(
        NamedExpression field,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        if (commandAnalyzerName != null) {
            return PlannerUtils.resolveAnalyzer(commandAnalyzerName, analysisRegistry);
        }
        if (field instanceof FieldAttribute fa && fa.field() instanceof TextEsField text && text.analyzerName() != null) {
            try {
                return PlannerUtils.resolveAnalyzer(text.analyzerName(), analysisRegistry);
            } catch (InvalidArgumentException e) {
                // index.analysis name this node cannot build. Fail open to standard. The name came from the mapping,
                // not the query.
                // TODO: warn when a mapping analyzer falls back.
                return PlannerUtils.resolveAnalyzer(HighlightQueryBuilders.DEFAULT_ANALYZER_NAME, analysisRegistry);
            }
        }
        // TO_TEXT already verified a declared analyzer, so this resolve either succeeds or never ran.
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(field);
        return PlannerUtils.resolveAnalyzer(declared != null ? declared : HighlightQueryBuilders.DEFAULT_ANALYZER_NAME, analysisRegistry);
    }
}
