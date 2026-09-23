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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders.DEFAULT_ANALYZER_NAME;

/**
 * Analyzer used to tokenize each HIGHLIGHT ON field.
 * WITH {@code analyzer} applies to every field. Otherwise a mapped text field uses {@link TextEsField#analyzerName},
 * a TO_TEXT column uses its declared analyzer, and anything else uses {@code standard}. When the queried indices
 * disagree on a field's analyzer and the row's {@code _index} is available, each index uses its own analyzer.
 */
public final class HighlightAnalyzers {

    private static final String INDEX_LOCAL_REASON = "its analyzer is defined in the index settings, which no node can rebuild by name";

    private HighlightAnalyzers() {}

    /**
     * The per-field analyzers, in ON order, for every combination of analyzers some row needs.
     *
     * @param variants        {@code variants.getFirst()} applies to rows whose index is not in {@code variantByIndex}
     * @param variantByIndex  index name to the position in {@code variants} its rows use. Empty when all rows share one.
     */
    public record Resolved(List<Map<String, NamedAnalyzer>> variants, Map<String, Integer> variantByIndex) {}

    /**
     * A mapping analyzer that fails to resolve on this node falls back to {@code standard} and emits a warning
     * through {@code warnings}. Names typed by the user ({@code WITH}, {@code TO_TEXT}) still throw.
     *
     * @param perIndex whether the operator will know each row's index, so disagreeing indices can each use their own
     *                 analyzer instead of falling back to {@code standard}
     */
    public static Resolved resolve(
        List<? extends NamedExpression> onFields,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry,
        boolean perIndex,
        Consumer<String> warnings
    ) {
        NamedAnalyzer commandAnalyzer = PlannerUtils.resolveAnalyzer(commandAnalyzerName, analysisRegistry);
        Map<String, NamedAnalyzer> defaults = new LinkedHashMap<>();
        Map<String, Map<String, NamedAnalyzer>> overridesByIndex = new TreeMap<>();
        for (NamedExpression field : onFields) {
            String name = field.name();
            List<IndexAnalyzerGroup> groups = perIndex ? analyzerGroups(field) : null;
            if (commandAnalyzer != null) {
                defaults.put(name, commandAnalyzer);
            } else if (groups != null) {
                defaults.put(name, PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry));
                for (IndexAnalyzerGroup group : groups) {
                    NamedAnalyzer analyzer = mappingAnalyzer(
                        name,
                        " for indices " + new TreeSet<>(group.indices()),
                        group.analyzerName(),
                        group.positionIncrementGap(),
                        INDEX_LOCAL_REASON,
                        analysisRegistry,
                        warnings
                    );
                    group.indices()
                        .forEach(index -> overridesByIndex.computeIfAbsent(index, k -> new LinkedHashMap<>()).put(name, analyzer));
                }
            } else {
                defaults.put(name, analyzerOf(field, analysisRegistry, warnings));
            }
        }
        // Indices that end up with the same analyzer and gap for every field share a variant.
        List<Map<String, NamedAnalyzer>> variants = new ArrayList<>();
        variants.add(defaults);
        Map<List<AnalyzerKey>, Integer> variantIds = new LinkedHashMap<>();
        variantIds.put(AnalyzerKey.of(defaults), 0);
        Map<String, Integer> variantByIndex = new LinkedHashMap<>();
        overridesByIndex.forEach((index, overrides) -> {
            Map<String, NamedAnalyzer> analyzers = new LinkedHashMap<>(defaults);
            analyzers.putAll(overrides);
            Integer variant = variantIds.computeIfAbsent(AnalyzerKey.of(analyzers), k -> {
                variants.add(analyzers);
                return variants.size() - 1;
            });
            variantByIndex.put(index, variant);
        });
        return new Resolved(variants, variantByIndex);
    }

    /** {@link NamedAnalyzer#equals} only compares names; the gap matters for phrase matches across values. */
    private record AnalyzerKey(String name, int positionIncrementGap) {
        static List<AnalyzerKey> of(Map<String, NamedAnalyzer> fieldAnalyzers) {
            return fieldAnalyzers.entrySet()
                .stream()
                .map(e -> new AnalyzerKey(e.getValue().name(), e.getValue().getPositionIncrementGap(e.getKey())))
                .toList();
        }
    }

    /** Which indices use which analyzer when the queried indices disagree on a mapped text field, otherwise {@code null}. */
    public static @Nullable List<IndexAnalyzerGroup> analyzerGroups(NamedExpression field) {
        EsField esField = field instanceof FieldAttribute fa ? fa.field() : null;
        // Partially unmapped fields stay wrapped until UnionTypesCleanup.
        if (esField instanceof PotentiallyUnmappedSingleTypeEsField punk) {
            esField = punk.mappedField();
        }
        return esField instanceof TextEsField text ? text.analyzerGroups() : null;
    }

    private static NamedAnalyzer analyzerOf(NamedExpression field, @Nullable AnalysisRegistry analysisRegistry, Consumer<String> warnings) {
        // Only a FieldAttribute still carries the mapping analyzer. RENAME and EVAL produce a ReferenceAttribute,
        // which keeps a TO_TEXT analyzer but not a mapping one, so a renamed mapped field falls back to standard.
        if (field instanceof FieldAttribute fa && fa.field() instanceof TextEsField text) {
            String fallbackReason = switch (text.unknownAnalyzer()) {
                case NONE -> null;
                case CONFLICT -> "the queried indices disagree on the analyzer for this field";
                case INDEX_LOCAL -> INDEX_LOCAL_REASON;
            };
            return mappingAnalyzer(
                field.name(),
                "",
                text.analyzerName(),
                text.positionIncrementGap(),
                fallbackReason,
                analysisRegistry,
                warnings
            );
        }
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(field);
        return PlannerUtils.resolveAnalyzer(Objects.requireNonNullElse(declared, DEFAULT_ANALYZER_NAME), analysisRegistry);
    }

    /**
     * {@code analyzerName} with the field's {@code gap}, or {@code standard} and a warning when there is no name or this
     * node cannot resolve it. {@code scope} names the indices the fallback applies to, empty when it applies to every row.
     */
    private static NamedAnalyzer mappingAnalyzer(
        String fieldName,
        String scope,
        @Nullable String analyzerName,
        int gap,
        @Nullable String fallbackReason,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        if (analyzerName != null) {
            try {
                NamedAnalyzer resolved = PlannerUtils.resolveAnalyzer(analyzerName, analysisRegistry);
                return resolved.getPositionIncrementGap(resolved.name()) == gap ? resolved : new NamedAnalyzer(resolved, gap);
            } catch (InvalidArgumentException e) {
                // index.analysis names are withheld, so this is a plugin analyzer this node did not load.
                fallbackReason = "analyzer [" + analyzerName + "] is not registered on this node";
            }
        }
        if (fallbackReason != null) {
            warnings.accept(
                "HIGHLIGHT on ["
                    + fieldName
                    + "] falls back to [standard]"
                    + scope
                    + ": "
                    + fallbackReason
                    + ". Highlights may differ from what matched; specify WITH {\"analyzer\": <registered analyzer>} to control this."
            );
        }
        return PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry);
    }
}
