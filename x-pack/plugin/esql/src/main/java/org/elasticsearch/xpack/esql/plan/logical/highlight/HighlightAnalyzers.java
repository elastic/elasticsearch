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
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
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

    private HighlightAnalyzers() {}

    /**
     * The per-field analyzers, in ON order, for every combination of analyzers some row needs.
     *
     * @param variants        {@code variants.getFirst()} applies to rows whose index is not in {@code variantByIndex}
     * @param variantByIndex  index name to the position in {@code variants} its rows use. Empty when all rows share one.
     */
    public record Resolved(List<Map<String, NamedAnalyzer>> variants, Map<String, Integer> variantByIndex) {
        public Map<String, NamedAnalyzer> defaultAnalyzers() {
            return variants.getFirst();
        }
    }

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
            if (commandAnalyzer != null) {
                defaults.put(name, commandAnalyzer);
            } else if (perIndex
                && field instanceof FieldAttribute fa
                && fa.field() instanceof TextEsField text
                && text.analyzerGroups() != null) {
                    NamedAnalyzer standard = PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry);
                    for (IndexAnalyzerGroup group : text.analyzerGroups()) {
                        NamedAnalyzer analyzer = groupAnalyzer(name, group, standard, analysisRegistry, warnings);
                        for (String index : group.indices()) {
                            overridesByIndex.computeIfAbsent(index, k -> new LinkedHashMap<>()).put(name, analyzer);
                        }
                    }
                    defaults.put(name, standard);
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

    private static NamedAnalyzer analyzerOf(NamedExpression field, @Nullable AnalysisRegistry analysisRegistry, Consumer<String> warnings) {
        // Only a FieldAttribute still carries the mapping analyzer. RENAME and EVAL produce a ReferenceAttribute,
        // which keeps a TO_TEXT analyzer but not a mapping one, so a renamed mapped field falls back to standard.
        if (field instanceof FieldAttribute fa && fa.field() instanceof TextEsField text) {
            return mappingAnalyzer(field.name(), text, analysisRegistry, warnings);
        }
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(field);
        return PlannerUtils.resolveAnalyzer(Objects.requireNonNullElse(declared, DEFAULT_ANALYZER_NAME), analysisRegistry);
    }

    private static NamedAnalyzer mappingAnalyzer(
        String fieldName,
        TextEsField text,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        String fallbackReason = switch (text.unknownAnalyzer()) {
            case NONE -> null;
            case CONFLICT -> "the queried indices disagree on the analyzer for this field";
            case INDEX_LOCAL -> INDEX_LOCAL_REASON;
        };
        if (text.analyzerName() != null) {
            try {
                return withGap(PlannerUtils.resolveAnalyzer(text.analyzerName(), analysisRegistry), text.positionIncrementGap());
            } catch (InvalidArgumentException e) {
                // index.analysis names arrive as INDEX_LOCAL, so this is a plugin analyzer this node did not load.
                fallbackReason = unregisteredReason(text.analyzerName());
            }
        }
        if (fallbackReason != null) {
            warnings.accept("HIGHLIGHT on [" + fieldName + "] falls back to [standard]: " + fallbackReason + WARNING_SUFFIX);
        }
        return PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry);
    }

    /** Like {@link #mappingAnalyzer} for one group of indices; the warning names the indices that fall back. */
    private static NamedAnalyzer groupAnalyzer(
        String fieldName,
        IndexAnalyzerGroup group,
        NamedAnalyzer standard,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        String fallbackReason = INDEX_LOCAL_REASON;
        if (group.analyzerName() != null) {
            try {
                return withGap(PlannerUtils.resolveAnalyzer(group.analyzerName(), analysisRegistry), group.positionIncrementGap());
            } catch (InvalidArgumentException e) {
                fallbackReason = unregisteredReason(group.analyzerName());
            }
        }
        warnings.accept(
            "HIGHLIGHT on ["
                + fieldName
                + "] uses [standard] for indices "
                + new TreeSet<>(group.indices())
                + ": "
                + fallbackReason
                + WARNING_SUFFIX
        );
        return standard;
    }

    private static NamedAnalyzer withGap(NamedAnalyzer resolved, int gap) {
        return resolved.getPositionIncrementGap(resolved.name()) == gap ? resolved : new NamedAnalyzer(resolved, gap);
    }

    private static String unregisteredReason(String analyzerName) {
        return "analyzer [" + analyzerName + "] is not registered on this node";
    }

    private static final String INDEX_LOCAL_REASON = "its analyzer is defined in the index settings, which no node can rebuild by name";
    private static final String WARNING_SUFFIX =
        ". Highlights may differ from what matched; specify WITH {\"analyzer\": <registered analyzer>} to control this.";
}
