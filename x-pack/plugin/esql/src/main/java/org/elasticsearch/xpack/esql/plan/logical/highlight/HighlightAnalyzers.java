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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders.DEFAULT_ANALYZER_NAME;

/**
 * Analyzer used to tokenize each HIGHLIGHT ON field.
 * WITH {@code analyzer} applies to every field. Otherwise a mapped text field, or a FORK or UNION ALL column merged
 * from mapped fields, uses {@link TextEsField#analyzerName}, a TO_TEXT column uses its declared analyzer, and anything
 * else uses {@code standard}. When the queried indices disagree on a field's analyzer and the row's {@code _index} is
 * available, each index uses its own analyzer.
 */
public final class HighlightAnalyzers {

    private static final String INDEX_LOCAL_REASON = "its analyzer is defined in the index settings, which no node can rebuild by name";
    private static final String NOT_REPORTED_REASON = "the node holding it did not report its analyzer";

    private HighlightAnalyzers() {}

    /**
     * The per-field analyzers, in ON order, for every combination of analyzers some row needs.
     *
     * @param analysisGroups  {@code analysisGroups.getFirst()} applies to rows whose index is not in {@code groupByIndex}
     * @param groupByIndex    index name to the position in {@code analysisGroups} its rows use, only for indices that do
     *                        not use the first. Empty when all rows share one.
     */
    public record Resolved(List<Map<String, NamedAnalyzer>> analysisGroups, Map<String, Integer> groupByIndex) {}

    /**
     * A mapping analyzer that fails to resolve on this node falls back to {@code standard} and emits a warning
     * through {@code warnings}. Names typed by the user ({@code WITH}, {@code TO_TEXT}) still throw.
     *
     * @param fieldMappings the mapping of each ON column that FORK or UNION ALL merged from mapped fields, by name
     * @param perIndex whether the operator will know each row's index, so disagreeing indices can each use their own
     *                 analyzer instead of falling back to {@code standard}
     */
    public static Resolved resolve(
        List<? extends NamedExpression> onFields,
        Map<String, TextEsField> fieldMappings,
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
            List<IndexAnalyzerGroup> groups = perIndex ? analyzerGroups(field, fieldMappings) : null;
            if (commandAnalyzer != null) {
                defaults.put(name, commandAnalyzer);
            } else if (groups != null) {
                defaults.put(name, standard(analysisRegistry));
                for (IndexAnalyzerGroup group : groups) {
                    NamedAnalyzer analyzer = mappingAnalyzer(
                        name,
                        group.indices(),
                        group.analyzerName(),
                        group.positionIncrementGap(),
                        group.indexLocal() ? INDEX_LOCAL_REASON : NOT_REPORTED_REASON,
                        analysisRegistry,
                        warnings
                    );
                    group.indices()
                        .forEach(index -> overridesByIndex.computeIfAbsent(index, k -> new LinkedHashMap<>()).put(name, analyzer));
                }
            } else {
                defaults.put(name, analyzerOf(field, fieldMappings, analysisRegistry, warnings));
            }
        }
        // Unlike an IndexAnalyzerGroup, which covers one field, indices share an analysis group only when they end up
        // with the same analyzer and gap for every field.
        List<Map<String, NamedAnalyzer>> analysisGroups = new ArrayList<>();
        analysisGroups.add(defaults);
        Map<List<AnalyzerKey>, Integer> groupIds = new LinkedHashMap<>();
        groupIds.put(AnalyzerKey.of(defaults), 0);
        Map<String, Integer> groupByIndex = new LinkedHashMap<>();
        overridesByIndex.forEach((index, overrides) -> {
            Map<String, NamedAnalyzer> analyzers = new LinkedHashMap<>(defaults);
            analyzers.putAll(overrides);
            int groupId = groupIds.computeIfAbsent(AnalyzerKey.of(analyzers), k -> {
                analysisGroups.add(analyzers);
                return analysisGroups.size() - 1;
            });
            if (groupId != 0) {
                groupByIndex.put(index, groupId);
            }
        });
        return new Resolved(analysisGroups, groupByIndex);
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
    public static @Nullable List<IndexAnalyzerGroup> analyzerGroups(NamedExpression field, Map<String, TextEsField> fieldMappings) {
        TextEsField text = mappingOf(field, fieldMappings);
        return text == null ? null : text.analyzerGroups();
    }

    /**
     * The text mapping {@code field} is analyzed with: a mapped field's own, or the one {@code fieldMappings} carries for
     * a column FORK or UNION ALL merged from mapped fields. {@code null} for any other column.
     */
    public static @Nullable TextEsField mappingOf(NamedExpression field, Map<String, TextEsField> fieldMappings) {
        EsField esField = field instanceof FieldAttribute fa ? fa.field() : fieldMappings.get(field.name());
        // Partially unmapped fields stay wrapped until UnionTypesCleanup.
        if (esField instanceof PotentiallyUnmappedSingleTypeEsField punk) {
            esField = punk.mappedField();
        }
        return esField instanceof TextEsField text ? text : null;
    }

    private static NamedAnalyzer analyzerOf(
        NamedExpression field,
        Map<String, TextEsField> fieldMappings,
        @Nullable AnalysisRegistry analysisRegistry,
        Consumer<String> warnings
    ) {
        // RENAME and EVAL produce a ReferenceAttribute, which keeps a TO_TEXT analyzer but not a mapping one, so a
        // renamed mapped field falls back to standard.
        TextEsField text = mappingOf(field, fieldMappings);
        if (text != null) {
            String fallbackReason = switch (text.unknownAnalyzer()) {
                case NONE -> null;
                case CONFLICT -> "the queried indices disagree on the analyzer for this field";
                case INDEX_LOCAL -> INDEX_LOCAL_REASON;
                case BRANCH_CONFLICT -> "the FORK or UNION ALL branches disagree on the analyzer for this column";
            };
            return mappingAnalyzer(
                field.name(),
                Set.of(),
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
     * node cannot resolve it. {@code indices} are the ones the fallback applies to, empty when it applies to every row.
     */
    private static NamedAnalyzer mappingAnalyzer(
        String fieldName,
        Set<String> indices,
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
                    + (indices.isEmpty() ? "" : " for indices " + new TreeSet<>(indices))
                    + ": "
                    + fallbackReason
                    + ". Highlights may differ from what matched; specify WITH {\"analyzer\": <registered analyzer>} to control this."
            );
        }
        return standard(analysisRegistry);
    }

    private static NamedAnalyzer standard(@Nullable AnalysisRegistry analysisRegistry) {
        return PlannerUtils.resolveAnalyzer(DEFAULT_ANALYZER_NAME, analysisRegistry);
    }
}
