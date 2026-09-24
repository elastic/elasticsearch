/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Compute-side configuration handed straight to {@link HighlightOperator}.
 * <p>
 * It contains two groups of values:
 * <ul>
 *     <li>user-facing highlight options resolved from {@code WITH { ... }}</li>
 *     <li>execution context (per-field {@link NamedAnalyzer}s and translated {@link Query} per {@link AnalysisGroup}, and
 *     target field names) attached during planning via {@link #withExecutionContext(List, Map, List)}</li>
 * </ul>
 * Keeping this record in the compute module (rather than referencing the ES|QL planning-layer options type) keeps
 * operator wiring localized to the compute package.
 *
 * @param queryText          source query text used in debug/plan descriptions.
 * @param preTag             opening tag inserted before each matched term.
 * @param postTag            closing tag inserted after each matched term.
 * @param encoder            encoder mode; {@link #HTML_ENCODER} escapes markup, any other value keeps raw text.
 * @param numberOfFragments  maximum number of fragments per field; {@code 0} means return the whole value.
 * @param fragmentSize       preferred fragment length in characters when sentence boundaries are used.
 * @param noMatchSize        fallback leading-text size returned when no query match is found.
 * @param wordBoundary       when {@code true} the unified highlighter breaks fragments on word boundaries instead of
 *                           sentences (the {@code boundary_scanner=word} option).
 * @param locale             locale used by the break iterator (the {@code boundary_scanner_locale} option).
 * @param orderByScore       when {@code true} fragments are returned by descending score instead of document order
 *                           (the {@code order=score} option).
 * @param analyzerName       {@code analyzer} from WITH, applied to every field. {@code null} means each field uses
 *                           its mapping analyzer, a TO_TEXT declaration, or {@code standard}.
 * @param maxAnalyzedOffset  per-field analysis bound; a negative value means "use the default index setting" in the
 *                           current coordinator-side operator.
 * @param analysisGroups     analyzers and query per combination of analyzers some row needs. Rows use
 *                           {@code analysisGroups.getFirst()} unless {@code groupByIndex} says otherwise.
 * @param groupByIndex       {@code _index} value to the position in {@code analysisGroups} its rows use. Empty when
 *                           every row shares the first group.
 * @param fieldNames         highlighted field names, in the same order as field evaluators.
 */
public record HighlightConfig(
    String queryText,
    String preTag,
    String postTag,
    String encoder,
    int numberOfFragments,
    int fragmentSize,
    int noMatchSize,
    boolean wordBoundary,
    Locale locale,
    boolean orderByScore,
    String analyzerName,
    int maxAnalyzedOffset,
    List<AnalysisGroup> analysisGroups,
    Map<String, Integer> groupByIndex,
    List<String> fieldNames
) {

    /** Encoder name that escapes HTML markup in the highlighted text; any other value uses the default (no escaping). */
    public static final String HTML_ENCODER = "html";

    /** Caps the indices {@link #describe()} names per analysis group, as there can be thousands. */
    private static final int MAX_DESCRIBED_INDICES = 3;

    /**
     * The analyzer each ON field is analyzed and searched with, aligned by index with {@link #fieldNames}, and the
     * Lucene query translated with those analyzers. Indices that analyze every ON field the same way share one group.
     */
    public record AnalysisGroup(List<NamedAnalyzer> fieldAnalyzers, Query query) {
        public AnalysisGroup {
            fieldAnalyzers = List.copyOf(fieldAnalyzers);
            Objects.requireNonNull(query, "HIGHLIGHT query must be set in execution context");
        }
    }

    public HighlightConfig(
        String queryText,
        String preTag,
        String postTag,
        String encoder,
        int numberOfFragments,
        int fragmentSize,
        int noMatchSize,
        boolean wordBoundary,
        Locale locale,
        boolean orderByScore,
        String analyzerName,
        int maxAnalyzedOffset
    ) {
        this(
            queryText,
            preTag,
            postTag,
            encoder,
            numberOfFragments,
            fragmentSize,
            noMatchSize,
            wordBoundary,
            locale,
            orderByScore,
            analyzerName,
            maxAnalyzedOffset,
            List.of(),
            Map.of(),
            List.of()
        );
    }

    public HighlightConfig {
        analysisGroups = List.copyOf(analysisGroups);
        groupByIndex = Map.copyOf(groupByIndex);
        fieldNames = List.copyOf(fieldNames);
    }

    /** Single-group shorthand: every row uses {@code fieldAnalyzers} and {@code query}. */
    public HighlightConfig withExecutionContext(List<NamedAnalyzer> fieldAnalyzers, Query query, List<String> fieldNames) {
        return withExecutionContext(List.of(new AnalysisGroup(fieldAnalyzers, query)), Map.of(), fieldNames);
    }

    /** Multi-group form: {@code groupByIndex} maps an {@code _index} value to the position in {@code analysisGroups} its rows use. */
    public HighlightConfig withExecutionContext(
        List<AnalysisGroup> analysisGroups,
        Map<String, Integer> groupByIndex,
        List<String> fieldNames
    ) {
        return new HighlightConfig(
            queryText,
            preTag,
            postTag,
            encoder,
            numberOfFragments,
            fragmentSize,
            noMatchSize,
            wordBoundary,
            locale,
            orderByScore,
            analyzerName,
            maxAnalyzedOffset,
            analysisGroups,
            groupByIndex,
            fieldNames
        );
    }

    public List<AnalysisGroup> requiredAnalysisGroups() {
        if (analysisGroups.isEmpty()) {
            throw new IllegalStateException("HIGHLIGHT field analyzers must be set in execution context");
        }
        return analysisGroups;
    }

    public String describe() {
        return "query="
            + queryText
            + ", pre_tag="
            + preTag
            + ", post_tag="
            + postTag
            + ", encoder="
            + encoder
            + ", number_of_fragments="
            + numberOfFragments
            + ", fragment_size="
            + fragmentSize
            + ", no_match_size="
            + noMatchSize
            + ", word_boundary="
            + wordBoundary
            + ", locale="
            + locale
            + ", order_by_score="
            + orderByScore
            + ", analyzer="
            + (analysisGroups.isEmpty() ? analyzerName : describeAnalyzers(analysisGroups.getFirst()))
            + describePerIndexAnalyzers()
            + ", max_analyzed_offset="
            + maxAnalyzedOffset;
    }

    /** One analyzer name, or {@code {field=analyzer, ...}} when fields differ. */
    private String describeAnalyzers(AnalysisGroup group) {
        List<NamedAnalyzer> fieldAnalyzers = group.fieldAnalyzers();
        if (fieldAnalyzers.stream().map(NamedAnalyzer::name).distinct().count() == 1) {
            return fieldAnalyzers.getFirst().name();
        }
        return IntStream.range(0, fieldNames.size())
            .mapToObj(i -> fieldNames.get(i) + "=" + fieldAnalyzers.get(i).name())
            .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * {@code , per_index_analyzer=[analyzer=[index, ...], ...]} for the groups after the first, in group order and naming
     * at most {@link #MAX_DESCRIBED_INDICES} indices each; empty when every row uses the first group.
     */
    private String describePerIndexAnalyzers() {
        if (groupByIndex.isEmpty()) {
            return "";
        }
        List<SortedSet<String>> indicesByGroup = analysisGroups.stream().<SortedSet<String>>map(g -> new TreeSet<>()).toList();
        groupByIndex.forEach((index, group) -> indicesByGroup.get(group).add(index));
        return IntStream.range(1, analysisGroups.size())
            .mapToObj(g -> describeAnalyzers(analysisGroups.get(g)) + "=" + describeIndices(indicesByGroup.get(g)))
            .collect(Collectors.joining(", ", ", per_index_analyzer=[", "]"));
    }

    private static String describeIndices(SortedSet<String> indices) {
        String sample = indices.stream().limit(MAX_DESCRIBED_INDICES).collect(Collectors.joining(", "));
        int more = indices.size() - MAX_DESCRIBED_INDICES;
        return "[" + sample + (more > 0 ? ", ...and " + more + " more" : "") + "]";
    }

    @Override
    public String toString() {
        return "HighlightConfig[" + describe() + "]";
    }
}
