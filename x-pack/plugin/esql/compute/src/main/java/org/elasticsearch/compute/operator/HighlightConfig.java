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
import java.util.Objects;

/**
 * Compute-side configuration handed straight to {@link HighlightOperator}.
 * <p>
 * It contains two groups of values:
 * <ul>
 *     <li>user-facing highlight options resolved from {@code WITH { ... }}</li>
 *     <li>execution context (per-field {@link NamedAnalyzer}s, translated {@link Query}, and target field names)
 *     attached during planning via {@link #withExecutionContext(List, Query, List)}</li>
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
 * @param analyzerName       {@code analyzer} override from WITH, applied to every field; {@code null} lets each field
 *                           use its borrowed leaf analyzer or the default analyzer.
 * @param maxAnalyzedOffset  per-field analysis bound; a negative value means "use the default index setting" in the
 *                           current coordinator-side operator.
 * @param fieldAnalyzers     the analyzer each ON field is analyzed and searched with, aligned by index with
 *                           {@code fieldNames}; each field may carry a different analyzer.
 * @param query              translated Lucene query used for matching and snippet extraction.
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
    List<NamedAnalyzer> fieldAnalyzers,
    Query query,
    List<String> fieldNames
) {

    /** Encoder name that escapes HTML markup in the highlighted text; any other value uses the default (no escaping). */
    public static final String HTML_ENCODER = "html";

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
            null,
            List.of()
        );
    }

    public HighlightConfig {
        fieldAnalyzers = List.copyOf(fieldAnalyzers);
        fieldNames = List.copyOf(fieldNames);
    }

    public HighlightConfig withExecutionContext(List<NamedAnalyzer> fieldAnalyzers, Query query, List<String> fieldNames) {
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
            fieldAnalyzers,
            query,
            fieldNames
        );
    }

    public List<NamedAnalyzer> requiredFieldAnalyzers() {
        if (fieldAnalyzers.isEmpty()) {
            throw new IllegalStateException("HIGHLIGHT field analyzers must be set in execution context");
        }
        return fieldAnalyzers;
    }

    public Query requiredQuery() {
        return Objects.requireNonNull(query, "HIGHLIGHT query must be set in execution context");
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
            + describeAnalyzers()
            + ", max_analyzed_offset="
            + maxAnalyzedOffset;
    }

    /** One analyzer name, or {@code {field=analyzer, ...}} when fields differ. Falls back to {@link #analyzerName}. */
    private String describeAnalyzers() {
        if (fieldAnalyzers.isEmpty()) {
            return String.valueOf(analyzerName);
        }
        List<String> names = fieldAnalyzers.stream().map(NamedAnalyzer::name).toList();
        if (names.stream().distinct().count() == 1) {
            return names.get(0);
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldNames.get(i)).append('=').append(names.get(i));
        }
        return sb.append('}').toString();
    }

    @Override
    public String toString() {
        return "HighlightConfig[" + describe() + "]";
    }
}
