/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.Locale;

/**
 * Compute-side configuration handed straight to {@link HighlightOperator}.
 * The planner resolves the user-facing {@code WITH { ... }} options into the primitive values carried here, and the
 * operator builds its Lucene machinery from them. Keeping this record in the compute module (rather than referencing
 * the ES|QL planning-layer options type) means the operator depends only on plain primitives.
 *
 * @param wordBoundary       when {@code true} the unified highlighter breaks fragments on word boundaries instead of
 *                           sentences (the {@code boundary_scanner=word} option).
 * @param locale             locale used by the break iterator (the {@code boundary_scanner_locale} option).
 * @param orderByScore       when {@code true} fragments are returned by descending score instead of document order
 *                           (the {@code order=score} option).
 * @param maxAnalyzedOffset  per-field analysis bound; a negative value means "use the default index setting" in the
 *                           current coordinator-side operator.
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
    int maxAnalyzedOffset
) {

    /** Encoder name that escapes HTML markup in the highlighted text; any other value uses the default (no escaping). */
    public static final String HTML_ENCODER = "html";

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
            + ", max_analyzed_offset="
            + maxAnalyzedOffset;
    }

    @Override
    public String toString() {
        return "HighlightConfig[" + describe() + "]";
    }
}
