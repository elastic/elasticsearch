/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.util.List;

/**
 * Resolved, plain-Java view of the {@code WITH { ... }} options of a {@link Highlight} command, with defaults applied.
 * Built once at local-execution-planning time from the (foldable) {@link MapExpression} so the operator factory only
 * deals with primitives.
 */
// TODO: extend this record to carry the remaining options so they reach the operator:
// - boundary_scanner (sentence|word), boundary_scanner_locale, boundary_chars, boundary_max_scan -> break iterator
// - order (none|score) -> passage sort comparator
// - max_analyzed_offset -> QueryMaxAnalyzedOffset / re-analysis bound (currently always the index-setting default)
// - phrase_limit -> phrase matching cap
// TODO: add an analyzer name field here once the "analyzer" option is supported.
public record HighlightOptions(String preTag, String postTag, String encoder, int numberOfFragments, int fragmentSize, int noMatchSize) {

    public static final String DEFAULT_PRE_TAG = "<em>";
    public static final String DEFAULT_POST_TAG = "</em>";
    public static final String DEFAULT_ENCODER = "default";
    public static final String HTML_ENCODER = "html";
    public static final int DEFAULT_NUMBER_OF_FRAGMENTS = 5;
    public static final int DEFAULT_FRAGMENT_SIZE = 100;
    public static final int DEFAULT_NO_MATCH_SIZE = 0;

    public static HighlightOptions from(MapExpression options, FoldContext foldContext) {
        if (options == null) {
            return new HighlightOptions(
                DEFAULT_PRE_TAG,
                DEFAULT_POST_TAG,
                DEFAULT_ENCODER,
                DEFAULT_NUMBER_OF_FRAGMENTS,
                DEFAULT_FRAGMENT_SIZE,
                DEFAULT_NO_MATCH_SIZE
            );
        }
        return new HighlightOptions(
            string(options.get(Highlight.PRE_TAGS), foldContext, DEFAULT_PRE_TAG),
            string(options.get(Highlight.POST_TAGS), foldContext, DEFAULT_POST_TAG),
            string(options.get(Highlight.ENCODER), foldContext, DEFAULT_ENCODER),
            integer(options.get(Highlight.NUMBER_OF_FRAGMENTS), foldContext, DEFAULT_NUMBER_OF_FRAGMENTS),
            integer(options.get(Highlight.FRAGMENT_SIZE), foldContext, DEFAULT_FRAGMENT_SIZE),
            integer(options.get(Highlight.NO_MATCH_SIZE), foldContext, DEFAULT_NO_MATCH_SIZE)
        );
    }

    /**
     * Reads a string option. Tags may be given as a single string ({@code "pre_tags": "<b>"}) or a list.
     */
    // TODO: support multiple pre_tags/post_tags (Query DSL rotates through the list per match) instead of rejecting them.
    private static String string(Expression value, FoldContext foldContext, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        Object folded = value.fold(foldContext);
        if (folded instanceof List<?> list) {
            if (list.size() > 1) {
                throw new IllegalArgumentException(
                    "HIGHLIGHT does not support multiple tags yet, but got [" + list.size() + "]; provide a single tag"
                );
            }
            return list.isEmpty() ? defaultValue : BytesRefs.toString(list.getFirst());
        }
        return BytesRefs.toString(folded);
    }

    private static int integer(Expression value, FoldContext foldContext, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        Object folded = value.fold(foldContext);
        if (folded instanceof Number number) {
            int intValue = number.intValue();
            if (intValue < 0) {
                throw new IllegalArgumentException("HIGHLIGHT option must be >= 0 but got [" + folded + "]");
            }
            return intValue;
        }
        throw new IllegalArgumentException("Expected a numeric HIGHLIGHT option but got [" + folded + "]");
    }
}
