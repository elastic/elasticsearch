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
import java.util.Locale;

/**
 * Resolved, plain-Java view of the {@code WITH { ... }} options of a {@link Highlight} command, with defaults applied.
 * Built once at local-execution-planning time from the (foldable) {@link MapExpression} so the operator factory only
 * deals with primitives.
 * <p>
 * {@code boundary_chars}, {@code boundary_max_scan} and {@code phrase_limit} are accepted for Query DSL parity but are
 * only honoured by the FastVectorHighlighter. HIGHLIGHT always uses the unified highlighter, so they are no-ops here.
 */
public record HighlightOptions(
    String preTag,
    String postTag,
    String encoder,
    int numberOfFragments,
    int fragmentSize,
    int noMatchSize,
    String boundaryScanner,
    Locale boundaryScannerLocale,
    String order,
    int maxAnalyzedOffset,
    int phraseLimit
) {

    public static final String DEFAULT_PRE_TAG = "<em>";
    public static final String DEFAULT_POST_TAG = "</em>";
    public static final String DEFAULT_ENCODER = "default";
    public static final String HTML_ENCODER = "html";
    public static final int DEFAULT_NUMBER_OF_FRAGMENTS = 5;
    public static final int DEFAULT_FRAGMENT_SIZE = 100;
    public static final int DEFAULT_NO_MATCH_SIZE = 0;

    public static final String BOUNDARY_SCANNER_SENTENCE = "sentence";
    public static final String BOUNDARY_SCANNER_WORD = "word";
    public static final String ORDER_NONE = "none";
    public static final String ORDER_SCORE = "score";

    public static final String DEFAULT_BOUNDARY_SCANNER = BOUNDARY_SCANNER_SENTENCE;
    public static final Locale DEFAULT_BOUNDARY_SCANNER_LOCALE = Locale.ROOT;
    public static final String DEFAULT_ORDER = ORDER_NONE;

    public static final List<String> ALLOWED_ENCODERS = List.of(DEFAULT_ENCODER, HTML_ENCODER);
    public static final List<String> ALLOWED_BOUNDARY_SCANNERS = List.of(BOUNDARY_SCANNER_SENTENCE, BOUNDARY_SCANNER_WORD);
    public static final List<String> ALLOWED_ORDERS = List.of(ORDER_NONE, ORDER_SCORE);
    // -1 means "use the index setting", matching Query DSL.
    public static final int DEFAULT_MAX_ANALYZED_OFFSET = -1;
    public static final int DEFAULT_PHRASE_LIMIT = 256;

    public static HighlightOptions from(MapExpression options, FoldContext foldContext) {
        if (options == null) {
            return defaults();
        }
        return new HighlightOptions(
            string(options.get(Highlight.PRE_TAGS), foldContext, DEFAULT_PRE_TAG),
            string(options.get(Highlight.POST_TAGS), foldContext, DEFAULT_POST_TAG),
            string(options.get(Highlight.ENCODER), foldContext, DEFAULT_ENCODER),
            integer(options.get(Highlight.NUMBER_OF_FRAGMENTS), foldContext, DEFAULT_NUMBER_OF_FRAGMENTS),
            integer(options.get(Highlight.FRAGMENT_SIZE), foldContext, DEFAULT_FRAGMENT_SIZE),
            integer(options.get(Highlight.NO_MATCH_SIZE), foldContext, DEFAULT_NO_MATCH_SIZE),
            string(options.get(Highlight.BOUNDARY_SCANNER), foldContext, DEFAULT_BOUNDARY_SCANNER),
            locale(options.get(Highlight.BOUNDARY_SCANNER_LOCALE), foldContext),
            string(options.get(Highlight.ORDER), foldContext, DEFAULT_ORDER),
            maxAnalyzedOffset(options.get(Highlight.MAX_ANALYZED_OFFSET), foldContext),
            integer(options.get(Highlight.PHRASE_LIMIT), foldContext, DEFAULT_PHRASE_LIMIT)
        );
    }

    private static HighlightOptions defaults() {
        return new HighlightOptions(
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            DEFAULT_ENCODER,
            DEFAULT_NUMBER_OF_FRAGMENTS,
            DEFAULT_FRAGMENT_SIZE,
            DEFAULT_NO_MATCH_SIZE,
            DEFAULT_BOUNDARY_SCANNER,
            DEFAULT_BOUNDARY_SCANNER_LOCALE,
            DEFAULT_ORDER,
            DEFAULT_MAX_ANALYZED_OFFSET,
            DEFAULT_PHRASE_LIMIT
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

    private static Locale locale(Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_BOUNDARY_SCANNER_LOCALE;
        }
        return Locale.forLanguageTag(BytesRefs.toString(value.fold(foldContext)));
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

    /**
     * {@code max_analyzed_offset} matches Query DSL semantics: a positive integer, or {@code -1} to fall back to the index
     * setting. {@code 0} and any value below {@code -1} are rejected (see {@code AbstractHighlighterBuilder#maxAnalyzedOffset}).
     */
    private static int maxAnalyzedOffset(Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_MAX_ANALYZED_OFFSET;
        }
        Object folded = value.fold(foldContext);
        if (folded instanceof Number number) {
            int intValue = number.intValue();
            if (intValue < -1 || intValue == 0) {
                throw new IllegalArgumentException("[max_analyzed_offset] must be a positive integer, or -1 but got [" + folded + "]");
            }
            return intValue;
        }
        throw new IllegalArgumentException("Expected a numeric HIGHLIGHT option but got [" + folded + "]");
    }
}
