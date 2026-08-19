/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.LocaleUtils;
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
 * only honoured by the FastVectorHighlighter. HIGHLIGHT always uses the unified highlighter, so they are grammar-only
 * no-ops that never reach this record.
 * <p>
 * The validation done while building this record is also the single source of truth for analysis-time checks:
 * {@link Highlight#postAnalysisVerification} reuses {@link #validate} so invalid values fail during analysis rather
 * than only later during local planning.
 */
public record HighlightOptions(
    String preTag,
    String postTag,
    String encoder,
    String analyzerName,
    int numberOfFragments,
    int fragmentSize,
    int noMatchSize,
    String boundaryScanner,
    Locale boundaryScannerLocale,
    String order,
    int maxAnalyzedOffset
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
    // -1 means "use the index setting"; the current coordinator-side operator uses the default index value.
    public static final int DEFAULT_MAX_ANALYZED_OFFSET = -1;

    /**
     * A string-valued enum option together with its allowed values and case-sensitivity. Shared by
     * {@link Highlight#postAnalysisVerification} and {@link #from} so the two paths can never disagree on the allowed
     * set or whether matching is case-insensitive.
     */
    public record EnumOption(String name, List<String> allowed, boolean caseInsensitive) {
        public String normalize(String raw) {
            return caseInsensitive ? raw.toLowerCase(Locale.ROOT) : raw;
        }

        public boolean isValid(String raw) {
            return allowed.contains(normalize(raw));
        }
    }

    // encoder is case-sensitive to mirror Query DSL (default/html only); boundary_scanner and order are case-insensitive.
    public static final EnumOption ENCODER_OPTION = new EnumOption(Highlight.ENCODER, ALLOWED_ENCODERS, false);
    public static final EnumOption BOUNDARY_SCANNER_OPTION = new EnumOption(Highlight.BOUNDARY_SCANNER, ALLOWED_BOUNDARY_SCANNERS, true);
    public static final EnumOption ORDER_OPTION = new EnumOption(Highlight.ORDER, ALLOWED_ORDERS, true);

    public static HighlightOptions from(MapExpression options, FoldContext foldContext) {
        if (options == null) {
            return defaults();
        }
        return new HighlightOptions(
            string(Highlight.PRE_TAGS, options.get(Highlight.PRE_TAGS), foldContext, DEFAULT_PRE_TAG),
            string(Highlight.POST_TAGS, options.get(Highlight.POST_TAGS), foldContext, DEFAULT_POST_TAG),
            ENCODER_OPTION.normalize(string(Highlight.ENCODER, options.get(Highlight.ENCODER), foldContext, DEFAULT_ENCODER)),
            analyzerName(Highlight.ANALYZER, options.get(Highlight.ANALYZER), foldContext),
            integer(Highlight.NUMBER_OF_FRAGMENTS, options.get(Highlight.NUMBER_OF_FRAGMENTS), foldContext, DEFAULT_NUMBER_OF_FRAGMENTS),
            integer(Highlight.FRAGMENT_SIZE, options.get(Highlight.FRAGMENT_SIZE), foldContext, DEFAULT_FRAGMENT_SIZE),
            integer(Highlight.NO_MATCH_SIZE, options.get(Highlight.NO_MATCH_SIZE), foldContext, DEFAULT_NO_MATCH_SIZE),
            BOUNDARY_SCANNER_OPTION.normalize(
                string(Highlight.BOUNDARY_SCANNER, options.get(Highlight.BOUNDARY_SCANNER), foldContext, DEFAULT_BOUNDARY_SCANNER)
            ),
            locale(Highlight.BOUNDARY_SCANNER_LOCALE, options.get(Highlight.BOUNDARY_SCANNER_LOCALE), foldContext),
            ORDER_OPTION.normalize(string(Highlight.ORDER, options.get(Highlight.ORDER), foldContext, DEFAULT_ORDER)),
            maxAnalyzedOffset(Highlight.MAX_ANALYZED_OFFSET, options.get(Highlight.MAX_ANALYZED_OFFSET), foldContext)
        );
    }

    private static HighlightOptions defaults() {
        return new HighlightOptions(
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            DEFAULT_ENCODER,
            null /* analyzerName */,
            DEFAULT_NUMBER_OF_FRAGMENTS,
            DEFAULT_FRAGMENT_SIZE,
            DEFAULT_NO_MATCH_SIZE,
            DEFAULT_BOUNDARY_SCANNER,
            DEFAULT_BOUNDARY_SCANNER_LOCALE,
            DEFAULT_ORDER,
            DEFAULT_MAX_ANALYZED_OFFSET
        );
    }

    /**
     * Type/range-checks a single (non-null, foldable) option value by parsing it exactly as {@link #from} would and
     * discarding the result, throwing {@link IllegalArgumentException} on a bad value. Enum options ({@code encoder},
     * {@code boundary_scanner}, {@code order}) are checked separately against their {@link EnumOption} descriptor, and
     * {@code phrase_limit} is grammar-only, so all of them are no-ops here. {@code boundary_chars} and
     * {@code boundary_max_scan} are FastVectorHighlighter-only at execution but we still type-check them for Query DSL
     * parity.
     */
    public static void validate(String name, Expression value, FoldContext foldContext) {
        switch (name) {
            case Highlight.PRE_TAGS, Highlight.POST_TAGS -> string(name, value, foldContext, null);
            case Highlight.ANALYZER -> analyzerName(name, value, foldContext);
            case Highlight.BOUNDARY_CHARS -> requireString(name, value.fold(foldContext));
            case Highlight.BOUNDARY_SCANNER_LOCALE -> locale(name, value, foldContext);
            case Highlight.NUMBER_OF_FRAGMENTS, Highlight.FRAGMENT_SIZE, Highlight.NO_MATCH_SIZE, Highlight.BOUNDARY_MAX_SCAN -> integer(
                name,
                value,
                foldContext,
                0
            );
            case Highlight.MAX_ANALYZED_OFFSET -> maxAnalyzedOffset(name, value, foldContext);
            case Highlight.ENCODER, Highlight.BOUNDARY_SCANNER, Highlight.ORDER, Highlight.PHRASE_LIMIT -> {
                // Handled elsewhere (enums against EnumOption, phrase_limit is grammar-only).
            }
            // Unreachable: the parser already rejected anything not in VALID_OPTION_NAMES.
            default -> throw new AssertionError("Unexpected option [" + name + "] in HIGHLIGHT");
        }
    }

    /**
     * Reads a string option. A list form is accepted only when it contains at most one value.
     */
    // TODO: support multiple pre_tags/post_tags (Query DSL rotates through the list per match) instead of rejecting them.
    private static String string(String name, Expression value, FoldContext foldContext, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        Object folded = value.fold(foldContext);
        if (folded instanceof List<?> list) {
            if (list.size() > 1) {
                throw new IllegalArgumentException(
                    "Option [" + name + "] expects a single string value, found [" + list.size() + "] values"
                );
            }
            return list.isEmpty() ? defaultValue : requireString(name, list.getFirst());
        }
        return requireString(name, folded);
    }

    /** Reads the {@code analyzer} name without resolving it. */
    static String analyzerName(String name, Expression value, FoldContext foldContext) {
        if (value == null) {
            return null;
        }
        return requireString(name, value.fold(foldContext));
    }

    /**
     * Coerces a folded value to a string only when it actually is one, rejecting numbers, booleans and other types
     * rather than silently stringifying them (e.g. {@code pre_tags: 123}).
     */
    private static String requireString(String name, Object folded) {
        if (folded instanceof BytesRef || folded instanceof String) {
            return BytesRefs.toString(folded);
        }
        throw new IllegalArgumentException("Option [" + name + "] must be a string, found [" + folded + "]");
    }

    /**
     * Parses {@code boundary_scanner_locale} as a language tag (for example {@code en-US}). Uses the strict
     * {@link LocaleUtils#parseLanguageTag(String)} so a malformed tag fails here instead of silently degrading to
     * {@link Locale#ROOT}.
     */
    private static Locale locale(String name, Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_BOUNDARY_SCANNER_LOCALE;
        }
        String languageTag = requireString(name, value.fold(foldContext));
        try {
            return LocaleUtils.parseLanguageTag(languageTag);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Option [" + name + "] has invalid language tag: " + e.getMessage(),
                e.getCause() != null ? e.getCause() : e
            );
        }
    }

    private static int integer(String name, Expression value, FoldContext foldContext, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        int intValue = integral(name, value.fold(foldContext));
        if (intValue < 0) {
            throw new IllegalArgumentException("Option [" + name + "] must be >= 0, found [" + intValue + "]");
        }
        return intValue;
    }

    /**
     * {@code max_analyzed_offset} accepts the same values as Query DSL: a positive integer, or {@code -1} to fall back
     * to the index setting. {@code 0} and anything below {@code -1} are rejected (see
     * {@code AbstractHighlighterBuilder#maxAnalyzedOffset}).
     */
    private static int maxAnalyzedOffset(String name, Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_MAX_ANALYZED_OFFSET;
        }
        int intValue = integral(name, value.fold(foldContext));
        if (intValue < -1 || intValue == 0) {
            throw new IllegalArgumentException("Option [" + name + "] must be a positive integer, or -1, found [" + intValue + "]");
        }
        return intValue;
    }

    /**
     * Extracts an int from a folded numeric option, rejecting non-numbers and fractional values (e.g. {@code 0.9})
     * rather than silently truncating them.
     */
    private static int integral(String name, Object folded) {
        if (folded instanceof Number number) {
            if (number instanceof Float || number instanceof Double) {
                double doubleValue = number.doubleValue();
                if (Double.isFinite(doubleValue) == false || doubleValue != Math.floor(doubleValue)) {
                    throw new IllegalArgumentException("Option [" + name + "] must be an integer, found [" + folded + "]");
                }
            }
            return number.intValue();
        }
        throw new IllegalArgumentException("Option [" + name + "] must be numeric, found [" + folded + "]");
    }
}
