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
 * The per-option type, range and enum rules enforced while building this record are the single source of truth for
 * validation: {@link Highlight#postAnalysisVerification} reuses {@link #validate} so that invalid option values fail at
 * analysis time on the same path as the enum checks, instead of only later during local planning.
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
     * Validation policy for a string-valued enum option: its allowed values and whether matching is case-insensitive.
     * Shared by analyzer verification ({@link Highlight#postAnalysisVerification}) and parsing ({@link #from}) so the two
     * paths cannot drift on either the allowed set or the case-sensitivity contract.
     */
    // TODO: this EnumOption descriptor (and the validate(...) bridge that catches IllegalArgumentException) is
    // HIGHLIGHT-specific. It's fine as-is, but if we ever want tighter consistency with the rest of ES|QL the enum/type
    // checks could move toward the shared Options-style descriptor that the full-text functions use.
    public record EnumOption(String name, List<String> allowed, boolean caseInsensitive) {
        /** Normalizes a raw value for comparison/storage: lower-cased when case-insensitive, otherwise unchanged. */
        public String normalize(String raw) {
            return caseInsensitive ? raw.toLowerCase(Locale.ROOT) : raw;
        }

        public boolean isValid(String raw) {
            return allowed.contains(normalize(raw));
        }
    }

    // encoder is case-sensitive to mirror Query DSL (default/html only); boundary_scanner and order are normalized
    // case-insensitively.
    public static final EnumOption ENCODER_OPTION = new EnumOption(Highlight.ENCODER, ALLOWED_ENCODERS, false);
    public static final EnumOption BOUNDARY_SCANNER_OPTION = new EnumOption(Highlight.BOUNDARY_SCANNER, ALLOWED_BOUNDARY_SCANNERS, true);
    public static final EnumOption ORDER_OPTION = new EnumOption(Highlight.ORDER, ALLOWED_ORDERS, true);

    public static HighlightOptions from(MapExpression options, FoldContext foldContext) {
        if (options == null) {
            return defaults();
        }
        return new HighlightOptions(
            string(options.get(Highlight.PRE_TAGS), foldContext, DEFAULT_PRE_TAG),
            string(options.get(Highlight.POST_TAGS), foldContext, DEFAULT_POST_TAG),
            ENCODER_OPTION.normalize(string(options.get(Highlight.ENCODER), foldContext, DEFAULT_ENCODER)),
            integer(options.get(Highlight.NUMBER_OF_FRAGMENTS), foldContext, DEFAULT_NUMBER_OF_FRAGMENTS),
            integer(options.get(Highlight.FRAGMENT_SIZE), foldContext, DEFAULT_FRAGMENT_SIZE),
            integer(options.get(Highlight.NO_MATCH_SIZE), foldContext, DEFAULT_NO_MATCH_SIZE),
            BOUNDARY_SCANNER_OPTION.normalize(string(options.get(Highlight.BOUNDARY_SCANNER), foldContext, DEFAULT_BOUNDARY_SCANNER)),
            locale(options.get(Highlight.BOUNDARY_SCANNER_LOCALE), foldContext),
            ORDER_OPTION.normalize(string(options.get(Highlight.ORDER), foldContext, DEFAULT_ORDER)),
            maxAnalyzedOffset(options.get(Highlight.MAX_ANALYZED_OFFSET), foldContext)
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
            DEFAULT_MAX_ANALYZED_OFFSET
        );
    }

    /**
     * Type/range-checks a single (non-null, foldable) option value by parsing it the same way {@link #from} would and
     * discarding the result, throwing {@link IllegalArgumentException} on a bad value. Enum options are checked
     * separately by the verifier against their {@link EnumOption} descriptor (so it keeps the established
     * {@code Invalid [..] value [..]} message), so they - along with unknown names (validated in the parser) - are
     * no-ops here.
     * <p>
     * {@code boundary_chars} and {@code boundary_max_scan} are FastVectorHighlighter-only no-ops at execution, but we
     * still check their value types here for Query DSL parity.
     */
    public static void validate(String name, Expression value, FoldContext foldContext) {
        switch (name) {
            case Highlight.PRE_TAGS, Highlight.POST_TAGS -> string(value, foldContext, null);
            case Highlight.BOUNDARY_CHARS -> requireString(value.fold(foldContext));
            case Highlight.BOUNDARY_SCANNER_LOCALE -> locale(value, foldContext);
            case Highlight.NUMBER_OF_FRAGMENTS, Highlight.FRAGMENT_SIZE, Highlight.NO_MATCH_SIZE, Highlight.BOUNDARY_MAX_SCAN -> integer(
                value,
                foldContext,
                0
            );
            case Highlight.MAX_ANALYZED_OFFSET -> maxAnalyzedOffset(value, foldContext);
            case Highlight.PHRASE_LIMIT -> {
                // Grammar-only no-op: accepted for Query DSL parity, intentionally not parsed or value-validated.
            }
            case Highlight.ENCODER, Highlight.BOUNDARY_SCANNER, Highlight.ORDER -> {
                // Verified separately against the EnumOption descriptor.
            }
            default -> {
                // Unknown name; the parser already rejected anything not in VALID_OPTION_NAMES.
            }
        }
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
            return list.isEmpty() ? defaultValue : requireString(list.getFirst());
        }
        return requireString(folded);
    }

    /**
     * Coerces a folded value to a string only when it actually is one. Numbers, booleans and other types are rejected
     * rather than silently stringified via {@link BytesRefs#toString} (e.g. {@code pre_tags: 123}).
     */
    private static String requireString(Object folded) {
        if (folded instanceof BytesRef || folded instanceof String) {
            return BytesRefs.toString(folded);
        }
        throw new IllegalArgumentException("Expected a string HIGHLIGHT option but got [" + folded + "]");
    }

    /**
     * Parses {@code boundary_scanner_locale} as a language tag (for example {@code en-US}). Malformed tags are rejected
     * early instead of silently degrading to {@link Locale#ROOT} via {@link LocaleUtils#parseLanguageTag(String)}, which
     * normalizes the JDK {@link java.util.IllformedLocaleException} into a stable {@link IllegalArgumentException} so the
     * failure surfaces on the same {@code HIGHLIGHT} validation path as the other options.
     */
    private static Locale locale(Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_BOUNDARY_SCANNER_LOCALE;
        }
        return LocaleUtils.parseLanguageTag(requireString(value.fold(foldContext)));
    }

    private static int integer(Expression value, FoldContext foldContext, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        int intValue = integral(value.fold(foldContext));
        if (intValue < 0) {
            throw new IllegalArgumentException("HIGHLIGHT option must be >= 0 but got [" + intValue + "]");
        }
        return intValue;
    }

    /**
     * {@code max_analyzed_offset} accepts the same values as Query DSL: a positive integer, or {@code -1} to fall back
     * to the index setting. {@code 0} and any value below {@code -1} are rejected (see
     * {@code AbstractHighlighterBuilder#maxAnalyzedOffset}).
     */
    private static int maxAnalyzedOffset(Expression value, FoldContext foldContext) {
        if (value == null) {
            return DEFAULT_MAX_ANALYZED_OFFSET;
        }
        int intValue = integral(value.fold(foldContext));
        if (intValue < -1 || intValue == 0) {
            throw new IllegalArgumentException("[max_analyzed_offset] must be a positive integer, or -1 but got [" + intValue + "]");
        }
        return intValue;
    }

    /**
     * Extracts an integral value from a folded numeric option, rejecting non-numbers as well as numbers with a fractional
     * part (e.g. {@code number_of_fragments: 0.9}) instead of silently truncating them via {@link Number#intValue()}.
     */
    private static int integral(Object folded) {
        if (folded instanceof Number number) {
            if (number instanceof Float || number instanceof Double) {
                double doubleValue = number.doubleValue();
                if (Double.isFinite(doubleValue) == false || doubleValue != Math.floor(doubleValue)) {
                    throw new IllegalArgumentException("Expected an integer HIGHLIGHT option but got [" + folded + "]");
                }
            }
            return number.intValue();
        }
        throw new IllegalArgumentException("Expected a numeric HIGHLIGHT option but got [" + folded + "]");
    }
}
