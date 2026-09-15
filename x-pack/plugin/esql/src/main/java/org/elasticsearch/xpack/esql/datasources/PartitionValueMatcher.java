/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;

import java.util.ArrayList;
import java.util.List;

/**
 * The one comparison of a partition value against a filter literal, shared by the two layers that prune by it:
 * {@link FileSplitProvider} (files) and the listing walk in {@code GlobExpander} (folders). A folder skipped at
 * listing time is unrecoverable downstream, so where the layers cannot be proven to agree, the walk must not
 * prune. Sharing this code covers the comparator; the LITERAL is the remaining gap — the analyzer implicitly
 * casts string literals for some column types (boolean among them), so the read layer may compare a cast value
 * where a pre-resolution hint still holds the raw string. Hence the kind guard: a hint whose literal's kind
 * (number/boolean/text) differs from the typed folder value's is undecidable, never an exclusion.
 * {@link #matchesFolders} is the listing entry point; evaluation is three-valued, and "cannot decide" (a NULL
 * partition, a kind mismatch) always means keep.
 */
public final class PartitionValueMatcher {

    private PartitionValueMatcher() {}

    /**
     * Which folders of one {@code key=value} listing level a set of hints keeps, by typed value ({@code values} are
     * the decoded folder values of one key across the level, {@code null} for the NULL partition).
     *
     * <p>The read layer types a column over the FINAL glob-matched files, which the walk cannot know yet: a sibling
     * like {@code month=abc/} holding only non-matching files widens the level's type to keyword while contributing
     * nothing to the final list. So a folder is dropped only when a hint excludes it under BOTH the level-wide
     * typing and the value's own typing in isolation — {@code month == 6} keeps {@code month=06} (integer 6
     * matches) yet prunes {@code month=abc} (excluded either way). Keeping too much is always safe.
     */
    public static boolean[] matchesFolders(List<String> values, List<PartitionFilterHint> hints) {
        DataType levelType = HivePartitionDetector.inferType(values);
        boolean[] keep = new boolean[values.size()];
        for (int i = 0; i < values.size(); i++) {
            String raw = values.get(i);
            keep[i] = true;
            if (raw == null) {
                continue; // a NULL partition compares unknown against every hint — never pruned
            }
            Object levelTyped = HivePartitionDetector.castValue(raw, levelType);
            DataType aloneType = HivePartitionDetector.inferType(List.of(raw));
            Object aloneTyped = aloneType == levelType ? levelTyped : HivePartitionDetector.castValue(raw, aloneType);
            for (PartitionFilterHint hint : hints) {
                if (excludes(levelTyped, hint) && (aloneType == levelType || excludes(aloneTyped, hint))) {
                    keep[i] = false;
                    break;
                }
            }
        }
        return keep;
    }

    /** Whether {@code hint} definitively excludes this typed value; unknown is not exclusion. */
    private static boolean excludes(Object typed, PartitionFilterHint hint) {
        Boolean matches = matches(typed, hint);
        return matches != null && matches == false;
    }

    /**
     * Whether a typed partition value satisfies one hint, three-valued: {@code null} (a NULL partition value, a
     * malformed hint, or a literal whose kind differs from the value's) means the caller must not prune. The kind
     * guard is what keeps a raw string hint from disagreeing with the read layer's implicitly-cast literal:
     * {@code WHERE flag IN ("True", "false")} reaches the walk as text against a boolean-typed folder value, and
     * text-vs-boolean must be "cannot decide", not {@code "true".equals("True")}. Mirrors
     * {@link FileSplitProvider#evaluateFilter} for the kinds it can decide.
     */
    @Nullable
    static Boolean matches(@Nullable Object partitionValue, PartitionFilterHint hint) {
        if (partitionValue == null || hint.values().isEmpty()) {
            return null;
        }
        Kind valueKind = kindOf(partitionValue);
        if (valueKind == Kind.OTHER) {
            return null;
        }
        if (hint.operator() == PartitionFilterHintExtractor.Operator.IN) {
            boolean undecidable = false;
            for (Object candidate : hint.values()) {
                if (candidate == null || kindOf(candidate) != valueKind) {
                    undecidable = true;
                    continue;
                }
                if (compareEquals(partitionValue, candidate)) {
                    return true;
                }
            }
            return undecidable ? null : false;
        }
        Object literal = hint.values().get(0);
        if (literal == null || kindOf(literal) != valueKind) {
            return null;
        }
        return switch (hint.operator()) {
            case EQUALS -> compareEquals(partitionValue, literal);
            case NOT_EQUALS -> compareEquals(partitionValue, literal) == false;
            case GREATER_THAN -> compareValues(partitionValue, literal) > 0;
            case GREATER_THAN_OR_EQUAL -> compareValues(partitionValue, literal) >= 0;
            case LESS_THAN -> compareValues(partitionValue, literal) < 0;
            case LESS_THAN_OR_EQUAL -> compareValues(partitionValue, literal) <= 0;
            case IN -> null; // handled above
        };
    }

    /** The comparison classes a folder value or hint literal can belong to; only equal kinds are comparable. */
    private enum Kind {
        NUMBER,
        BOOLEAN,
        TEXT,
        OTHER
    }

    private static Kind kindOf(Object value) {
        if (value instanceof Number) {
            return Kind.NUMBER;
        }
        if (value instanceof Boolean) {
            return Kind.BOOLEAN;
        }
        if (value instanceof String || value instanceof BytesRef) {
            return Kind.TEXT;
        }
        return Kind.OTHER;
    }

    /**
     * The partition key a {@code key=value} folder name binds — {@code null} when not partition-shaped. Shares
     * {@link HivePartitionDetector#segmentKey}'s grammar, on which the walk's soundness depends. Reserved keys
     * surface under their renamed form ({@link ReservedPartitionNames}), the name a query's filter refers to.
     */
    @Nullable
    public static String folderKey(String folderName) {
        String key = HivePartitionDetector.segmentKey(folderName);
        return key == null ? null : ReservedPartitionNames.surface(key);
    }

    /**
     * The decoded value of a {@code key=value} folder name — {@code null} for the NULL-partition sentinel. Only
     * meaningful when {@link #folderKey} accepted the name.
     */
    @Nullable
    public static String folderValue(String folderName) {
        return HivePartitionDetector.segmentValue(folderName);
    }

    /** The hints naming {@code column}. */
    public static List<PartitionFilterHint> hintsFor(String column, List<PartitionFilterHint> hints) {
        List<PartitionFilterHint> matching = new ArrayList<>();
        for (PartitionFilterHint hint : hints) {
            if (hint.columnName().equals(column)) {
                matching.add(hint);
            }
        }
        return matching;
    }

    /**
     * String form of a partition value or filter literal. Keyword partition values arrive as Java {@code String}
     * (from {@code HivePartitionDetector.castValue}) while an ES|QL keyword literal is a Lucene {@code BytesRef}
     * whose {@code toString()} is a hex dump — so a raw {@code toString()} comparison of the two never matches.
     * {@link BytesRefs#toString(Object)} UTF8-decodes a {@code BytesRef} and falls back to {@code toString()}
     * otherwise, so both sides normalize to the same text before any string compare or numeric parse.
     */
    private static String stringOf(Object value) {
        return BytesRefs.toString(value);
    }

    static boolean compareEquals(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return compareNumbers(na, nb) == 0;
        }
        return stringOf(a).equals(stringOf(b));
    }

    static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null partition values");
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return compareNumbers(na, nb);
        }
        // Coerce mixed Number/text cases: a partition value may be stored as "2024" (String) while the literal from
        // the filter is Integer 2024, or vice versa. Only when exactly one side is already a Number — two text values
        // are compared as text, so a KEYWORD partition never has "0123" and "123" collapse into the same value.
        if (a instanceof Number na) {
            Number nb = parseNumber(stringOf(b));
            return nb != null ? compareNumbers(na, nb) : keywordCompare(a, b);
        }
        if (b instanceof Number nb) {
            Number na = parseNumber(stringOf(a));
            return na != null ? compareNumbers(na, nb) : keywordCompare(a, b);
        }
        return keywordCompare(a, b);
    }

    /**
     * Orders two numeric values. Integral types are compared as {@code long}, never as {@code double}: above
     * 2^53 a {@code double} cannot separate adjacent longs, so an epoch-micros or snowflake-id partition value
     * would compare <em>equal</em> to its neighbour. That is not a rounding nit — it makes the matcher return a
     * confident {@code false} for {@code ts != <adjacent>} and prune a file whose every row matches the filter.
     */
    private static int compareNumbers(Number a, Number b) {
        if (isIntegral(a) && isIntegral(b)) {
            return Long.compare(a.longValue(), b.longValue());
        }
        return Double.compare(a.doubleValue(), b.doubleValue());
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    /** The text parsed as a number, or {@code null} if it is not numeric. */
    private static Number parseNumber(String text) {
        try {
            return Long.valueOf(text);
        } catch (NumberFormatException notALong) {
            try {
                return Double.valueOf(text);
            } catch (NumberFormatException notANumber) {
                return null;
            }
        }
    }

    /**
     * Orders two non-numeric values the way ES|QL orders keywords: by UTF-8 bytes, which is code-point order.
     * {@link String#compareTo} would order by UTF-16 code units instead, and the two disagree whenever one side is a
     * supplementary-plane character (a folder named {@code region=<emoji>}) and the other sits in {@code U+E000..U+FFFF}
     * — the surrogate compares low, the engine compares it high, and a range predicate would prune a matching file.
     */
    private static int keywordCompare(Object a, Object b) {
        return new BytesRef(stringOf(a)).compareTo(new BytesRef(stringOf(b)));
    }
}
