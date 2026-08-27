/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.spi.TemporalInference;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Infers column types from CSV data by sampling rows when headers lack explicit type annotations.
 * <p>
 * Each column starts at the most specific candidate type and widens on the first value that
 * doesn't fit. Type candidates from most specific to least:
 * <ol>
 *   <li>{@code BOOLEAN} — only {@code true}/{@code false} (case-insensitive)</li>
 *   <li>{@code INTEGER} — fits in {@code int}</li>
 *   <li>{@code LONG} — fits in {@code long}</li>
 *   <li>{@code DOUBLE} — any floating-point number</li>
 *   <li>{@code DATETIME} — ISO-8601, date-only, zone-less timestamps</li>
 *   <li>{@code DATE_NANOS} — timestamps carrying sub-millisecond digits, which {@code DATETIME}
 *       would silently truncate (see {@link TemporalInference})</li>
 *   <li>{@code KEYWORD} — universal fallback (everything is a string)</li>
 * </ol>
 * Null and empty values are compatible with every type. Columns with only null/empty values
 * default to KEYWORD. When a value doesn't fit the current candidate, the column widens to the
 * next candidate. Boolean and temporal columns that were confirmed by at least one value skip
 * directly to KEYWORD on mismatch (since a column with both "true" and "42" is most likely a
 * string column, not numeric) — with one exception: a confirmed DATETIME column that meets a
 * nanosecond timestamp steps to DATE_NANOS rather than collapsing to KEYWORD, because the two are
 * the same kind of thing and the column is simply more precise than its first value suggested.
 * Without that exception a mixed-precision column's type would depend on which row came first.
 * <p>
 * For files smaller than the sample size, all rows are used. The inference runs in a single
 * sequential pass over the sample.
 */
public class CsvSchemaInferrer {

    static final int DEFAULT_SAMPLE_SIZE = 20_000;

    private static final DataType[] TYPE_CANDIDATES = {
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.KEYWORD };

    /** A value is not a timestamp at all. */
    private static final int NOT_TEMPORAL = 0;
    /** A timestamp the column reads as {@code datetime}. */
    private static final int TEMPORAL_DATETIME = 1;
    /**
     * A timestamp that {@code datetime} cannot read without dropping digits, and that {@code date_nanos}
     * can both represent and decode &mdash; so it moves the column.
     */
    private static final int TEMPORAL_NANOS_FORCED = 2;

    private CsvSchemaInferrer() {}

    /**
     * Widens an already-inferred schema against additional rows that were not part of the initial
     * sample. Uses the same {@link #narrowCandidate} logic as {@link #inferSchema}, starting from
     * the already-confirmed candidate types (every column is treated as confirmed, since the initial
     * sample already committed to its type). Any column whose inferred type cannot parse a value in
     * {@code additionalRows} is advanced through {@link #TYPE_CANDIDATES} toward KEYWORD.
     * <p>
     * Returns the same {@code schema} reference when no widening is needed (including when
     * {@code additionalRows} is empty), and a new list otherwise.
     *
     * @param schema         the schema returned by a prior {@link #inferSchema} call
     * @param additionalRows rows that were not included in the initial sample
     * @param datetimeFormatter the same formatter used for the initial inference
     */
    static List<Attribute> widenSchema(List<Attribute> schema, List<String[]> additionalRows, @Nullable DateFormatter datetimeFormatter) {
        if (additionalRows.isEmpty()) {
            return schema;
        }
        int numCols = schema.size();
        int[] candidateIdx = new int[numCols];
        for (int col = 0; col < numCols; col++) {
            DataType type = schema.get(col).dataType();
            candidateIdx[col] = TYPE_CANDIDATES.length - 1; // default: KEYWORD
            for (int i = 0; i < TYPE_CANDIDATES.length - 1; i++) {
                if (TYPE_CANDIDATES[i] == type) {
                    candidateIdx[col] = i;
                    break;
                }
            }
        }
        int nonKeywordCount = 0;
        for (int col = 0; col < numCols; col++) {
            if (candidateIdx[col] < TYPE_CANDIDATES.length - 1) {
                nonKeywordCount++;
            }
        }
        boolean anyWidened = false;
        outer: for (String[] row : additionalRows) {
            for (int col = 0; col < numCols; col++) {
                if (candidateIdx[col] >= TYPE_CANDIDATES.length - 1) {
                    continue;
                }
                String value = col < row.length ? row[col] : null;
                if (value != null) {
                    value = value.trim();
                }
                if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                    continue;
                }
                // All columns are confirmed by the initial sample (confirmed=true).
                int newIdx = narrowCandidate(candidateIdx[col], true, value, datetimeFormatter);
                if (newIdx != candidateIdx[col]) {
                    candidateIdx[col] = newIdx;
                    anyWidened = true;
                    if (newIdx >= TYPE_CANDIDATES.length - 1) {
                        nonKeywordCount--;
                    }
                }
            }
            if (nonKeywordCount == 0) {
                break outer;
            }
        }
        if (anyWidened == false) {
            return schema;
        }
        List<Attribute> widened = new ArrayList<>(numCols);
        for (int col = 0; col < numCols; col++) {
            Attribute original = schema.get(col);
            DataType newType = TYPE_CANDIDATES[candidateIdx[col]];
            if (newType != original.dataType()) {
                widened.add(new ReferenceAttribute(Source.EMPTY, null, original.name(), newType, Nullability.TRUE, null, false));
            } else {
                widened.add(original);
            }
        }
        return widened;
    }

    /**
     * Infers schema from column names and sample data rows.
     *
     * @param columnNames       header names (plain, without type annotations)
     * @param sampleRows        sample data rows; each row is a string array of cell values
     * @param datetimeFormatter the file-level {@code datetime_format} parser, or null for ISO-8601. A column is
     *                          inferred {@code DATETIME} only when this parser accepts its values, so inference sees
     *                          the same dialect the reader will later parse with. Numeric candidates are tried first
     *                          (see {@link #TYPE_CANDIDATES}), so an all-digit column stays numeric regardless.
     * @return list of attributes with inferred types
     */
    static List<Attribute> inferSchema(String[] columnNames, List<String[]> sampleRows, @Nullable DateFormatter datetimeFormatter) {
        int numCols = columnNames.length;
        int[] candidateIdx = new int[numCols];
        // Whether the column's current candidate type was confirmed by at least one matching value
        boolean[] typeConfirmed = new boolean[numCols];
        // Whether the column has seen at least one non-null value
        boolean[] seenValue = new boolean[numCols];

        for (String[] row : sampleRows) {
            for (int col = 0; col < numCols; col++) {
                if (candidateIdx[col] >= TYPE_CANDIDATES.length - 1) {
                    continue;
                }
                String value = col < row.length ? row[col] : null;
                if (value != null) {
                    value = value.trim();
                }
                if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                    continue;
                }
                seenValue[col] = true;
                candidateIdx[col] = narrowCandidate(candidateIdx[col], typeConfirmed[col], value, datetimeFormatter);
                typeConfirmed[col] = true;
            }
        }

        List<Attribute> attributes = new ArrayList<>(numCols);
        for (int col = 0; col < numCols; col++) {
            String name = columnNames[col].trim();
            DataType type = seenValue[col] ? TYPE_CANDIDATES[candidateIdx[col]] : DataType.KEYWORD;
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, false));
        }
        return attributes;
    }

    /**
     * Finds the narrowest type candidate that can represent the given value, starting from the
     * current candidate index. When a column has been confirmed as BOOLEAN, DATETIME or DATE_NANOS by
     * previous values and a new value doesn't fit, skip directly to KEYWORD (since a column with
     * "true" and "42" is most likely a string column, not numeric).
     * For unconfirmed columns or numeric types, narrow one step at a time.
     * <p>
     * The one exception to the skip rule is DATETIME meeting a nanosecond timestamp: that steps to
     * DATE_NANOS. Both are timestamps, and which one the column needs is not knowable until a value
     * demands the extra precision — so collapsing to KEYWORD there would make the column's type
     * depend on row order, with millis-first landing KEYWORD and nanos-first landing DATE_NANOS.
     * <p>
     * The value is classified as temporal at most once per call, however many rungs are walked: both
     * temporal rungs read the same answer, so adding DATE_NANOS costs no additional parse.
     * <p>
     * DATE_NANOS accepts <em>any</em> timestamp, including ones outside the {@code date_nanos} window
     * that no value could ever have forced. That is deliberate: rejecting them here would make the
     * column's type depend on whether the out-of-window row came before or after the forcing one.
     * Once some value has established that the column is nanosecond-precision, an out-of-window
     * timestamp elsewhere in it is a bad cell rather than evidence the column is really a string.
     * <p>
     * Be clear about what that costs. Such a cell fails at read time under the error policy, and the
     * default policy is {@code FAIL_FAST} — so a schemaless file mixing in-window nanosecond
     * timestamps with out-of-window ones goes from a silently-truncating success to a failed query.
     * That is the same thing that happens to the same file under a declared {@code date_nanos}
     * schema today, and the value that forced the column always decodes; but it is a real change in
     * outcome, not merely a change in which cells are marked bad.
     */
    private static int narrowCandidate(int currentIdx, boolean confirmed, String value, @Nullable DateFormatter datetimeFormatter) {
        int temporal = -1; // computed on demand, at most once, and only if a temporal rung is reached
        while (currentIdx < TYPE_CANDIDATES.length - 1) {
            DataType current = TYPE_CANDIDATES[currentIdx];
            if (current == DataType.DATETIME || current == DataType.DATE_NANOS) {
                if (temporal < 0) {
                    temporal = classifyTemporal(value, datetimeFormatter);
                }
                // DATETIME holds every timestamp that does not move the column; DATE_NANOS takes any
                // timestamp at all, including out-of-window ones — see the acceptance note below.
                boolean fits = current == DataType.DATETIME ? temporal == TEMPORAL_DATETIME : temporal != NOT_TEMPORAL;
                if (fits) {
                    return currentIdx;
                }
                if (confirmed && current == DataType.DATETIME && temporal == TEMPORAL_NANOS_FORCED) {
                    currentIdx++; // the exception: widen to DATE_NANOS instead of collapsing
                    continue;
                }
            } else if (canParse(current, value)) {
                return currentIdx;
            }
            if (confirmed && (current == DataType.BOOLEAN || current == DataType.DATETIME || current == DataType.DATE_NANOS)) {
                return TYPE_CANDIDATES.length - 1;
            }
            currentIdx++;
        }
        return currentIdx;
    }

    /**
     * Whether a value fits one of the non-temporal candidate types. The temporal rungs are settled by
     * {@link #classifyTemporal} instead, which has to tell the two precisions apart rather than just
     * answering yes or no, and KEYWORD is never asked because the walk stops before it.
     */
    private static boolean canParse(DataType type, String value) {
        return switch (type) {
            case BOOLEAN -> Booleans.isBoolean(value.toLowerCase(Locale.ROOT));
            case INTEGER -> canParseInt(value);
            case LONG -> canParseLong(value);
            case DOUBLE -> canParseDouble(value);
            // Unreachable: the walk stops before KEYWORD and intercepts both temporal rungs above.
            default -> throw new AssertionError("not a non-temporal candidate type: " + type);
        };
    }

    private static boolean canParseInt(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean canParseLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean canParseDouble(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Classifies a value as not-a-timestamp, a timestamp {@code datetime} reads losslessly, or one
     * that only {@code date_nanos} reads losslessly.
     * <p>
     * Which values are <em>accepted</em> as timestamps is unchanged: the parse below is the same one
     * this method has always done, and its result — previously discarded — is what decides the
     * precision. So the extra rung costs no extra parse.
     * <p>
     * A declared {@code datetime_format} never yields DATE_NANOS. The user has said how their
     * timestamps are written, declaring the schema is the way to ask for nanoseconds, and we would
     * otherwise be routing the column onto a decode rail their pattern may not parse.
     */
    private static int classifyTemporal(String value, @Nullable DateFormatter datetimeFormatter) {
        if (datetimeFormatter != null) {
            return datetimeFormatter.tryParse(value) != null ? TEMPORAL_DATETIME : NOT_TEMPORAL;
        }
        try {
            ZonedDateTime parsed = DateUtils.asDateTime(value);
            // The whitespace-separated dialect is accepted here but rejected by the date_nanos decode
            // rail, so such a value must not be what flips a column onto that rail — it would turn a
            // readable cell into a per-cell error. Checked only for values already carrying
            // sub-millisecond digits, so ordinary timestamps never pay for it.
            if (TemporalInference.forcesDateNanos(parsed) && value.indexOf(' ') < 0) {
                return TEMPORAL_NANOS_FORCED;
            }
            return TEMPORAL_DATETIME;
        } catch (DateTimeParseException e) {
            return NOT_TEMPORAL;
        }
    }
}
