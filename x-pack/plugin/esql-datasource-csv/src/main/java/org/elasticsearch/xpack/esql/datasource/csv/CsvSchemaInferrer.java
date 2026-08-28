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
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

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
 * next candidate. What a column becomes when a value does not fit its current type is not decided
 * here at all: the ladder recognises which type accepts the value, and
 * {@link org.elasticsearch.xpack.esql.datasources.spi.TypeWidening} says what the accepted type and
 * that evidence combine to. So a column with "true" and "42" resolves KEYWORD, and one with
 * millisecond and nanosecond timestamps resolves DATE_NANOS, without either being written here.
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

    /** How a value reads on the temporal rungs. {@code null} anywhere below means "not classified yet". */
    private enum Temporal {
        /** Not a timestamp at all. */
        NOT_TEMPORAL,
        /** A timestamp the column reads as {@code datetime}. */
        DATETIME,
        /**
         * A timestamp that {@code datetime} cannot read without dropping digits, and that
         * {@code date_nanos} can both represent and decode &mdash; so it moves the column.
         */
        NANOS_FORCED
    }

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
     * Commits a column to the type that represents everything it has shown so far, given one more
     * value.
     * <p>
     * Two questions, kept apart. {@link #recognise} answers "which is the narrowest candidate whose
     * string form accepts this token" &mdash; knowledge only a text reader has, since only it is
     * handed raw text. {@link TypeWidening} answers "which single type represents the accepted type
     * and this new evidence" &mdash; the question NDJSON and cross-file reconciliation also ask, so it
     * is answered in one place for all of them.
     * <p>
     * Fusing the two is what made a numeric column commit to {@code datetime} on its first timestamp:
     * walking the ladder answers the recognition question, and the ladder's next rung is not the type
     * a number and a timestamp have in common. The lattice says they have none below {@code keyword},
     * and says it without a special case per pair.
     * <p>
     * An unconfirmed column has nothing to combine with, so its first value simply sets the type.
     *
     * @param currentIdx the candidate the column has committed to so far
     * @param confirmed  whether any value has confirmed that candidate
     */
    private static int narrowCandidate(int currentIdx, boolean confirmed, String value, @Nullable DateFormatter datetimeFormatter) {
        int evidenceIdx = recognise(currentIdx, value, datetimeFormatter);
        if (confirmed == false) {
            return evidenceIdx;
        }
        if (evidenceIdx == currentIdx) {
            // The column's own type still fits, which is the common case for every settled column.
            // Returning here is what keeps the lattice a per-commitment cost rather than a per-value
            // one; join(t, t) is t, so this changes nothing but the work done to find that out.
            return currentIdx;
        }
        DataType accepted = TYPE_CANDIDATES[currentIdx];
        DataType evidence = TYPE_CANDIDATES[evidenceIdx];
        DataType committed = TypeWidening.join(accepted, evidence, TypeWidening.Policy.INFERENCE);
        // The join never invents a third type, so the answer is one of these three rungs and there is
        // no rung to search for. A test in the lattice's own suite pins that property exhaustively,
        // rather than it being merely true today.
        if (committed == accepted) {
            return currentIdx;
        }
        if (committed == evidence) {
            return evidenceIdx;
        }
        return TYPE_CANDIDATES.length - 1; // KEYWORD, the lattice's top and the ladder's last rung
    }

    /**
     * The narrowest candidate at or after {@code fromIdx} whose string form accepts this value, or
     * KEYWORD when none does.
     * <p>
     * Starting at the column's own candidate rather than at the first rung is what keeps this cheap:
     * a settled numeric column tries its own type first and stops. On the default ISO rail it also
     * costs nothing in accuracy, because the rungs below any accepted type either cannot accept the
     * value at all (their string forms are disjoint) or are narrower types that join back to it
     * anyway &mdash; a {@code long} column meeting {@code 42} commits to {@code long} whether the
     * evidence is read as {@code integer} or as {@code long}. A declared {@code datetime_format} can
     * break that disjointness, since an alternation may accept a token a numeric rung would also
     * accept; the outcome there is whatever it was before this became a lattice lookup, which is why
     * the walk is not started from the first rung.
     * <p>
     * The value is classified as temporal at most once, however many rungs are walked, so the two
     * temporal rungs share one parse.
     */
    private static int recognise(int fromIdx, String value, @Nullable DateFormatter datetimeFormatter) {
        Temporal temporal = null; // computed on demand, and only if a temporal rung is reached
        for (int idx = fromIdx; idx < TYPE_CANDIDATES.length - 1; idx++) {
            DataType candidate = TYPE_CANDIDATES[idx];
            if (candidate == DataType.DATETIME || candidate == DataType.DATE_NANOS) {
                if (temporal == null) {
                    temporal = classifyTemporal(value, datetimeFormatter);
                }
                // DATETIME accepts the timestamps it reads without dropping digits; DATE_NANOS accepts
                // any timestamp at all, including ones outside its own window. That second half is a
                // recognition rule, not a lattice edge, and it is what stops a settled date_nanos
                // column from treating an out-of-window timestamp as evidence that it is really a
                // string: the value is recognised at the DATE_NANOS rung, so the column stays put.
                // Such a cell then fails per-cell at decode, which is what a declared date_nanos
                // schema does with the same file.
                boolean fits = candidate == DataType.DATETIME ? temporal == Temporal.DATETIME : temporal != Temporal.NOT_TEMPORAL;
                if (fits) {
                    return idx;
                }
            } else if (canParse(candidate, value)) {
                return idx;
            }
        }
        return TYPE_CANDIDATES.length - 1;
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
    private static Temporal classifyTemporal(String value, @Nullable DateFormatter datetimeFormatter) {
        if (datetimeFormatter != null) {
            return datetimeFormatter.tryParse(value) != null ? Temporal.DATETIME : Temporal.NOT_TEMPORAL;
        }
        try {
            ZonedDateTime parsed = DateUtils.asDateTime(value);
            // The whitespace-separated dialect is accepted here but rejected by the date_nanos decode
            // rail, so such a value must not be what flips a column onto that rail — it would turn a
            // readable cell into a per-cell error. Checked only for values already carrying
            // sub-millisecond digits, so ordinary timestamps never pay for it.
            if (TemporalInference.forcesDateNanos(parsed) && value.indexOf(' ') < 0) {
                return Temporal.NANOS_FORCED;
            }
            return Temporal.DATETIME;
        } catch (DateTimeParseException e) {
            return Temporal.NOT_TEMPORAL;
        }
    }
}
