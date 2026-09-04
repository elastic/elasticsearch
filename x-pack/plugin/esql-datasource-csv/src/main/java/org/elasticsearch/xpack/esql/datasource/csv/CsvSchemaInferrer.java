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
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

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
 * default to KEYWORD. When a value doesn't fit the current candidate, an unconfirmed column takes
 * the narrowest candidate that does fit. For a confirmed column the ladder only recognises which type
 * accepts the value; {@link org.elasticsearch.xpack.esql.datasources.spi.TypeWidening} then says what
 * that evidence and the accepted type combine to, which is often neither of them. So a column with
 * "true" and "42" resolves KEYWORD, a numeric column meeting a timestamp resolves KEYWORD rather than
 * the next rung down, and one holding millisecond and nanosecond timestamps resolves DATE_NANOS
 * &mdash; none of which is written here.
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
         * A {@code datetime} the {@code date_nanos} rail cannot parse. Tracked apart because a column
         * holding one must never end up on that rail, whatever some other value in it says.
         */
        DATETIME_UNDECODABLE_AS_NANOS,
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
     * sample already committed to its type). A column whose type cannot represent a value in
     * {@code additionalRows} moves to whatever type represents both, which for a confirmed column is
     * usually {@code KEYWORD}.
     * <p>
     * A column can also move the other way. If these rows show a dialect the {@code date_nanos} rail
     * cannot parse, a column sitting on that rail is demoted to {@code datetime} even though nothing
     * widened &mdash; so "nothing widened" is not the same as "nothing changed".
     * <p>
     * Returns the same {@code schema} reference only when neither happens (including when
     * {@code additionalRows} is empty), and a new list otherwise.
     *
     * @param schema         the schema returned by a prior {@link #inferSchema} call
     * @param additionalRows rows that were not included in the initial sample
     * @param datetimeFormatter the same formatter used for the initial inference
     */
    static List<Attribute> widenSchema(List<Attribute> schema, List<String[]> additionalRows, @Nullable DateFormatter datetimeFormatter) {
        return widenSchema(schema, additionalRows, datetimeFormatter, new boolean[schema.size()]);
    }

    /** As above, carrying the sample's undecodable-dialect evidence forward. */
    static List<Attribute> widenSchema(
        List<Attribute> schema,
        List<String[]> additionalRows,
        @Nullable DateFormatter datetimeFormatter,
        boolean[] sawUndecodableTemporal
    ) {
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
                int newIdx = narrowCandidate(candidateIdx[col], true, value, datetimeFormatter, sawUndecodableTemporal, col);
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
        boolean anyDemotion = false;
        for (int col = 0; col < numCols; col++) {
            if (sawUndecodableTemporal[col] && TYPE_CANDIDATES[candidateIdx[col]] == DataType.DATE_NANOS) {
                anyDemotion = true;
                break;
            }
        }
        // Not just anyWidened: the window can contribute a dialect the nanos rail cannot decode without
        // moving any rung, and returning the original schema there would leave the column on that rail.
        if (anyWidened == false && anyDemotion == false) {
            return schema;
        }
        List<Attribute> widened = new ArrayList<>(numCols);
        for (int col = 0; col < numCols; col++) {
            Attribute original = schema.get(col);
            DataType newType = demoteIfDialectCannotDecode(TYPE_CANDIDATES[candidateIdx[col]], sawUndecodableTemporal[col]);
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
        return inferSchema(columnNames, sampleRows, datetimeFormatter, new boolean[columnNames.length]);
    }

    /**
     * As above, reporting per column whether the sample held a timestamp the date_nanos rail cannot parse.
     * <p>
     * A caller that goes on to widen against a second window must pass the same array back, or a
     * column demoted here could be promoted again by a nanosecond value in that window with the
     * sample's evidence forgotten.
     */
    static List<Attribute> inferSchema(
        String[] columnNames,
        List<String[]> sampleRows,
        @Nullable DateFormatter datetimeFormatter,
        boolean[] sawUndecodableTemporal
    ) {
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
                candidateIdx[col] = narrowCandidate(
                    candidateIdx[col],
                    typeConfirmed[col],
                    value,
                    datetimeFormatter,
                    sawUndecodableTemporal,
                    col
                );
                typeConfirmed[col] = true;
            }
        }

        List<Attribute> attributes = new ArrayList<>(numCols);
        for (int col = 0; col < numCols; col++) {
            String name = columnNames[col].trim();
            DataType type = seenValue[col] ? TYPE_CANDIDATES[candidateIdx[col]] : DataType.KEYWORD;
            type = demoteIfDialectCannotDecode(type, sawUndecodableTemporal[col]);
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, false));
        }
        return attributes;
    }

    /**
     * Keeps a column off the {@code date_nanos} rail when its own sample held a timestamp that rail
     * cannot decode.
     * <p>
     * Screening the forcing value alone is not enough: a column can be flipped by a well-formed
     * nanosecond value and still hold cells in a dialect that rail cannot parse, which then
     * fail per-cell at read under the default FAIL_FAST policy — a file that reads today would stop
     * reading. Demoting to {@code datetime} keeps exactly the behaviour such a file has now. It costs
     * the sub-millisecond digits on that column, which is the same thing {@code datetime} has always
     * done there, and only for files mixing two dialects in one column.
     * <p>
     * Applied at resolution rather than during the walk, so the answer does not depend on whether the
     * row carrying that dialect came before or after the one that forced the flip.
     */
    private static DataType demoteIfDialectCannotDecode(DataType resolved, boolean sawUndecodableTemporal) {
        return resolved == DataType.DATE_NANOS && sawUndecodableTemporal ? DataType.DATETIME : resolved;
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
    private static int narrowCandidate(
        int currentIdx,
        boolean confirmed,
        String value,
        @Nullable DateFormatter datetimeFormatter,
        boolean[] sawUndecodableTemporal,
        int col
    ) {
        int evidenceIdx = recognise(currentIdx, value, datetimeFormatter, sawUndecodableTemporal, col);
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
        // The join never invents a third type — a test in the lattice's own suite pins that
        // exhaustively — so the answer is either the evidence or the top, and there is no rung to
        // search for. It cannot be the accepted type: the identity case returned above, and
        // recognition starts at the accepted rung, so any evidence that gets here is strictly wider.
        return committed == evidence ? evidenceIdx : TYPE_CANDIDATES.length - 1;
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
    private static int recognise(
        int fromIdx,
        String value,
        @Nullable DateFormatter datetimeFormatter,
        boolean[] sawUndecodableTemporal,
        int col
    ) {
        Temporal temporal = null; // computed on demand, and only if a temporal rung is reached
        for (int idx = fromIdx; idx < TYPE_CANDIDATES.length - 1; idx++) {
            DataType candidate = TYPE_CANDIDATES[idx];
            if (candidate == DataType.DATETIME || candidate == DataType.DATE_NANOS) {
                if (temporal == null) {
                    temporal = classifyTemporal(value, datetimeFormatter);
                    if (temporal == Temporal.DATETIME_UNDECODABLE_AS_NANOS) {
                        sawUndecodableTemporal[col] = true;
                    }
                }
                // DATETIME accepts the timestamps it reads without dropping digits; DATE_NANOS accepts
                // any timestamp at all, including ones outside its own window. That second half is a
                // recognition rule, not a lattice edge, and it is what stops a settled date_nanos
                // column from treating an out-of-window timestamp as evidence that it is really a
                // string: the value is recognised at the DATE_NANOS rung, so the column stays put.
                // Such a cell then fails per-cell at decode, which is what a declared date_nanos
                // schema does with the same file.
                boolean fits = candidate == DataType.DATETIME
                    ? (temporal == Temporal.DATETIME || temporal == Temporal.DATETIME_UNDECODABLE_AS_NANOS)
                    : temporal != Temporal.NOT_TEMPORAL;
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
     * The formatter the {@code date_nanos} decode rail parses with. Asking it directly is the point:
     * the CSV rail's own parser accepts dialects this one rejects, and a column holding one must never
     * be typed {@code date_nanos}.
     * <p>
     * This replaced two successive attempts to restate that grammar by shape — first "has no space",
     * then "has no space and has seconds" — which between them missed three dialects: seconds-less
     * times, then signed and over-long years. Each was cheaper and each was a restatement of someone
     * else's parser, which drifts the moment that parser accepts something new. The oracle costs a
     * parse on values already known to be timestamps; the guesses cost correctness.
     * <p>
     * Taken from {@code EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER} rather than re-declared
     * from its pattern string, so no copy of it is left to drift.
     * <p>
     * Note it is the DIALECT being asked about, not the range: a value this parses but that falls
     * outside the representable window is deliberately kept (demoting on it would make a column's type
     * depend on row order) and fails per-cell at read, exactly as a declared {@code date_nanos} schema
     * makes it fail.
     */
    private static final DateFormatter NANOS_RAIL_FORMAT = EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;

    private static boolean nanosRailCanDecode(String value) {
        return NANOS_RAIL_FORMAT.tryParse(value) != null;
    }

    /**
     * Classifies a value four ways: not a timestamp, one {@code datetime} reads losslessly, one only
     * {@code date_nanos} reads losslessly, and one the {@code date_nanos} rail cannot parse at all.
     * <p>
     * Which values are <em>accepted</em> as timestamps is unchanged: the first parse below is the one
     * this method has always done, and its result — previously discarded — is what decides the
     * precision. Deciding the fourth case does cost a second parse, against the rail's own formatter;
     * see {@link #NANOS_RAIL_FORMAT} for why that is worth paying and what it replaced.
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
            // Some dialects this parser accepts, the date_nanos rail cannot parse at all. Reported
            // rather than merely screened: it must not be the value that flips a column
            // onto that rail, AND a column that holds one must not be flipped by some other value
            // either, or this cell turns from readable into a per-cell error.
            boolean nanosRailCanDecode = nanosRailCanDecode(value);
            if (nanosRailCanDecode && TemporalInference.forcesDateNanos(parsed)) {
                return Temporal.NANOS_FORCED;
            }
            return nanosRailCanDecode ? Temporal.DATETIME : Temporal.DATETIME_UNDECODABLE_AS_NANOS;
        } catch (DateTimeParseException e) {
            return Temporal.NOT_TEMPORAL;
        }
    }
}
