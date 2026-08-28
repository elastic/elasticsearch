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
 *   <li>{@code KEYWORD} — universal fallback (everything is a string)</li>
 * </ol>
 * Null and empty values are compatible with every type. Columns with only null/empty values
 * default to KEYWORD. When a value doesn't fit the current candidate, the column widens to the
 * next candidate. Boolean and datetime columns that were confirmed by at least one value skip
 * directly to KEYWORD on mismatch (since a column with both "true" and "42" is most likely a
 * string column, not numeric).
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
        DataType.KEYWORD };

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
     * current candidate index. When a column has been confirmed as BOOLEAN or DATETIME by previous
     * values and a new value doesn't fit, skip directly to KEYWORD (since a column with "true" and
     * "42" is most likely a string column, not numeric).
     * For unconfirmed columns or numeric types, narrow one step at a time.
     */
    private static int narrowCandidate(int currentIdx, boolean confirmed, String value, @Nullable DateFormatter datetimeFormatter) {
        while (currentIdx < TYPE_CANDIDATES.length - 1) {
            if (canParse(TYPE_CANDIDATES[currentIdx], value, datetimeFormatter)) {
                return currentIdx;
            }
            DataType current = TYPE_CANDIDATES[currentIdx];
            if (confirmed && (current == DataType.BOOLEAN || current == DataType.DATETIME)) {
                return TYPE_CANDIDATES.length - 1;
            }
            currentIdx++;
        }
        return currentIdx;
    }

    private static boolean canParse(DataType type, String value, @Nullable DateFormatter datetimeFormatter) {
        return switch (type) {
            case BOOLEAN -> Booleans.isBoolean(value.toLowerCase(Locale.ROOT));
            case INTEGER -> canParseInt(value);
            case LONG -> canParseLong(value);
            case DOUBLE -> canParseDouble(value);
            case DATETIME -> canParseDatetime(value, datetimeFormatter);
            default -> true;
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

    private static boolean canParseDatetime(String value, @Nullable DateFormatter datetimeFormatter) {
        if (datetimeFormatter != null) {
            return datetimeFormatter.tryParse(value) != null;
        }
        try {
            DateUtils.asDateTime(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
