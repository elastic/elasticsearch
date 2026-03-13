/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

    static final int DEFAULT_SAMPLE_SIZE = 100;

    private static final DataType[] TYPE_CANDIDATES = {
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.DATETIME,
        DataType.KEYWORD };

    private CsvSchemaInferrer() {}

    /**
     * Infers schema from column names and sample data rows.
     *
     * @param columnNames header names (plain, without type annotations)
     * @param sampleRows  sample data rows; each row is a string array of cell values
     * @return list of attributes with inferred types
     */
    static List<Attribute> inferSchema(String[] columnNames, List<String[]> sampleRows) {
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
                candidateIdx[col] = narrowCandidate(candidateIdx[col], typeConfirmed[col], value);
                typeConfirmed[col] = true;
            }
        }

        List<Attribute> attributes = new ArrayList<>(numCols);
        for (int col = 0; col < numCols; col++) {
            String name = columnNames[col].trim();
            DataType type = seenValue[col] ? TYPE_CANDIDATES[candidateIdx[col]] : DataType.KEYWORD;
            EsField field = new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
            attributes.add(new FieldAttribute(Source.EMPTY, name, field));
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
    private static int narrowCandidate(int currentIdx, boolean confirmed, String value) {
        while (currentIdx < TYPE_CANDIDATES.length - 1) {
            if (canParse(TYPE_CANDIDATES[currentIdx], value)) {
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

    private static boolean canParse(DataType type, String value) {
        return switch (type) {
            case BOOLEAN -> Booleans.isBoolean(value.toLowerCase(Locale.ROOT));
            case INTEGER -> canParseInt(value);
            case LONG -> canParseLong(value);
            case DOUBLE -> canParseDouble(value);
            case DATETIME -> canParseDatetime(value);
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

    private static boolean canParseDatetime(String value) {
        try {
            DateUtils.asDateTime(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
