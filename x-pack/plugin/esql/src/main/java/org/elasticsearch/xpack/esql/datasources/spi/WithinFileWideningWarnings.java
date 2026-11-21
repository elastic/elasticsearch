/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.function.Consumer;

/**
 * Emits the within-file schema-inference-widening warnings shared by CSV/TSV and NDJSON: a
 * keyword-fallback notice when a column folded to {@link DataType#KEYWORD}, and a precision-loss
 * notice when a column merged {@code long} and {@code double} evidence. Each reader first resolves
 * its own format-specific widening record into a {@link WidenedColumn} (truncating the value — see
 * {@link WidenedColumn#MAX_VALUE_LENGTH}), then delegates warning text and {@link SkipWarnings}
 * grouping here, parameterized only by the noun its summary uses for a column ({@code column} for
 * CSV, {@code field} for NDJSON) and the noun its row-numbering uses ({@code row}/{@code record}).
 */
public final class WithinFileWideningWarnings {

    private WithinFileWideningWarnings() {}

    /**
     * @param sourceLocation the object this sample came from, ALREADY escaped by the caller -- CSV through
     *                       {@link RowLocation#displayObjectName}, NDJSON through
     *                       {@link RowLocation#escapedForDisplay} -- because it is printed inside a bracketed
     *                       clause this method builds by concatenation and a store-supplied path is free to
     *                       close that bracket. The column name beside it is escaped HERE instead, so the two
     *                       store-supplied fields of this detail are escaped on opposite sides of the call;
     *                       a third caller handing this one a raw path reopens the forgery
     * @param columnNoun     "column" or "field", used only in the summary line (e.g. "A field's
     *                       inferred type changed..."); the per-widening detail always says "column"
     *                       regardless, matching both readers' existing wording
     * @param sampleUnitNoun "row" or "record", used in the per-widening detail's sample position
     */
    public static void report(
        List<WidenedColumn> widenedColumns,
        String sourceLocation,
        String columnNoun,
        String sampleUnitNoun,
        Consumer<String> warningSink
    ) {
        if (widenedColumns.isEmpty()) {
            return;
        }
        SkipWarnings keywordWarnings = null;
        SkipWarnings precisionWarnings = null;
        for (WidenedColumn widened : widenedColumns) {
            // The column name is store-supplied too -- an NDJSON field name is an arbitrary JSON key -- and it is
            // printed first inside this same bracketed sentence, so leaving it raw would let it forge the whole
            // detail that follows and defeat the escape applied to the location. The sampled value is the third
            // such field and is deliberately NOT escaped: it is the user's own data, bracketed cells are ordinary
            // in it (csv-headerless.csv-spec pins warnings reading value [[-2.14] and value [12]]), and an escape
            // there would mangle the common case to close the rare one, and where the perimeter of the escape
            // should fall is a question about every message embedding a sampled value, not about this one.
            String detail = "column ["
                + RowLocation.escapedForDisplay(widened.columnName())
                + "] at sample "
                + sampleUnitNoun
                + " ["
                + widened.sampleRow()
                + "] of ["
                + sourceLocation
                + "]: value ["
                + widened.value()
                + "] forced type ["
                + widened.toType().typeName()
                + "] (was ["
                + widened.fromType().typeName()
                + "])";
            if (widened.toType() == DataType.KEYWORD) {
                if (keywordWarnings == null) {
                    keywordWarnings = new SkipWarnings(keywordSummary(columnNoun), warningSink);
                }
                keywordWarnings.add(detail);
            } else {
                if (precisionWarnings == null) {
                    precisionWarnings = new SkipWarnings(precisionSummary(columnNoun), warningSink);
                }
                precisionWarnings.add(detail);
            }
        }
    }

    private static String keywordSummary(String columnNoun) {
        return "A "
            + columnNoun
            + "'s inferred type changed partway through the schema sample and is read as [keyword]; "
            + "set [schema_resolution] to [strict] to fail instead";
    }

    private static String precisionSummary(String columnNoun) {
        return "A "
            + columnNoun
            + " mixing [long] and [double] within the schema sample is read as [double], losing precision above 2^53; "
            + "set [schema_resolution] to [strict] to fail instead";
    }
}
