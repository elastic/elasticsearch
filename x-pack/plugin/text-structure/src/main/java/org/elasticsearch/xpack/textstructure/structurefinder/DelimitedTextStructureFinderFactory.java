/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class DelimitedTextStructureFinderFactory implements TextStructureFinderFactory {

    static final double DELIMITER_OVERRIDDEN_ALLOWED_FRACTION_OF_BAD_LINES = 0.10d;
    static final double FORMAT_OVERRIDDEN_ALLOWED_FRACTION_OF_BAD_LINES = 0.05d;
    private final CsvPreference csvPreference;
    private final int minFieldsPerRow;
    private final boolean trimFields;

    DelimitedTextStructureFinderFactory(char delimiter, char quote, int minFieldsPerRow, boolean trimFields) {
        csvPreference = new CsvPreference.Builder(quote, delimiter, "\n").build();
        this.minFieldsPerRow = minFieldsPerRow;
        this.trimFields = trimFields;
    }

    DelimitedTextStructureFinderFactory makeSimilar(Character quote, Boolean shouldTrimFields) {

        return new DelimitedTextStructureFinderFactory(
            (char) csvPreference.getDelimiterChar(),
            (quote == null) ? csvPreference.getQuoteChar() : quote,
            minFieldsPerRow,
            (shouldTrimFields == null) ? this.trimFields : shouldTrimFields
        );
    }

    @Override
    public boolean canFindFormat(TextStructure.Format format) {
        return format == null || format == TextStructure.Format.DELIMITED;
    }

    /**
     * Rules are:
     * - It must contain at least two complete records
     * - There must be a minimum number of fields per record (otherwise text with no commas could be treated as CSV!)
     * - Every record except the last must have the same number of fields
     * The reason the last record is allowed to have fewer fields than the others is that
     * it could have been truncated when the text was sampled.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample, double allowedFractionOfBadLines) {
        String formatName = switch ((char) csvPreference.getDelimiterChar()) {
            case ',' -> "CSV";
            case '\t' -> "TSV";
            default -> Character.getName(csvPreference.getDelimiterChar()).toLowerCase(Locale.ROOT) + " delimited values";
        };
        return DelimitedTextStructureFinder.canCreateFromSample(
            explanation,
            sample,
            minFieldsPerRow,
            csvPreference,
            formatName,
            allowedFractionOfBadLines
        );
    }

    @Override
    public TextStructureFinder createFromSample(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException {
        CsvPreference adjustedCsvPreference = new CsvPreference.Builder(csvPreference).maxLinesPerRow(lineMergeSizeLimit).build();
        return DelimitedTextStructureFinder.makeDelimitedTextStructureFinder(
            explanation,
            sample,
            charsetName,
            hasByteOrderMarker,
            adjustedCsvPreference,
            trimFields,
            overrides,
            timeoutChecker
        );
    }
}
