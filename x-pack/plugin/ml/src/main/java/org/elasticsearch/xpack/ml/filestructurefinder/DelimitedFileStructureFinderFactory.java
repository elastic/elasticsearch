/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class DelimitedFileStructureFinderFactory implements FileStructureFinderFactory {

    private final CsvPreference csvPreference;
    private final int minFieldsPerRow;
    private final boolean trimFields;

    DelimitedFileStructureFinderFactory(char delimiter, int minFieldsPerRow, boolean trimFields) {
        csvPreference = new CsvPreference.Builder('"', delimiter, "\n").build();
        this.minFieldsPerRow = minFieldsPerRow;
        this.trimFields = trimFields;
    }

    /**
     * Rules are:
     * - It must contain at least two complete records
     * - There must be a minimum number of fields per record (otherwise files with no commas could be treated as CSV!)
     * - Every record except the last must have the same number of fields
     * The reason the last record is allowed to have fewer fields than the others is that
     * it could have been truncated when the file was sampled.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample) {
        String formatName;
        switch ((char) csvPreference.getDelimiterChar()) {
            case ',':
                formatName = "CSV";
                break;
            case '\t':
                formatName = "TSV";
                break;
            default:
                formatName = Character.getName(csvPreference.getDelimiterChar()).toLowerCase(Locale.ROOT) + " delimited values";
                break;
        }
        return DelimitedFileStructureFinder.canCreateFromSample(explanation, sample, minFieldsPerRow, csvPreference, formatName);
    }

    @Override
    public FileStructureFinder createFromSample(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker)
        throws IOException {
        return DelimitedFileStructureFinder.makeDelimitedFileStructureFinder(explanation, sample, charsetName, hasByteOrderMarker,
            csvPreference, trimFields);
    }
}
