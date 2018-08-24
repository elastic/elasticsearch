/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.util.List;

public class CsvLogStructureFinderFactory implements LogStructureFinderFactory {

    /**
     * Rules are:
     * - The file must be valid CSV
     * - It must contain at least two complete records
     * - There must be at least two fields per record (otherwise files with no commas could be treated as CSV!)
     * - Every CSV record except the last must have the same number of fields
     * The reason the last record is allowed to have fewer fields than the others is that
     * it could have been truncated when the file was sampled.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample) {
        return SeparatedValuesLogStructureFinder.canCreateFromSample(explanation, sample, 2, CsvPreference.EXCEL_PREFERENCE, "CSV");
    }

    @Override
    public LogStructureFinder createFromSample(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker)
        throws IOException {
        return SeparatedValuesLogStructureFinder.makeSeparatedValuesLogStructureFinder(explanation, sample, charsetName, hasByteOrderMarker,
            CsvPreference.EXCEL_PREFERENCE, false);
    }
}
