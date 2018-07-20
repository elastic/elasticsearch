/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.util.Objects;

public class CsvLogFileStructureFactory implements LogFileStructureFactory {

    private final Terminal terminal;

    public CsvLogFileStructureFactory(Terminal terminal) {
        this.terminal = Objects.requireNonNull(terminal);
    }

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
    public boolean canCreateFromSample(String sample) {
        return SeparatedValuesLogFileStructure.canCreateFromSample(terminal, sample, 2, CsvPreference.EXCEL_PREFERENCE, "CSV");
    }

    @Override
    public LogFileStructure createFromSample(String sampleFileName, String indexName, String typeName, String elasticsearchHost,
                                             String logstashHost, String logstashFileTimezone, String sample, String charsetName)
        throws IOException {
        return new SeparatedValuesLogFileStructure(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost,
            logstashFileTimezone, sample, charsetName, CsvPreference.EXCEL_PREFERENCE, false);
    }
}
