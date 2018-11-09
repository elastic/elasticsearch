/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A writer for transforming and piping CSV data from an
 * inputstream to outputstream.
 * The data written to outputIndex is length encoded each record
 * consists of number of fields followed by length/value pairs.
 * See CLengthEncodedInputParser.h in the C++ code for a more
 * detailed description.
 * A control field is added to the end of each length encoded
 * line.
 */
class CsvDataToProcessWriter extends AbstractDataToProcessWriter {

    private static final Logger LOGGER = LogManager.getLogger(CsvDataToProcessWriter.class);

    /**
     * Maximum number of lines allowed within a single CSV record.
     * <p>
     * In the scenario where there is a misplaced quote, there is
     * the possibility that it results to a single record expanding
     * over many lines. Supercsv will eventually deplete all memory
     * from the JVM. We set a limit to an arbitrary large number
     * to prevent that from happening. Unfortunately, supercsv
     * throws an exception which means we cannot recover and continue
     * reading new records from the next line.
     */
    private static final int MAX_LINES_PER_RECORD = 10000;

    CsvDataToProcessWriter(boolean includeControlField, boolean includeTokensField, AutodetectProcess autodetectProcess,
                           DataDescription dataDescription, AnalysisConfig analysisConfig,
                           DataCountsReporter dataCountsReporter) {
        super(includeControlField, includeTokensField, autodetectProcess, dataDescription, analysisConfig, dataCountsReporter, LOGGER);
    }

    /**
     * Read the csv inputIndex, transform to length encoded values and pipe to
     * the OutputStream. If any of the expected fields in the
     * analysis inputIndex or if the expected time field is missing from the CSV
     * header a exception is thrown
     */
    @Override
    public void write(InputStream inputStream, CategorizationAnalyzer categorizationAnalyzer, XContentType xContentType,
                      BiConsumer<DataCounts, Exception> handler) throws IOException {
        CsvPreference csvPref = new CsvPreference.Builder(
                dataDescription.getQuoteCharacter(),
                dataDescription.getFieldDelimiter(),
                new String(new char[]{DataDescription.LINE_ENDING}))
                .maxLinesPerRow(MAX_LINES_PER_RECORD).build();

        dataCountsReporter.startNewIncrementalCount();

        try (CsvListReader csvReader = new CsvListReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8), csvPref)) {
            String[] header = csvReader.getHeader(true);
            if (header == null) { // null if EoF
                handler.accept(dataCountsReporter.incrementalStats(), null);
                return;
            }

            long inputFieldCount = Math.max(header.length - 1, 0); // time field doesn't count

            buildFieldIndexMapping(header);

            int maxIndex = 0;
            for (Integer index : inFieldIndexes.values()) {
                maxIndex = Math.max(index, maxIndex);
            }
            Integer categorizationFieldIndex = inFieldIndexes.get(analysisConfig.getCategorizationFieldName());

            int numFields = outputFieldCount();
            String[] record = new String[numFields];

            List<String> line;
            while ((line = csvReader.read()) != null) {
                Arrays.fill(record, "");

                if (maxIndex >= line.size()) {
                    LOGGER.warn("Not enough fields in csv record, expected at least " + maxIndex + ". " + line);

                    for (InputOutputMap inOut : inputOutputMap) {
                        if (inOut.inputIndex >= line.size()) {
                            dataCountsReporter.reportMissingField();
                            continue;
                        }

                        String field = line.get(inOut.inputIndex);
                        record[inOut.outputIndex] = (field == null) ? "" : field;
                    }
                } else {
                    for (InputOutputMap inOut : inputOutputMap) {
                        String field = line.get(inOut.inputIndex);
                        record[inOut.outputIndex] = (field == null) ? "" : field;
                    }
                }

                if (categorizationAnalyzer != null && categorizationFieldIndex != null && categorizationFieldIndex < line.size()) {
                    tokenizeForCategorization(categorizationAnalyzer, line.get(categorizationFieldIndex), record);
                }
                transformTimeAndWrite(record, inputFieldCount);
            }

            // This function can throw
            dataCountsReporter.finishReporting(ActionListener.wrap(
                    response -> handler.accept(dataCountsReporter.incrementalStats(), null),
                    e -> handler.accept(null, e)
            ));
        }
    }

    @Override
    protected boolean checkForMissingFields(Collection<String> inputFields, Map<String, Integer> inputFieldIndexes, String[] header) {
        for (String field : inputFields) {
            if (AnalysisConfig.AUTO_CREATED_FIELDS.contains(field)) {
                continue;
            }
            Integer index = inputFieldIndexes.get(field);
            if (index == null) {
                String msg = String.format(Locale.ROOT, "Field configured for analysis '%s' is not in the CSV header '%s'",
                        field, Arrays.toString(header));

                LOGGER.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }

        return true;
    }
}
