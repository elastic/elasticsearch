/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A writer for transforming and piping JSON data from an
 * inputstream to outputstream.
 * The data written to outputIndex is length encoded each record
 * consists of number of fields followed by length/value pairs.
 * See CLengthEncodedInputParser.h in the C++ code for a more
 * detailed description.
 */
public class JsonDataToProcessWriter extends AbstractDataToProcessWriter {

    private static final Logger LOGGER = LogManager.getLogger(JsonDataToProcessWriter.class);
    private final XContentParserConfiguration parserConfig;

    public JsonDataToProcessWriter(
        boolean includeControlField,
        boolean includeTokensField,
        AutodetectProcess autodetectProcess,
        DataDescription dataDescription,
        AnalysisConfig analysisConfig,
        DataCountsReporter dataCountsReporter,
        NamedXContentRegistry xContentRegistry
    ) {
        super(includeControlField, includeTokensField, autodetectProcess, dataDescription, analysisConfig, dataCountsReporter, LOGGER);
        this.parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
    }

    /**
     * Read the JSON inputIndex, transform to length encoded values and pipe to
     * the OutputStream. No transformation is applied to the data the timestamp
     * is expected in seconds from the epoch. If any of the fields in
     * <code>analysisFields</code> or the <code>DataDescription</code>s
     * timeField is missing from the JSON inputIndex an exception is thrown
     */
    @Override
    public void write(
        InputStream inputStream,
        CategorizationAnalyzer categorizationAnalyzer,
        XContentType xContentType,
        BiConsumer<DataCounts, Exception> handler
    ) throws IOException {
        dataCountsReporter.startNewIncrementalCount();

        if (xContentType.canonical() == XContentType.JSON) {
            writeJsonXContent(categorizationAnalyzer, inputStream);
        } else if (xContentType.canonical() == XContentType.SMILE) {
            writeSmileXContent(categorizationAnalyzer, inputStream);
        } else {
            throw new RuntimeException("XContentType [" + xContentType + "] is not supported by JsonDataToProcessWriter");
        }

        dataCountsReporter.finishReporting();
        handler.accept(dataCountsReporter.incrementalStats(), null);
    }

    private void writeJsonXContent(CategorizationAnalyzer categorizationAnalyzer, InputStream inputStream) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, inputStream)) {
            writeJson(categorizationAnalyzer, parser);
        }
    }

    private void writeSmileXContent(CategorizationAnalyzer categorizationAnalyzer, InputStream inputStream) throws IOException {
        while (true) {
            byte[] nextObject = findNextObject(XContentType.SMILE.xContent().streamSeparator(), inputStream);
            if (nextObject.length == 0) {
                break;
            }
            try (XContentParser parser = XContentFactory.xContent(XContentType.SMILE).createParser(parserConfig, nextObject)) {
                writeJson(categorizationAnalyzer, parser);
            }
        }
    }

    private byte[] findNextObject(byte marker, InputStream data) throws IOException {
        // The underlying stream, MarkSupportingStreamInputWrapper, doesn't care about
        // readlimit, so just set to -1. We could pick a value, but I worry that if the
        // underlying implementation changes it may cause strange behavior, whereas -1 should
        // blow up immediately
        assert (data.markSupported());
        data.mark(-1);

        int nextByte;
        int counter = 0;
        do {
            nextByte = data.read();
            counter += 1;

            // marker & 0xFF to deal with Java's lack of unsigned bytes...
            if (nextByte == (marker & 0xFF)) {
                data.reset();
                byte[] buffer = new byte[counter];
                data.read(buffer);
                return buffer;
            }
        } while (nextByte != -1);

        return new byte[0];
    }

    private void writeJson(CategorizationAnalyzer categorizationAnalyzer, XContentParser parser) throws IOException {
        Collection<String> inputFields = inputFields();

        buildFieldIndexMapping(inputFields.toArray(new String[0]));

        int numFields = outputFieldCount();
        String[] input = new String[numFields];
        String[] record = new String[numFields];

        // We never expect to get the control field or categorization tokens field
        boolean[] gotFields = new boolean[inputFields.size()];

        XContentRecordReader recordReader = new XContentRecordReader(parser, inFieldIndexes, LOGGER);
        Integer categorizationFieldIndex = inFieldIndexes.get(analysisConfig.getCategorizationFieldName());
        long inputFieldCount = recordReader.read(input, gotFields);
        while (inputFieldCount >= 0) {
            Arrays.fill(record, "");

            inputFieldCount = Math.max(inputFieldCount - 1, 0); // time field doesn't count

            long missing = missingFieldCount(gotFields);
            if (missing > 0) {
                dataCountsReporter.reportMissingFields(missing);
            }

            for (InputOutputMap inOut : inputOutputMap) {
                String field = input[inOut.inputIndex];
                field = (field == null) ? "" : field;
                if (categorizationFieldIndex != null && inOut.inputIndex == categorizationFieldIndex) {
                    field = maybeTruncateCatgeorizationField(field);
                }
                record[inOut.outputIndex] = field;
            }

            if (categorizationAnalyzer != null && categorizationFieldIndex != null) {
                tokenizeForCategorization(categorizationAnalyzer, input[categorizationFieldIndex], record);
            }

            transformTimeAndWrite(record, inputFieldCount);

            inputFieldCount = recordReader.read(input, gotFields);
        }
    }

    /**
     * Don't enforce the check that all the fields are present in JSON docs.
     * Always returns true
     */
    @Override
    protected boolean checkForMissingFields(Collection<String> inputFields, Map<String, Integer> inputFieldIndexes, String[] header) {
        return true;
    }

    /**
     * Return the number of missing fields
     */
    private static long missingFieldCount(boolean[] gotFieldFlags) {
        long count = 0;

        for (boolean gotFieldFlag : gotFieldFlags) {
            if (gotFieldFlag == false) {
                ++count;
            }
        }

        return count;
    }
}
