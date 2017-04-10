/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;

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
class JsonDataToProcessWriter extends AbstractDataToProcessWriter {

    private static final Logger LOGGER = Loggers.getLogger(JsonDataToProcessWriter.class);
    private NamedXContentRegistry xContentRegistry;

    JsonDataToProcessWriter(boolean includeControlField, AutodetectProcess autodetectProcess,
            DataDescription dataDescription, AnalysisConfig analysisConfig,
            DataCountsReporter dataCountsReporter, NamedXContentRegistry xContentRegistry) {
        super(includeControlField, autodetectProcess, dataDescription, analysisConfig,
                dataCountsReporter, LOGGER);
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Read the JSON inputIndex, transform to length encoded values and pipe to
     * the OutputStream. No transformation is applied to the data the timestamp
     * is expected in seconds from the epoch. If any of the fields in
     * <code>analysisFields</code> or the <code>DataDescription</code>s
     * timeField is missing from the JOSN inputIndex an exception is thrown
     */
    @Override
    public void write(InputStream inputStream, XContentType xContentType, BiConsumer<DataCounts, Exception> handler)
            throws IOException {
        dataCountsReporter.startNewIncrementalCount();

        if (xContentType.equals(XContentType.JSON)) {
            writeJsonXContent(inputStream);
        } else if (xContentType.equals(XContentType.SMILE)) {
            writeSmileXContent(inputStream);
        } else {
            throw new RuntimeException("XContentType [" + xContentType
                    + "] is not supported by JsonDataToProcessWriter");
        }

        // this line can throw and will be propagated
        dataCountsReporter.finishReporting(
                ActionListener.wrap(
                        response -> handler.accept(dataCountsReporter.incrementalStats(), null),
                        e -> handler.accept(null, e)
                ));
    }

    private void writeJsonXContent(InputStream inputStream) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, inputStream)) {
            writeJson(parser);
        }
    }

    private void writeSmileXContent(InputStream inputStream) throws IOException {
        while (true) {
            byte[] nextObject = findNextObject(XContentType.SMILE.xContent().streamSeparator(), inputStream);
            if (nextObject.length == 0) {
                break;
            }
            try (XContentParser parser = XContentFactory.xContent(XContentType.SMILE)
                    .createParser(xContentRegistry, nextObject)) {
                writeJson(parser);
            }
        }
    }

    private byte[] findNextObject(byte marker, InputStream data) throws IOException {
        // The underlying stream, MarkSupportingStreamInputWrapper, doesn't care about
        // readlimit, so just set to -1.  We could pick a value, but I worry that if the
        // underlying implementation changes it may cause strange behavior, whereas -1 should
        // blow up immediately
        assert(data.markSupported());
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

    private void writeJson(XContentParser parser) throws IOException {
        Collection<String> analysisFields = inputFields();

        buildFieldIndexMapping(analysisFields.toArray(new String[0]));

        int numFields = outputFieldCount();
        String[] input = new String[numFields];
        String[] record = new String[numFields];

        // We never expect to get the control field
        boolean[] gotFields = new boolean[analysisFields.size()];

        XContentRecordReader recordReader = new XContentRecordReader(parser, inFieldIndexes,
                LOGGER);
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
                record[inOut.outputIndex] = (field == null) ? "" : field;
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
    protected boolean checkForMissingFields(Collection<String> inputFields,
            Map<String, Integer> inputFieldIndexes,
            String[] header) {
        return true;
    }

    /**
     * Return the number of missing fields
     */
    private static long missingFieldCount(boolean[] gotFieldFlags) {
        long count = 0;

        for (int i = 0; i < gotFieldFlags.length; i++) {
            if (gotFieldFlags[i] == false) {
                ++count;
            }
        }

        return count;
    }
}
