/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

public abstract class AbstractDataToProcessWriter implements DataToProcessWriter {

    private static final int TIME_FIELD_OUT_INDEX = 0;
    private static final long MS_IN_SECOND = 1000;

    private final boolean includeControlField;

    protected final AutodetectProcess autodetectProcess;
    protected final DataDescription dataDescription;
    protected final AnalysisConfig analysisConfig;
    protected final DataCountsReporter dataCountsReporter;

    private final Logger logger;
    private final DateTransformer dateTransformer;
    private long latencySeconds;

    protected Map<String, Integer> inFieldIndexes;
    protected List<InputOutputMap> inputOutputMap;

    // epoch in seconds
    private long latestEpochMs;
    private long latestEpochMsThisUpload;

    protected AbstractDataToProcessWriter(boolean includeControlField, AutodetectProcess autodetectProcess,
                                          DataDescription dataDescription, AnalysisConfig analysisConfig,
                                          DataCountsReporter dataCountsReporter, Logger logger) {
        this.includeControlField = includeControlField;
        this.autodetectProcess = Objects.requireNonNull(autodetectProcess);
        this.dataDescription = Objects.requireNonNull(dataDescription);
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.dataCountsReporter = Objects.requireNonNull(dataCountsReporter);
        this.logger = Objects.requireNonNull(logger);
        this.latencySeconds = analysisConfig.getLatency() == null ? 0 : analysisConfig.getLatency().seconds();

        Date date = dataCountsReporter.getLatestRecordTime();
        latestEpochMsThisUpload = 0;
        latestEpochMs = 0;
        if (date != null) {
            latestEpochMs = date.getTime();
        }

        boolean isDateFormatString = dataDescription.isTransformTime() && !dataDescription.isEpochMs();
        if (isDateFormatString) {
            dateTransformer = new DateFormatDateTransformer(dataDescription.getTimeFormat());
        } else {
            dateTransformer = new DoubleDateTransformer(dataDescription.isEpochMs());
        }
    }

    /**
     * Set up the field index mappings. This must be called before
     * {@linkplain DataToProcessWriter#write(InputStream, XContentType, BiConsumer)}
     * <p>
     * Finds the required input indexes in the <code>header</code> and sets the
     * mappings to the corresponding output indexes.
     */
    void buildFieldIndexMapping(String[] header) throws IOException {
        Collection<String> inputFields = inputFields();
        inFieldIndexes = inputFieldIndexes(header, inputFields);
        checkForMissingFields(inputFields, inFieldIndexes, header);

        inputOutputMap = createInputOutputMap(inFieldIndexes);
        dataCountsReporter.setAnalysedFieldsPerRecord(analysisConfig.analysisFields().size());
    }

    /**
     * Write the header.
     * The header is created from the list of analysis input fields, the time field and the control field.
     */
    @Override
    public void writeHeader() throws IOException {
        Map<String, Integer> outFieldIndexes = outputFieldIndexes();

        // header is all the analysis input fields + the time field + control field
        int numFields = outFieldIndexes.size();
        String[] record = new String[numFields];

        for (Map.Entry<String, Integer> entry : outFieldIndexes.entrySet()) {
            record[entry.getValue()] = entry.getKey();
        }

        // Write the header
        autodetectProcess.writeRecord(record);
    }

    /**
     * Transform the input data and write to length encoded writer.<br>
     * <p>
     * Fields that aren't transformed i.e. those in inputOutputMap must be
     * copied from input to output before this function is called.
     * <p>
     * First all the transforms whose outputs the Date transform relies
     * on are executed then the date transform then the remaining transforms.
     *
     * @param record             The record that will be written to the length encoded writer after the time has been transformed.
     *                           This should be the same size as the number of output (analysis fields) i.e.
     *                           the size of the map returned by {@linkplain #outputFieldIndexes()}
     * @param numberOfFieldsRead The total number read not just those included in the analysis
     */
    protected boolean transformTimeAndWrite(String[] record, long numberOfFieldsRead) throws IOException {
        long epochMs;
        try {
            epochMs = dateTransformer.transform(record[TIME_FIELD_OUT_INDEX]);
        } catch (CannotParseTimestampException e) {
            dataCountsReporter.reportDateParseError(numberOfFieldsRead);
            logger.error(e.getMessage());
            return false;
        }

        record[TIME_FIELD_OUT_INDEX] = Long.toString(epochMs / MS_IN_SECOND);

        // Records have epoch seconds timestamp so compare for out of order in seconds
        if (epochMs / MS_IN_SECOND < latestEpochMs / MS_IN_SECOND - latencySeconds) {
            // out of order
            dataCountsReporter.reportOutOfOrderRecord(numberOfFieldsRead);

            if (epochMs > latestEpochMsThisUpload) {
                // record this timestamp even if the record won't be processed
                latestEpochMsThisUpload = epochMs;
                dataCountsReporter.reportLatestTimeIncrementalStats(latestEpochMsThisUpload);
            }
            return false;
        }

        latestEpochMs = Math.max(latestEpochMs, epochMs);
        latestEpochMsThisUpload = latestEpochMs;

        autodetectProcess.writeRecord(record);
        dataCountsReporter.reportRecordWritten(numberOfFieldsRead, epochMs);

        return true;
    }

    @Override
    public void flushStream() throws IOException {
        autodetectProcess.flushStream();
    }

    /**
     * Get all the expected input fields i.e. all the fields we
     * must see in the csv header
     */
    final Collection<String> inputFields() {
        Set<String> requiredFields = analysisConfig.analysisFields();
        requiredFields.add(dataDescription.getTimeField());

        return requiredFields;
    }

    /**
     * Find the indexes of the input fields from the header
     */
    protected final Map<String, Integer> inputFieldIndexes(String[] header, Collection<String> inputFields) {
        List<String> headerList = Arrays.asList(header);  // TODO header could be empty

        Map<String, Integer> fieldIndexes = new HashMap<String, Integer>();

        for (String field : inputFields) {
            int index = headerList.indexOf(field);
            if (index >= 0) {
                fieldIndexes.put(field, index);
            }
        }

        return fieldIndexes;
    }

    Map<String, Integer> getInputFieldIndexes() {
        return inFieldIndexes;
    }

    /**
     * Create indexes of the output fields.
     * This is the time field and all the fields configured for analysis
     * and the control field.
     * Time is the first field and the last is the control field
     */
    protected final Map<String, Integer> outputFieldIndexes() {
        Map<String, Integer> fieldIndexes = new HashMap<>();

        // time field
        fieldIndexes.put(dataDescription.getTimeField(), TIME_FIELD_OUT_INDEX);

        int index = TIME_FIELD_OUT_INDEX + 1;
        for (String field : analysisConfig.analysisFields()) {
            fieldIndexes.put(field, index++);
        }

        // control field
        if (includeControlField) {
            fieldIndexes.put(LengthEncodedWriter.CONTROL_FIELD_NAME, index);
        }

        return fieldIndexes;
    }

    /**
     * The number of fields used in the analysis field,
     * the time field and (sometimes) the control field
     */
    protected int outputFieldCount() {
        return analysisConfig.analysisFields().size() + (includeControlField ? 2 : 1);
    }

    protected Map<String, Integer> getOutputFieldIndexes() {
        return outputFieldIndexes();
    }

    /**
     * Create a map of input index to output index. This does not include the time or control fields.
     *
     * @param inFieldIndexes Map of field name to index in the input array
     */
    private List<InputOutputMap> createInputOutputMap(Map<String, Integer> inFieldIndexes) {
        List<InputOutputMap> inputOutputMap = new ArrayList<>();

        int outIndex = TIME_FIELD_OUT_INDEX;
        Integer inIndex = inFieldIndexes.get(dataDescription.getTimeField());
        if (inIndex == null) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "Input time field '%s' not found", dataDescription.getTimeField()));
        }
        inputOutputMap.add(new InputOutputMap(inIndex, outIndex));

        for (String field : analysisConfig.analysisFields()) {
            ++outIndex;
            inIndex = inFieldIndexes.get(field);
            if (inIndex != null) {
                inputOutputMap.add(new InputOutputMap(inIndex, outIndex));
            }
        }

        return inputOutputMap;
    }

    protected List<InputOutputMap> getInputOutputMap() {
        return inputOutputMap;
    }

    /**
     * Check that all the fields are present in the header.
     * Either return true or throw a MissingFieldException
     * <p>
     * Every input field should have an entry in <code>inputFieldIndexes</code>
     * otherwise the field cannot be found.
     */
    protected abstract boolean checkForMissingFields(Collection<String> inputFields, Map<String, Integer> inputFieldIndexes,
            String[] header);

    /**
     * Input and output array indexes map
     */
    protected class InputOutputMap {
        int inputIndex;
        int outputIndex;

        public InputOutputMap(int in, int out) {
            inputIndex = in;
            outputIndex = out;
        }
    }
}
