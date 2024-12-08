/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

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

import static org.elasticsearch.xpack.core.ml.utils.Intervals.alignToFloor;

public abstract class AbstractDataToProcessWriter implements DataToProcessWriter {

    private static final int TIME_FIELD_OUT_INDEX = 0;
    private static final long MS_IN_SECOND = 1000;

    private final boolean includeControlField;
    private final boolean includeTokensField;

    protected final AutodetectProcess autodetectProcess;
    protected final DataDescription dataDescription;
    protected final AnalysisConfig analysisConfig;
    protected final DataCountsReporter dataCountsReporter;

    private final Logger logger;
    private final DateTransformer dateTransformer;
    private final long bucketSpanMs;
    private final long latencySeconds;

    protected Map<String, Integer> inFieldIndexes;
    protected List<InputOutputMap> inputOutputMap;

    // epoch in seconds
    private long latestEpochMs;
    private long latestEpochMsThisUpload;

    private Set<String> termFields;

    protected AbstractDataToProcessWriter(
        boolean includeControlField,
        boolean includeTokensField,
        AutodetectProcess autodetectProcess,
        DataDescription dataDescription,
        AnalysisConfig analysisConfig,
        DataCountsReporter dataCountsReporter,
        Logger logger
    ) {
        this.includeControlField = includeControlField;
        this.includeTokensField = includeTokensField;
        this.autodetectProcess = Objects.requireNonNull(autodetectProcess);
        this.dataDescription = Objects.requireNonNull(dataDescription);
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.dataCountsReporter = Objects.requireNonNull(dataCountsReporter);
        this.logger = Objects.requireNonNull(logger);
        this.latencySeconds = analysisConfig.getLatency() == null ? 0 : analysisConfig.getLatency().seconds();
        this.bucketSpanMs = analysisConfig.getBucketSpan().getMillis();
        this.termFields = analysisConfig.termFields();

        Date date = dataCountsReporter.getLatestRecordTime();
        latestEpochMsThisUpload = 0;
        latestEpochMs = 0;
        if (date != null) {
            latestEpochMs = date.getTime();
        }

        boolean isDateFormatString = dataDescription.isTransformTime() && dataDescription.isEpochMs() == false;
        if (isDateFormatString) {
            dateTransformer = new DateFormatDateTransformer(dataDescription.getTimeFormat());
        } else {
            dateTransformer = new DoubleDateTransformer(dataDescription.isEpochMs());
        }
    }

    public String maybeTruncateCatgeorizationField(String categorizationField) {
        if (termFields.contains(analysisConfig.getCategorizationFieldName()) == false) {
            return categorizationField.substring(0, Math.min(categorizationField.length(), AnalysisConfig.MAX_CATEGORIZATION_FIELD_LENGTH));
        }
        return categorizationField;
    }

    /**
     * Set up the field index mappings. This must be called before
     * {@linkplain DataToProcessWriter#write(InputStream, CategorizationAnalyzer, XContentType, BiConsumer)}
     * <p>
     * Finds the required input indexes in the <code>header</code> and sets the
     * mappings to the corresponding output indexes.
     */
    void buildFieldIndexMapping(String[] header) {
        Collection<String> inputFields = inputFields();
        inFieldIndexes = inputFieldIndexes(header, inputFields);
        checkForMissingFields(inputFields, inFieldIndexes, header);

        inputOutputMap = createInputOutputMap(inFieldIndexes);
        // The time field doesn't count
        dataCountsReporter.setAnalysedFieldsPerRecord(inputFields().size() - 1);
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
     * Tokenize the field that has been configured for categorization, and store the resulting list of tokens in CSV
     * format in the appropriate field of the record to be sent to the process.
     * @param categorizationAnalyzer   The analyzer to use to convert the categorization field to a list of tokens
     * @param categorizationFieldValue The value of the categorization field to be tokenized
     * @param record                   The record to be sent to the process
     */
    protected void tokenizeForCategorization(
        CategorizationAnalyzer categorizationAnalyzer,
        String categorizationFieldValue,
        String[] record
    ) {
        assert includeTokensField;
        // -2 because last field is the control field, and last but one is the pre-tokenized tokens field
        record[record.length - 2] = tokenizeForCategorization(
            categorizationAnalyzer,
            analysisConfig.getCategorizationFieldName(),
            categorizationFieldValue
        );
    }

    /**
     * Accessible for testing only.
     */
    static String tokenizeForCategorization(
        CategorizationAnalyzer categorizationAnalyzer,
        String categorizationFieldName,
        String categorizationFieldValue
    ) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String token : categorizationAnalyzer.tokenizeField(categorizationFieldName, categorizationFieldValue)) {
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
            if (needsEscaping(token)) {
                builder.append('"');
                for (int i = 0; i < token.length(); ++i) {
                    char c = token.charAt(i);
                    if (c == '"') {
                        builder.append('"');
                    }
                    builder.append(c);
                }
                builder.append('"');
            } else {
                builder.append(token);
            }
        }
        return builder.toString();
    }

    private static boolean needsEscaping(String value) {
        for (int i = 0; i < value.length(); ++i) {
            char c = value.charAt(i);
            if (c == '"' || c == ',' || c == '\n' || c == '\r') {
                return true;
            }
        }
        return false;
    }

    /**
     * Transform the date in the input data and write all fields to the length encoded writer.
     * <p>
     * Fields  must be copied from input to output before this function is called.
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
        final long latestBucketFloor = alignToFloor(latestEpochMs, bucketSpanMs);

        // We care only about records that are older than the current bucket according to our latest timestamp
        // The native side handles random order within the same bucket without issue
        if (epochMs / MS_IN_SECOND < latestBucketFloor / MS_IN_SECOND - latencySeconds) {
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
        dataCountsReporter.reportRecordWritten(numberOfFieldsRead, epochMs, latestEpochMs);

        return true;
    }

    @Override
    public void flushStream() throws IOException {
        autodetectProcess.flushStream();
    }

    /**
     * Get all the expected input fields i.e. all the fields we
     * must see in the input
     */
    final Collection<String> inputFields() {
        Set<String> requiredFields = analysisConfig.analysisFields();
        requiredFields.add(dataDescription.getTimeField());
        requiredFields.remove(AnalysisConfig.ML_CATEGORY_FIELD);
        return requiredFields;
    }

    /**
     * Find the indexes of the input fields from the header
     */
    protected static Map<String, Integer> inputFieldIndexes(String[] header, Collection<String> inputFields) {
        List<String> headerList = Arrays.asList(header);  // TODO header could be empty

        Map<String, Integer> fieldIndexes = new HashMap<>();

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
            if (AnalysisConfig.ML_CATEGORY_FIELD.equals(field) == false) {
                fieldIndexes.put(field, index++);
            }
        }

        // field for categorization tokens
        if (includeTokensField) {
            fieldIndexes.put(LengthEncodedWriter.PRETOKENISED_TOKEN_FIELD, index++);
        }

        // control field
        if (includeControlField) {
            fieldIndexes.put(LengthEncodedWriter.CONTROL_FIELD_NAME, index++);
        }

        return fieldIndexes;
    }

    /**
     * The number of fields used in the analysis field,
     * the time field and (sometimes) the control field
     */
    protected int outputFieldCount() {
        return inputFields().size() + (includeControlField ? 1 : 0) + (includeTokensField ? 1 : 0);
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
            throw new IllegalStateException(String.format(Locale.ROOT, "Input time field '%s' not found", dataDescription.getTimeField()));
        }
        inputOutputMap.add(new InputOutputMap(inIndex, outIndex));

        for (String field : analysisConfig.analysisFields()) {
            if (AnalysisConfig.ML_CATEGORY_FIELD.equals(field) == false) {
                ++outIndex;
                inIndex = inFieldIndexes.get(field);
                if (inIndex != null) {
                    inputOutputMap.add(new InputOutputMap(inIndex, outIndex));
                }
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
    protected abstract boolean checkForMissingFields(
        Collection<String> inputFields,
        Map<String, Integer> inputFieldIndexes,
        String[] header
    );

    /**
     * Input and output array indexes map
     */
    protected static class InputOutputMap {
        int inputIndex;
        int outputIndex;

        public InputOutputMap(int in, int out) {
            inputIndex = in;
            outputIndex = out;
        }
    }
}
