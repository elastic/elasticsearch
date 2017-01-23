/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfigs;
import org.elasticsearch.xpack.ml.transforms.DependencySorter;
import org.elasticsearch.xpack.ml.transforms.Transform;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformIndex;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformResult;
import org.elasticsearch.xpack.ml.transforms.TransformException;
import org.elasticsearch.xpack.ml.transforms.TransformFactory;
import org.elasticsearch.xpack.ml.transforms.date.DateFormatTransform;
import org.elasticsearch.xpack.ml.transforms.date.DateTransform;
import org.elasticsearch.xpack.ml.transforms.date.DoubleDateTransform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractDataToProcessWriter implements DataToProcessWriter {

    protected static final int TIME_FIELD_OUT_INDEX = 0;
    private static final int MS_IN_SECOND = 1000;

    protected final boolean includeControlField;

    protected final AutodetectProcess autodetectProcess;
    protected final DataDescription dataDescription;
    protected final AnalysisConfig analysisConfig;
    protected final DataCountsReporter dataCountsReporter;
    protected final Logger logger;
    protected final TransformConfigs transformConfigs;

    protected List<Transform> dateInputTransforms;
    protected DateTransform dateTransform;
    protected List<Transform> postDateTransforms;

    protected Map<String, Integer> inFieldIndexes;
    protected List<InputOutputMap> inputOutputMap;

    private String[] scratchArea;
    private String[][] readWriteArea;

    // epoch in seconds
    private long latestEpochMs;
    private long latestEpochMsThisUpload;


    protected AbstractDataToProcessWriter(boolean includeControlField, AutodetectProcess autodetectProcess,
                                          DataDescription dataDescription, AnalysisConfig analysisConfig,
                                          TransformConfigs transformConfigs, DataCountsReporter dataCountsReporter, Logger logger) {
        this.includeControlField = includeControlField;
        this.autodetectProcess = Objects.requireNonNull(autodetectProcess);
        this.dataDescription = Objects.requireNonNull(dataDescription);
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.dataCountsReporter = Objects.requireNonNull(dataCountsReporter);
        this.logger = Objects.requireNonNull(logger);
        this.transformConfigs = Objects.requireNonNull(transformConfigs);

        postDateTransforms = new ArrayList<>();
        dateInputTransforms = new ArrayList<>();
        Date date = dataCountsReporter.getLatestRecordTime();
        latestEpochMsThisUpload = 0;
        latestEpochMs = 0;
        if (date != null) {
            latestEpochMs = date.getTime();
        }

        readWriteArea = new String[3][];
    }


    /**
     * Create the transforms. This must be called before
     * {@linkplain DataToProcessWriter#write(java.io.InputStream)}
     * even if no transforms are configured as it creates the
     * date transform and sets up the field mappings.<br>
     * <p>
     * Finds the required input indexes in the <code>header</code>
     * and sets the mappings for the transforms so they know where
     * to read their inputs and write outputs.
     * <p>
     * Transforms can be chained so some write their outputs to
     * a scratch area which is input to another transform
     */
    public void buildTransforms(String[] header) throws IOException {
        Collection<String> inputFields = inputFields();
        inFieldIndexes = inputFieldIndexes(header, inputFields);
        checkForMissingFields(inputFields, inFieldIndexes, header);

        Map<String, Integer> outFieldIndexes = outputFieldIndexes();
        inputOutputMap = createInputOutputMap(inFieldIndexes);
        dataCountsReporter.setAnalysedFieldsPerRecord(analysisConfig.analysisFields().size());

        Map<String, Integer> scratchAreaIndexes = scratchAreaIndexes(inputFields, outputFields(),
                dataDescription.getTimeField());
        scratchArea = new String[scratchAreaIndexes.size()];
        readWriteArea[TransformFactory.SCRATCH_ARRAY_INDEX] = scratchArea;

        buildDateTransform(scratchAreaIndexes, outFieldIndexes);

        List<TransformConfig> dateInputTransforms = DependencySorter.findDependencies(
                dataDescription.getTimeField(), transformConfigs.getTransforms());

        TransformFactory transformFactory = new TransformFactory();
        for (TransformConfig config : dateInputTransforms) {
            Transform tr = transformFactory.create(config, inFieldIndexes, scratchAreaIndexes,
                    outFieldIndexes, logger);
            this.dateInputTransforms.add(tr);
        }

        // get the transforms that don't input into the date
        List<TransformConfig> postDateTransforms = new ArrayList<>();
        for (TransformConfig tc : transformConfigs.getTransforms()) {
            if (dateInputTransforms.contains(tc) == false) {
                postDateTransforms.add(tc);
            }
        }

        postDateTransforms = DependencySorter.sortByDependency(postDateTransforms);
        for (TransformConfig config : postDateTransforms) {
            Transform tr = transformFactory.create(config, inFieldIndexes, scratchAreaIndexes,
                    outFieldIndexes, logger);
            this.postDateTransforms.add(tr);
        }
    }

    /**
     * Write the header.
     * The header is created from the list of analysis input fields,
     * the time field and the control field
     */
    @Override
    public void writeHeader() throws IOException {
        Map<String, Integer> outFieldIndexes = outputFieldIndexes();

        //  header is all the analysis input fields + the time field + control field
        int numFields = outFieldIndexes.size();
        String[] record = new String[numFields];

        Iterator<Map.Entry<String, Integer>> itr = outFieldIndexes.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, Integer> entry = itr.next();
            record[entry.getValue()] = entry.getKey();
        }

        // Write the header
        autodetectProcess.writeRecord(record);
    }

    protected void buildDateTransform(Map<String, Integer> scratchAreaIndexes, Map<String, Integer> outFieldIndexes) {
        List<TransformIndex> readIndexes = new ArrayList<>();

        Integer index = inFieldIndexes.get(dataDescription.getTimeField());
        if (index != null) {
            readIndexes.add(new TransformIndex(TransformFactory.INPUT_ARRAY_INDEX, index));
        } else {
            index = outFieldIndexes.get(dataDescription.getTimeField());
            if (index != null) {
                // date field could also be an output field
                readIndexes.add(new TransformIndex(TransformFactory.OUTPUT_ARRAY_INDEX, index));
            } else if (scratchAreaIndexes.containsKey(dataDescription.getTimeField())) {
                index = scratchAreaIndexes.get(dataDescription.getTimeField());
                readIndexes.add(new TransformIndex(TransformFactory.SCRATCH_ARRAY_INDEX, index));
            } else {
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "Transform input date field '%s' not found",
                                dataDescription.getTimeField()));
            }
        }

        List<TransformIndex> writeIndexes = new ArrayList<>();
        writeIndexes.add(new TransformIndex(TransformFactory.OUTPUT_ARRAY_INDEX,
                outFieldIndexes.get(dataDescription.getTimeField())));

        boolean isDateFormatString = dataDescription.isTransformTime() && !dataDescription.isEpochMs();
        if (isDateFormatString) {
            dateTransform = new DateFormatTransform(dataDescription.getTimeFormat(), readIndexes, writeIndexes, logger);
        } else {
            dateTransform = new DoubleDateTransform(dataDescription.isEpochMs(), readIndexes, writeIndexes, logger);
        }
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
     * @param input              The record the transforms should read their input from. The contents should
     *                           align with the header parameter passed to {@linkplain #buildTransforms(String[])}
     * @param output             The record that will be written to the length encoded writer.
     *                           This should be the same size as the number of output (analysis fields) i.e.
     *                           the size of the map returned by {@linkplain #outputFieldIndexes()}
     * @param numberOfFieldsRead The total number read not just those included in the analysis
     */
    protected boolean applyTransformsAndWrite(String[] input, String[] output, long numberOfFieldsRead)
            throws IOException {
        readWriteArea[TransformFactory.INPUT_ARRAY_INDEX] = input;
        readWriteArea[TransformFactory.OUTPUT_ARRAY_INDEX] = output;
        Arrays.fill(readWriteArea[TransformFactory.SCRATCH_ARRAY_INDEX], "");

        if (!applyTransforms(dateInputTransforms, numberOfFieldsRead)) {
            return false;
        }

        try {
            dateTransform.transform(readWriteArea);
        } catch (TransformException e) {
            dataCountsReporter.reportDateParseError(numberOfFieldsRead);
            logger.error(e.getMessage());
            return false;
        }

        long epochMs = dateTransform.epochMs();

        // Records have epoch seconds timestamp so compare for out of order in seconds
        if (epochMs / MS_IN_SECOND < latestEpochMs / MS_IN_SECOND - analysisConfig.getLatency()) {
            // out of order
            dataCountsReporter.reportOutOfOrderRecord(inFieldIndexes.size());

            if (epochMs > latestEpochMsThisUpload) {
                // record this timestamp even if the record won't be processed
                latestEpochMsThisUpload = epochMs;
                dataCountsReporter.reportLatestTimeIncrementalStats(latestEpochMsThisUpload);
            }
            return false;
        }

        // Now do the rest of the transforms
        if (!applyTransforms(postDateTransforms, numberOfFieldsRead)) {
            return false;
        }

        latestEpochMs = Math.max(latestEpochMs, epochMs);
        latestEpochMsThisUpload = latestEpochMs;

        autodetectProcess.writeRecord(output);
        dataCountsReporter.reportRecordWritten(numberOfFieldsRead, latestEpochMs);

        return true;
    }

    /**
     * If false then the transform is excluded
     */
    private boolean applyTransforms(List<Transform> transforms, long inputFieldCount) {
        for (Transform tr : transforms) {
            try {
                TransformResult result = tr.transform(readWriteArea);
                if (result == TransformResult.EXCLUDE) {
                    return false;
                }
            } catch (TransformException e) {
                logger.warn(e);
            }
        }

        return true;
    }

    @Override
    public void flush() throws IOException {
        autodetectProcess.flushStream();
    }

    /**
     * Get all the expected input fields i.e. all the fields we
     * must see in the csv header.
     * = transform input fields + analysis fields that aren't a transform output
     * + the date field - the transform output field names
     */
    public final Collection<String> inputFields() {
        Set<String> requiredFields = new HashSet<>(analysisConfig.analysisFields());
        requiredFields.add(dataDescription.getTimeField());
        requiredFields.addAll(transformConfigs.inputFieldNames());

        requiredFields.removeAll(transformConfigs.outputFieldNames()); // inputs not in a transform

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

    public Map<String, Integer> getInputFieldIndexes() {
        return inFieldIndexes;
    }

    /**
     * This output fields are the time field and all the fields
     * configured for analysis
     */
    public final Collection<String> outputFields() {
        List<String> outputFields = new ArrayList<>(analysisConfig.analysisFields());
        outputFields.add(dataDescription.getTimeField());

        return outputFields;
    }

    /**
     * Create indexes of the output fields.
     * This is the time field and all the fields configured for analysis
     * and the control field.
     * Time is the first field and the last is the control field
     */
    protected final Map<String, Integer> outputFieldIndexes() {
        Map<String, Integer> fieldIndexes = new HashMap<String, Integer>();

        // time field
        fieldIndexes.put(dataDescription.getTimeField(), TIME_FIELD_OUT_INDEX);

        int index = TIME_FIELD_OUT_INDEX + 1;
        List<String> analysisFields = analysisConfig.analysisFields();
        Collections.sort(analysisFields);

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
    public int outputFieldCount() {
        return analysisConfig.analysisFields().size() + (includeControlField ? 2 : 1);
    }

    protected Map<String, Integer> getOutputFieldIndexes() {
        return outputFieldIndexes();
    }


    /**
     * Find all the scratch area fields. These are those that are input to a
     * transform but are not written to the output or read from input. i.e. for
     * the case where a transforms output is used exclusively by another
     * transform
     *
     * @param inputFields
     *            Fields we expect in the header
     * @param outputFields
     *            Fields that are written to the analytics
     * @param dateTimeField date field
     */
    protected final Map<String, Integer> scratchAreaIndexes(Collection<String> inputFields, Collection<String> outputFields,
            String dateTimeField) {
        Set<String> requiredFields = new HashSet<>(transformConfigs.outputFieldNames());
        boolean dateTimeFieldIsTransformOutput = requiredFields.contains(dateTimeField);

        requiredFields.addAll(transformConfigs.inputFieldNames());

        requiredFields.removeAll(inputFields);
        requiredFields.removeAll(outputFields);

        // date time is a output of a transform AND the input to the date time transform
        // so add it back into the scratch area
        if (dateTimeFieldIsTransformOutput) {
            requiredFields.add(dateTimeField);
        }

        int index = 0;
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (String field : requiredFields) {
            result.put(field, new Integer(index++));
        }

        return result;
    }


    /**
     * For inputs that aren't transformed create a map of input index
     * to output index. This does not include the time or control fields
     *
     * @param inFieldIndexes Map of field name -&gt; index in the input array
     */
    protected final List<InputOutputMap> createInputOutputMap(Map<String, Integer> inFieldIndexes) {
        // where no transform
        List<InputOutputMap> inputOutputMap = new ArrayList<>();

        int outIndex = TIME_FIELD_OUT_INDEX + 1;
        for (String field : analysisConfig.analysisFields()) {
            Integer inIndex = inFieldIndexes.get(field);
            if (inIndex != null) {
                inputOutputMap.add(new InputOutputMap(inIndex, outIndex));
            }

            ++outIndex;
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
