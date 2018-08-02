/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;


/**
 * Utility class for running a Ml process<br>
 * The process runs in a clean environment.
 */
public class ProcessCtrl {

    /**
     * Autodetect API native program name - always loaded from the same directory as the controller process
     */
    public static final String AUTODETECT = "autodetect";
    static final String AUTODETECT_PATH = "./" + AUTODETECT;

    /**
     * The normalization native program name - always loaded from the same directory as the controller process
     */
    public static final String NORMALIZE = "normalize";
    static final String NORMALIZE_PATH = "./" + NORMALIZE;

    /**
     * Process controller native program name
     */
    public static final String CONTROLLER = "controller";

    /**
     * Name of the config setting containing the path to the logs directory
     */
    private static final int DEFAULT_MAX_NUM_RECORDS = 500;
    /**
     * The maximum number of anomaly records that will be written each bucket
     */
    public static final Setting<Integer> MAX_ANOMALY_RECORDS_SETTING = Setting.intSetting("max.anomaly.records", DEFAULT_MAX_NUM_RECORDS,
            Property.NodeScope);

    /*
     * General arguments
     */
    static final String JOB_ID_ARG = "--jobid=";

    /*
     * Arguments used by both autodetect and normalize
     */
    static final String BUCKET_SPAN_ARG = "--bucketspan=";
    public static final String DELETE_STATE_FILES_ARG = "--deleteStateFiles";
    static final String LENGTH_ENCODED_INPUT_ARG = "--lengthEncodedInput";
    static final String MODEL_CONFIG_ARG = "--modelconfig=";
    public static final String QUANTILES_STATE_PATH_ARG = "--quantilesState=";
    static final String PER_PARTITION_NORMALIZATION = "--perPartitionNormalization";

    /*
     * Arguments used by autodetect
     */
    static final String LATENCY_ARG = "--latency=";
    static final String RESULT_FINALIZATION_WINDOW_ARG = "--resultFinalizationWindow=";
    static final String MULTIVARIATE_BY_FIELDS_ARG = "--multivariateByFields";
    static final String PERSIST_INTERVAL_ARG = "--persistInterval=";
    static final String MAX_QUANTILE_INTERVAL_ARG = "--maxQuantileInterval=";
    static final String SUMMARY_COUNT_FIELD_ARG = "--summarycountfield=";
    static final String TIME_FIELD_ARG = "--timefield=";

    private static final int SECONDS_IN_HOUR = 3600;

    /**
     * Roughly how often should the C++ process persist state?  A staggering
     * factor that varies by job is added to this.
     */
    static final long DEFAULT_BASE_PERSIST_INTERVAL = 10800; // 3 hours

    /**
     * Roughly how often should the C++ process output quantiles when no
     * anomalies are being detected?  A staggering factor that varies by job is
     * added to this.
     */
    static final int BASE_MAX_QUANTILE_INTERVAL = 21600; // 6 hours

    /**
     * Name of the model config file
     */
    static final String ML_MODEL_CONF = "mlmodel.conf";

    /**
     * Persisted quantiles are written to disk so they can be read by
     * the autodetect program.  All quantiles files have this extension.
     */
    private static final String QUANTILES_FILE_EXTENSION = ".json";

    /**
     * Config setting storing the flag that disables model persistence
     */
    public static final Setting<Boolean> DONT_PERSIST_MODEL_STATE_SETTING = Setting.boolSetting("no.model.state.persist", false,
            Property.NodeScope);

    static String maxAnomalyRecordsArg(Settings settings) {
        return "--maxAnomalyRecords=" + MAX_ANOMALY_RECORDS_SETTING.get(settings);
    }

    private ProcessCtrl() {
    }

    /**
     * This random time of up to 1 hour is added to intervals at which we
     * tell the C++ process to perform periodic operations.  This means that
     * when there are many jobs there is a certain amount of staggering of
     * their periodic operations.  A given job will always be given the same
     * staggering interval (for a given JVM implementation).
     *
     * @param jobId The ID of the job to calculate the staggering interval for
     * @return The staggering interval
     */
    static int calculateStaggeringInterval(String jobId) {
        Random rng = new Random(jobId.hashCode());
        return rng.nextInt(SECONDS_IN_HOUR);
    }

    public static List<String> buildAutodetectCommand(Environment env, Settings settings, Job job, Logger logger) {
        List<String> command = new ArrayList<>();
        command.add(AUTODETECT_PATH);

        String jobId = JOB_ID_ARG + job.getId();
        command.add(jobId);

        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        if (analysisConfig != null) {
            addIfNotNull(analysisConfig.getBucketSpan(), BUCKET_SPAN_ARG, command);
            addIfNotNull(analysisConfig.getLatency(), LATENCY_ARG, command);
            addIfNotNull(analysisConfig.getSummaryCountFieldName(),
                    SUMMARY_COUNT_FIELD_ARG, command);
            if (Boolean.TRUE.equals(analysisConfig.getOverlappingBuckets())) {
                Long window = analysisConfig.getResultFinalizationWindow();
                if (window == null) {
                    window = AnalysisConfig.DEFAULT_RESULT_FINALIZATION_WINDOW;
                }
                command.add(RESULT_FINALIZATION_WINDOW_ARG + window);
            }
            if (Boolean.TRUE.equals(analysisConfig.getMultivariateByFields())) {
                command.add(MULTIVARIATE_BY_FIELDS_ARG);
            }

            if (analysisConfig.getUsePerPartitionNormalization()) {
                command.add(PER_PARTITION_NORMALIZATION);
            }
        }

        // Input is always length encoded
        command.add(LENGTH_ENCODED_INPUT_ARG);

        // Limit the number of output records
        command.add(maxAnomalyRecordsArg(settings));

        // always set the time field
        String timeFieldArg = TIME_FIELD_ARG + getTimeFieldOrDefault(job);
        command.add(timeFieldArg);

        int intervalStagger = calculateStaggeringInterval(job.getId());
        logger.debug("Periodic operations staggered by " + intervalStagger +" seconds for job '" + job.getId() + "'");

        // Supply a URL for persisting/restoring model state unless model
        // persistence has been explicitly disabled.
        if (DONT_PERSIST_MODEL_STATE_SETTING.get(settings)) {
            logger.info("Will not persist model state - "  + DONT_PERSIST_MODEL_STATE_SETTING + " setting was set");
        } else {
            // Persist model state every few hours even if the job isn't closed
            long persistInterval = (job.getBackgroundPersistInterval() == null) ?
                    (DEFAULT_BASE_PERSIST_INTERVAL + intervalStagger) :
                        job.getBackgroundPersistInterval().getSeconds();
                    command.add(PERSIST_INTERVAL_ARG + persistInterval);
        }

        int maxQuantileInterval = BASE_MAX_QUANTILE_INTERVAL + intervalStagger;
        command.add(MAX_QUANTILE_INTERVAL_ARG + maxQuantileInterval);

        if (modelConfigFilePresent(env)) {
            String modelConfigFile = XPackPlugin.resolveConfigFile(env, ML_MODEL_CONF).toString();
            command.add(MODEL_CONFIG_ARG + modelConfigFile);
        }

        return command;
    }

    private static String getTimeFieldOrDefault(Job job) {
        DataDescription dataDescription = job.getDataDescription();
        boolean useDefault = dataDescription == null
                || Strings.isNullOrEmpty(dataDescription.getTimeField());
        return useDefault ? DataDescription.DEFAULT_TIME_FIELD : dataDescription.getTimeField();
    }

    private static void addIfNotNull(TimeValue timeValue, String argKey, List<String> command) {
        addIfNotNull(timeValue == null ? null : timeValue.getSeconds(), argKey, command);
    }

    private static void addIfNotNull(List<TimeValue> timeValues, String argKey, List<String> command) {
        if (timeValues != null) {
            addIfNotNull(timeValues.stream().map(TimeValue::getSeconds).collect(Collectors.toList()), argKey, command);
        }
    }

    private static <T> void addIfNotNull(T object, String argKey, List<String> command) {
        if (object != null) {
            String param = argKey + object;
            command.add(param);
        }
    }

    /**
     * Return true if there is a file ES_HOME/config/mlmodel.conf
     */
    public static boolean modelConfigFilePresent(Environment env) {
        Path modelConfPath = XPackPlugin.resolveConfigFile(env, ML_MODEL_CONF);

        return Files.isRegularFile(modelConfPath);
    }

    /**
     * Build the command to start the normalizer process.
     */
    public static List<String> buildNormalizerCommand(Environment env, String jobId, String quantilesState, Integer bucketSpan,
            boolean perPartitionNormalization) throws IOException {

        List<String> command = new ArrayList<>();
        command.add(NORMALIZE_PATH);
        addIfNotNull(bucketSpan, BUCKET_SPAN_ARG, command);
        command.add(LENGTH_ENCODED_INPUT_ARG);
        if (perPartitionNormalization) {
            command.add(PER_PARTITION_NORMALIZATION);
        }

        if (quantilesState != null) {
            Path quantilesStateFilePath = writeNormalizerInitState(jobId, quantilesState, env);

            String stateFileArg = QUANTILES_STATE_PATH_ARG + quantilesStateFilePath;
            command.add(stateFileArg);
            command.add(DELETE_STATE_FILES_ARG);
        }

        if (modelConfigFilePresent(env)) {
            String modelConfigFile = XPackPlugin.resolveConfigFile(env, ML_MODEL_CONF).toString();
            command.add(MODEL_CONFIG_ARG + modelConfigFile);
        }

        return command;
    }

    /**
     * Write the normalizer init state to file.
     */
    public static Path writeNormalizerInitState(String jobId, String state, Environment env)
            throws IOException {
        // createTempFile has a race condition where it may return the same
        // temporary file name to different threads if called simultaneously
        // from multiple threads, hence add the thread ID to avoid this
        Path stateFile = Files.createTempFile(env.tmpFile(), jobId + "_quantiles_" + Thread.currentThread().getId(),
                QUANTILES_FILE_EXTENSION);

        try (BufferedWriter osw = Files.newBufferedWriter(stateFile, StandardCharsets.UTF_8)) {
            osw.write(state);
        }

        return stateFile;
    }
}
