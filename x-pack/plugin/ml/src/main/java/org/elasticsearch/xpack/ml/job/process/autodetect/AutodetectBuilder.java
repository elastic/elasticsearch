/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessBuilderUtils;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.AnalysisLimitsWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.FieldConfigWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.ModelPlotConfigWriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.xpack.ml.job.process.ProcessBuilderUtils.addIfNotNull;

/**
 * The autodetect process builder.
 */
public class AutodetectBuilder {

    /**
     * Autodetect API native program name - always loaded from the same directory as the controller process
     */
    public static final String AUTODETECT = "autodetect";
    static final String AUTODETECT_PATH = "./" + AUTODETECT;

    /*
     * Arguments used by both autodetect and normalize
     */
    public static final String BUCKET_SPAN_ARG = "--bucketspan=";
    public static final String DELETE_STATE_FILES_ARG = "--deleteStateFiles";
    public static final String LENGTH_ENCODED_INPUT_ARG = "--lengthEncodedInput";
    public static final String MODEL_CONFIG_ARG = "--modelconfig=";
    public static final String QUANTILES_STATE_PATH_ARG = "--quantilesState=";

    private static final String CONF_EXTENSION = ".conf";
    static final String JOB_ID_ARG = "--jobid=";
    private static final String LIMIT_CONFIG_ARG = "--limitconfig=";
    private static final String MODEL_PLOT_CONFIG_ARG = "--modelplotconfig=";
    private static final String FIELD_CONFIG_ARG = "--fieldconfig=";
    static final String LATENCY_ARG = "--latency=";
    static final String MULTIVARIATE_BY_FIELDS_ARG = "--multivariateByFields";
    static final String PERSIST_INTERVAL_ARG = "--persistInterval=";
    static final String MAX_QUANTILE_INTERVAL_ARG = "--maxQuantileInterval=";
    static final String SUMMARY_COUNT_FIELD_ARG = "--summarycountfield=";
    static final String TIME_FIELD_ARG = "--timefield=";
    static final String STOP_CATEGORIZATION_ON_WARN_ARG = "--stopCategorizationOnWarnStatus";

    /**
     * Name of the config setting containing the path to the logs directory
     */
    private static final int DEFAULT_MAX_NUM_RECORDS = 500;

    /**
     * The maximum number of anomaly records that will be written each bucket
     */
    // Though this setting is dynamic, it is only set when a new job is opened. So, already running jobs will not get the updated value.
    public static final Setting<Integer> MAX_ANOMALY_RECORDS_SETTING_DYNAMIC = Setting.intSetting("xpack.ml.max_anomaly_records",
            DEFAULT_MAX_NUM_RECORDS, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Config setting storing the flag that disables model persistence
     */
    public static final Setting<Boolean> DONT_PERSIST_MODEL_STATE_SETTING = Setting.boolSetting("no.model.state.persist", false,
            Setting.Property.NodeScope);

    private static final int SECONDS_IN_HOUR = 3600;

    /**
     * Roughly how often should the C++ process persist state?  A staggering
     * factor that varies by job is added to this.
     */
    private static final long DEFAULT_BASE_PERSIST_INTERVAL = 10800; // 3 hours

    /**
     * Roughly how often should the C++ process output quantiles when no
     * anomalies are being detected?  A staggering factor that varies by job is
     * added to this.
     */
    static final int BASE_MAX_QUANTILE_INTERVAL = 21600; // 6 hours

    /**
     * Persisted quantiles are written to disk so they can be read by
     * the autodetect program.  All quantiles files have this extension.
     */
    private static final String QUANTILES_FILE_EXTENSION = ".json";

    private final Job job;
    private final List<Path> filesToDelete;
    private final Logger logger;
    private final Environment env;
    private final Settings settings;
    private final NativeController controller;
    private final ProcessPipes processPipes;
    private Set<MlFilter> referencedFilters;
    private List<ScheduledEvent> scheduledEvents;
    private Quantiles quantiles;

    /**
     * Constructs an autodetect process builder
     *
     * @param job           The job configuration
     * @param filesToDelete This method will append File objects that need to be
     *                      deleted when the process completes
     * @param logger        The job's logger
     */
    public AutodetectBuilder(Job job, List<Path> filesToDelete, Logger logger, Environment env, Settings settings,
                             NativeController controller, ProcessPipes processPipes) {
        this.env = env;
        this.settings = settings;
        this.controller = controller;
        this.processPipes = processPipes;
        this.job = Objects.requireNonNull(job);
        this.filesToDelete = Objects.requireNonNull(filesToDelete);
        this.logger = Objects.requireNonNull(logger);
        referencedFilters = new HashSet<>();
        scheduledEvents = Collections.emptyList();
    }

    public AutodetectBuilder referencedFilters(Set<MlFilter> filters) {
        referencedFilters = filters;
        return this;
    }

    /**
     * Set quantiles to restore the normalizer state if any.
     *
     * @param quantiles the quantiles
     */
    public AutodetectBuilder quantiles(Quantiles quantiles) {
        this.quantiles = quantiles;
        return this;
    }

    public AutodetectBuilder scheduledEvents(List<ScheduledEvent> scheduledEvents) {
        this.scheduledEvents = scheduledEvents;
        return this;
    }

    /**
     * Requests that the controller daemon start an autodetect process.
     */
    public void build() throws IOException {

        List<String> command = buildAutodetectCommand();

        buildLimits(command);
        buildModelPlotConfig(command);

        buildQuantiles(command);
        buildFieldConfig(command);
        processPipes.addArgs(command);
        controller.startProcess(command);
    }

    /**
     * Visible for testing
     */
    List<String> buildAutodetectCommand() {
        List<String> command = new ArrayList<>();
        command.add(AUTODETECT_PATH);

        command.add(JOB_ID_ARG + job.getId());

        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        if (analysisConfig != null) {
            addIfNotNull(analysisConfig.getBucketSpan(), BUCKET_SPAN_ARG, command);
            addIfNotNull(analysisConfig.getLatency(), LATENCY_ARG, command);
            addIfNotNull(analysisConfig.getSummaryCountFieldName(), SUMMARY_COUNT_FIELD_ARG, command);
            if (Boolean.TRUE.equals(analysisConfig.getMultivariateByFields())) {
                command.add(MULTIVARIATE_BY_FIELDS_ARG);
            }
            if (Boolean.TRUE.equals(analysisConfig.getPerPartitionCategorizationConfig().isStopOnWarn())) {
                command.add(STOP_CATEGORIZATION_ON_WARN_ARG);
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
        logger.debug("[{}] Periodic operations staggered by {} seconds", job.getId(), intervalStagger);

        // Supply a URL for persisting/restoring model state unless model
        // persistence has been explicitly disabled.
        if (DONT_PERSIST_MODEL_STATE_SETTING.get(settings)) {
            logger.info("[{}] Will not persist model state - {} setting was set", job.getId(), DONT_PERSIST_MODEL_STATE_SETTING);
        } else {
            // Persist model state every few hours even if the job isn't closed
            long persistInterval = (job.getBackgroundPersistInterval() == null) ?
                    (DEFAULT_BASE_PERSIST_INTERVAL + intervalStagger) :
                    job.getBackgroundPersistInterval().getSeconds();
            command.add(PERSIST_INTERVAL_ARG + persistInterval);
        }

        int maxQuantileInterval = BASE_MAX_QUANTILE_INTERVAL + intervalStagger;
        command.add(MAX_QUANTILE_INTERVAL_ARG + maxQuantileInterval);

        if (ProcessBuilderUtils.modelConfigFilePresent(env)) {
            String modelConfigFile = XPackPlugin.resolveConfigFile(env, ProcessBuilderUtils.ML_MODEL_CONF).toString();
            command.add(MODEL_CONFIG_ARG + modelConfigFile);
        }

        return command;
    }

    static String maxAnomalyRecordsArg(Settings settings) {
        return "--maxAnomalyRecords=" + MAX_ANOMALY_RECORDS_SETTING_DYNAMIC.get(settings);
    }

    private static String getTimeFieldOrDefault(Job job) {
        DataDescription dataDescription = job.getDataDescription();
        boolean useDefault = dataDescription == null
                || Strings.isNullOrEmpty(dataDescription.getTimeField());
        return useDefault ? DataDescription.DEFAULT_TIME_FIELD : dataDescription.getTimeField();
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

    private void buildLimits(List<String> command) throws IOException {
        if (job.getAnalysisLimits() != null) {
            Path limitConfigFile = Files.createTempFile(env.tmpFile(), "limitconfig", CONF_EXTENSION);
            filesToDelete.add(limitConfigFile);
            writeLimits(job.getAnalysisLimits(), limitConfigFile);
            String limits = LIMIT_CONFIG_ARG + limitConfigFile.toString();
            command.add(limits);
        }
    }

    /**
     * Write the Ml autodetect model options to <code>emptyConfFile</code>.
     */
    private static void writeLimits(AnalysisLimits options, Path emptyConfFile) throws IOException {

        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(emptyConfFile), StandardCharsets.UTF_8)) {
            new AnalysisLimitsWriter(options, osw).write();
        }
    }

    private void buildModelPlotConfig(List<String> command) throws IOException {
        if (job.getModelPlotConfig() != null) {
            Path modelPlotConfigFile = Files.createTempFile(env.tmpFile(), "modelplotconfig", CONF_EXTENSION);
            filesToDelete.add(modelPlotConfigFile);
            writeModelPlotConfig(job.getModelPlotConfig(), modelPlotConfigFile);
            String modelPlotConfig = MODEL_PLOT_CONFIG_ARG + modelPlotConfigFile.toString();
            command.add(modelPlotConfig);
        }
    }

    private static void writeModelPlotConfig(ModelPlotConfig config, Path emptyConfFile)
            throws IOException {
        try (OutputStreamWriter osw = new OutputStreamWriter(
                Files.newOutputStream(emptyConfFile),
                StandardCharsets.UTF_8)) {
            new ModelPlotConfigWriter(config, osw).write();
        }
    }

    private void buildQuantiles(List<String> command) throws IOException {
        if (quantiles != null && !quantiles.getQuantileState().isEmpty()) {
            logger.info("Restoring quantiles for job '" + job.getId() + "'");

            Path normalizersStateFilePath = writeNormalizerInitState(job.getId(), quantiles.getQuantileState(), env);

            String quantilesStateFileArg = QUANTILES_STATE_PATH_ARG + normalizersStateFilePath;
            command.add(quantilesStateFileArg);
            command.add(DELETE_STATE_FILES_ARG);
        }
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

    private void buildFieldConfig(List<String> command) throws IOException {
        if (job.getAnalysisConfig() != null) {
            // write to a temporary field config file
            Path fieldConfigFile = Files.createTempFile(env.tmpFile(), "fieldconfig", CONF_EXTENSION);
            filesToDelete.add(fieldConfigFile);
            try (OutputStreamWriter osw = new OutputStreamWriter(
                    Files.newOutputStream(fieldConfigFile),
                    StandardCharsets.UTF_8)) {
                new FieldConfigWriter(job.getAnalysisConfig(), referencedFilters, scheduledEvents, osw, logger).write();
            }

            String fieldConfig = FIELD_CONFIG_ARG + fieldConfigFile.toString();
            command.add(fieldConfig);
        }
    }
}
