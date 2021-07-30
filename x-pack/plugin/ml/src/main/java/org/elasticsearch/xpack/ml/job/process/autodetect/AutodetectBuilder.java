/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.ScheduledEventToRuleWriter;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessBuilderUtils;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

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
import java.util.Set;
import java.util.stream.Collectors;

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
    public static final String DELETE_STATE_FILES_ARG = "--deleteStateFiles";
    public static final String LENGTH_ENCODED_INPUT_ARG = "--lengthEncodedInput";
    public static final String MODEL_CONFIG_ARG = "--modelconfig=";
    public static final String QUANTILES_STATE_PATH_ARG = "--quantilesState=";
    public static final String LICENSE_KEY_VALIDATED_ARG = "--validElasticLicenseKeyConfirmed=";

    private static final String JSON_EXTENSION = ".json";
    private static final String CONFIG_ARG = "--config=";
    private static final String EVENTS_CONFIG_ARG = "--eventsconfig=";
    private static final String FILTERS_CONFIG_ARG = "--filtersconfig=";

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
    public void build() throws IOException, InterruptedException {

        List<String> command = buildAutodetectCommand();

        buildFiltersConfig(command);
        buildScheduledEventsConfig(command);
        buildJobConfig(command);

        buildQuantiles(command);

        processPipes.addArgs(command);
        controller.startProcess(command);
    }

    /**
     * Visible for testing
     */
    List<String> buildAutodetectCommand() {
        List<String> command = new ArrayList<>();
        command.add(AUTODETECT_PATH);

        // Input is always length encoded
        command.add(LENGTH_ENCODED_INPUT_ARG);

        // Limit the number of output records
        command.add(maxAnomalyRecordsArg(settings));

        if (ProcessBuilderUtils.modelConfigFilePresent(env)) {
            String modelConfigFile = XPackPlugin.resolveConfigFile(env, ProcessBuilderUtils.ML_MODEL_CONF).toString();
            command.add(MODEL_CONFIG_ARG + modelConfigFile);
        }

        // License has been created by the open job action
        command.add(LICENSE_KEY_VALIDATED_ARG + true);

        return command;
    }

    static String maxAnomalyRecordsArg(Settings settings) {
        return "--maxAnomalyRecords=" + MAX_ANOMALY_RECORDS_SETTING_DYNAMIC.get(settings);
    }

    private void buildQuantiles(List<String> command) throws IOException {
        if (quantiles != null && quantiles.getQuantileState().isEmpty() == false) {
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

    private void buildScheduledEventsConfig(List<String> command) throws IOException {
        if (scheduledEvents.isEmpty()) {
            return;
        }
        Path eventsConfigFile = Files.createTempFile(env.tmpFile(), "eventsConfig", JSON_EXTENSION);
        filesToDelete.add(eventsConfigFile);

        List<ScheduledEventToRuleWriter> scheduledEventToRuleWriters = scheduledEvents.stream()
            .map(x -> new ScheduledEventToRuleWriter(x.getDescription(), x.toDetectionRule(job.getAnalysisConfig().getBucketSpan())))
            .collect(Collectors.toList());

        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(eventsConfigFile),StandardCharsets.UTF_8);
             XContentBuilder jsonBuilder = JsonXContent.contentBuilder()) {
            osw.write(Strings.toString(
                jsonBuilder.startObject()
                    .field(ScheduledEvent.RESULTS_FIELD.getPreferredName(), scheduledEventToRuleWriters)
                    .endObject()));
        }

        command.add(EVENTS_CONFIG_ARG + eventsConfigFile.toString());
    }

    private void buildJobConfig(List<String> command) throws IOException {
        Path configFile = Files.createTempFile(env.tmpFile(), "config", JSON_EXTENSION);
        filesToDelete.add(configFile);
        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(configFile),StandardCharsets.UTF_8);
            XContentBuilder jsonBuilder = JsonXContent.contentBuilder()) {

            job.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            osw.write(Strings.toString(jsonBuilder));
        }

        command.add(CONFIG_ARG + configFile.toString());
    }

    private void buildFiltersConfig(List<String> command) throws IOException {
        if (referencedFilters.isEmpty()) {
            return;
        }
        Path filtersConfigFile = Files.createTempFile(env.tmpFile(), "filtersConfig", JSON_EXTENSION);
        filesToDelete.add(filtersConfigFile);

        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(filtersConfigFile),StandardCharsets.UTF_8);
             XContentBuilder jsonBuilder = JsonXContent.contentBuilder()) {
            osw.write(Strings.toString(
                jsonBuilder.startObject()
                    .field(MlFilter.RESULTS_FIELD.getPreferredName(), referencedFilters)
                    .endObject()));
        }
        command.add(FILTERS_CONFIG_ARG + filtersConfigFile.toString());
    }
}
