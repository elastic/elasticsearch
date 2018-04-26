/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.AbstractNativeProcessBuilder;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.ProcessPipes;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.FieldConfigWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.ModelPlotConfigWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The autodetect process builder.
 */
public class AutodetectBuilder extends AbstractNativeProcessBuilder {
    private static final String MODEL_PLOT_CONFIG_ARG = "--modelplotconfig=";
    private static final String FIELD_CONFIG_ARG = "--fieldconfig=";

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
        super(job, filesToDelete, logger, env, settings, controller, processPipes);
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

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job, logger);

        buildLimits(command);
        buildModelPlotConfig(command);

        buildQuantiles(command);
        buildFieldConfig(command);
        processPipes.addArgs(command);
        controller.startProcess(command);
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

    private static void writeModelPlotConfig(ModelPlotConfig config, Path emptyConfFile) throws IOException {
        try (OutputStreamWriter osw = new OutputStreamWriter(
                Files.newOutputStream(emptyConfFile),
                StandardCharsets.UTF_8)) {
            new ModelPlotConfigWriter(config, osw).write();
        }
    }

    private void buildQuantiles(List<String> command) throws IOException {
        if (quantiles != null && !quantiles.getQuantileState().isEmpty()) {
            logger.info("Restoring quantiles for job '" + job.getId() + "'");

            Path normalizersStateFilePath = ProcessCtrl.writeNormalizerInitState(
                    job.getId(), quantiles.getQuantileState(), env);

            String quantilesStateFileArg = ProcessCtrl.QUANTILES_STATE_PATH_ARG + normalizersStateFilePath;
            command.add(quantilesStateFileArg);
            command.add(ProcessCtrl.DELETE_STATE_FILES_ARG);
        }
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
