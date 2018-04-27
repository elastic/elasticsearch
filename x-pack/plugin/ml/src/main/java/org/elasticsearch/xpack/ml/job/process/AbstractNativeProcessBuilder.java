/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.writer.AnalysisLimitsWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public abstract class AbstractNativeProcessBuilder {
    protected static final String CONF_EXTENSION = ".conf";

    private static final String LIMIT_CONFIG_ARG = "--limitconfig=";

    protected final Job job;
    protected final Logger logger;
    protected final Environment env;
    protected final Settings settings;
    protected final NativeController controller;
    protected final ProcessPipes processPipes;
    protected final List<Path> filesToDelete;

    protected AbstractNativeProcessBuilder(Job job, List<Path> filesToDelete, Logger logger, Environment env, Settings settings,
                                           NativeController controller, ProcessPipes processPipes) {
        this.job = Objects.requireNonNull(job);
        this.filesToDelete = Objects.requireNonNull(filesToDelete);
        this.logger = Objects.requireNonNull(logger);
        this.env = env;
        this.settings = settings;
        this.controller = controller;
        this.processPipes = processPipes;
    }

    protected void buildLimits(List<String> command) throws IOException {
        if (job.getAnalysisLimits() != null) {
            Path limitConfigFile = Files.createTempFile(env.tmpFile(), "limitconfig", CONF_EXTENSION);
            filesToDelete.add(limitConfigFile);
            writeLimits(job.getAnalysisLimits(), limitConfigFile);
            String limits = LIMIT_CONFIG_ARG + limitConfigFile.toString();
            command.add(limits);
        }
    }

    /**
     * Write the limit config options to <code>emptyConfFile</code>.
     */
    private static void writeLimits(AnalysisLimits options, Path emptyConfFile) throws IOException {

        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(emptyConfFile), StandardCharsets.UTF_8)) {
            new AnalysisLimitsWriter(options, osw).write();
        }
    }
}
