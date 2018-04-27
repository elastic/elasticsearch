/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.AbstractNativeProcessBuilder;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.ProcessPipes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * The categorize process builder.
 */
public class CategorizeBuilder extends AbstractNativeProcessBuilder {

    /**
     * Constructs a categorize process builder
     *
     * @param job           The job configuration
     * @param filesToDelete This method will append File objects that need to be
     *                      deleted when the process completes
     * @param logger        The job's logger
     */
    public CategorizeBuilder(Job job, List<Path> filesToDelete, Logger logger, Environment env, Settings settings,
                             NativeController controller, ProcessPipes processPipes) {
        super(job, filesToDelete, logger, env, settings, controller, processPipes);
    }

    /**
     * Requests that the controller daemon start a categorize process.
     */
    public void build() throws IOException {

        List<String> command = ProcessCtrl.buildCategorizeCommand(env, settings, job, logger);

        buildLimits(command);

        processPipes.addArgs(command);
        controller.startProcess(command);
    }
}
