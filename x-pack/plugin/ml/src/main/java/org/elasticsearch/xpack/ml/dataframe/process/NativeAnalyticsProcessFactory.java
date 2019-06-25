/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class NativeAnalyticsProcessFactory implements AnalyticsProcessFactory {

    private static final Logger LOGGER = LogManager.getLogger(NativeAnalyticsProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();
    public static final Duration PROCESS_STARTUP_TIMEOUT = Duration.ofSeconds(10);

    private final Environment env;
    private final NativeController nativeController;

    public NativeAnalyticsProcessFactory(Environment env, NativeController nativeController) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
    }

    @Override
    public AnalyticsProcess createAnalyticsProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig,
                                                   ExecutorService executorService) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, AnalyticsBuilder.ANALYTICS, jobId,
                true, false, true, true, false, false);

        // The extra 2 are for the checksum and the control field
        int numberOfFields = analyticsProcessConfig.cols() + 2;

        createNativeProcess(jobId, analyticsProcessConfig, filesToDelete, processPipes);

        NativeAnalyticsProcess analyticsProcess = new NativeAnalyticsProcess(jobId, processPipes.getLogStream().get(),
                processPipes.getProcessInStream().get(), processPipes.getProcessOutStream().get(), null, numberOfFields,
                filesToDelete, reason -> {});


        try {
            analyticsProcess.start(executorService);
            return analyticsProcess;
        } catch (EsRejectedExecutionException e) {
            try {
                IOUtils.close(analyticsProcess);
            } catch (IOException ioe) {
                LOGGER.error("Can't close data frame analytics process", ioe);
            }
            throw e;
        }
    }

    private void createNativeProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig, List<Path> filesToDelete,
                                     ProcessPipes processPipes) {
        AnalyticsBuilder analyticsBuilder = new AnalyticsBuilder(env, nativeController, processPipes, analyticsProcessConfig,
                filesToDelete);
        try {
            analyticsBuilder.build();
            processPipes.connectStreams(PROCESS_STARTUP_TIMEOUT);
        } catch (IOException e) {
            String msg = "Failed to launch data frame analytics process for job " + jobId;
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}
