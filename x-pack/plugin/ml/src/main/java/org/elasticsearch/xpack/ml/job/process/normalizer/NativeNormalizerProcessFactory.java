/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class NativeNormalizerProcessFactory implements NormalizerProcessFactory {

    private static final Logger LOGGER = Loggers.getLogger(NativeNormalizerProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();
    private static final Duration PROCESS_STARTUP_TIMEOUT = Duration.ofSeconds(10);

    private final Environment env;
    private final Settings settings;
    private final NativeController nativeController;

    public NativeNormalizerProcessFactory(Environment env, Settings settings, NativeController nativeController) {
        this.env = Objects.requireNonNull(env);
        this.settings = Objects.requireNonNull(settings);
        this.nativeController = Objects.requireNonNull(nativeController);
    }

    @Override
    public NormalizerProcess createNormalizerProcess(String jobId, String quantilesState, Integer bucketSpan,
                                                     ExecutorService executorService) {
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, NormalizerBuilder.NORMALIZE, jobId,
                true, false, true, true, false, false);
        createNativeProcess(jobId, quantilesState, processPipes, bucketSpan);

        return new NativeNormalizerProcess(jobId, settings, processPipes.getLogStream().get(),
                processPipes.getProcessInStream().get(), processPipes.getProcessOutStream().get(), executorService);
    }

    private void createNativeProcess(String jobId, String quantilesState, ProcessPipes processPipes, Integer bucketSpan) {

        try {
            List<String> command = new NormalizerBuilder(env, jobId, quantilesState, bucketSpan).build();
            processPipes.addArgs(command);
            nativeController.startProcess(command);
            processPipes.connectStreams(PROCESS_STARTUP_TIMEOUT);
        } catch (IOException e) {
            String msg = "Failed to launch normalizer for job " + jobId;
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}

