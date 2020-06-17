/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class NativeNormalizerProcessFactory implements NormalizerProcessFactory {

    private static final Logger LOGGER = LogManager.getLogger(NativeNormalizerProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private final AtomicLong counter;
    private volatile Duration processConnectTimeout;

    public NativeNormalizerProcessFactory(Environment env, NativeController nativeController, ClusterService clusterService) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.counter = new AtomicLong(0);
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(env.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.PROCESS_CONNECT_TIMEOUT,
            this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public NormalizerProcess createNormalizerProcess(String jobId, String quantilesState, Integer bucketSpan,
                                                     ExecutorService executorService) {
        // The job ID passed to the process pipes is only used to make the file names unique.  Since normalize can get run many times
        // in quick succession for the same job the job ID alone is not sufficient to guarantee that the normalizer process pipe names
        // are unique.  Therefore an increasing counter value is appended to the job ID to ensure uniqueness between calls.
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, NormalizerBuilder.NORMALIZE,
            jobId + "_" + counter.incrementAndGet(), false, true, true, false, false);
        createNativeProcess(jobId, quantilesState, processPipes, bucketSpan);

        NativeNormalizerProcess normalizerProcess = new NativeNormalizerProcess(jobId, nativeController, processPipes,
            processConnectTimeout);

        try {
            normalizerProcess.start(executorService);
            return normalizerProcess;
        } catch (IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to normalizer for job " + jobId;
            LOGGER.error(msg);
            try {
                IOUtils.close(normalizerProcess);
            } catch (IOException ioe) {
                LOGGER.error("Can't close normalizer", ioe);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    private void createNativeProcess(String jobId, String quantilesState, ProcessPipes processPipes, Integer bucketSpan) {

        try {
            List<String> command = new NormalizerBuilder(env, jobId, quantilesState, bucketSpan).build();
            processPipes.addArgs(command);
            nativeController.startProcess(command);
        } catch (IOException e) {
            String msg = "Failed to launch normalizer for job " + jobId;
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}

