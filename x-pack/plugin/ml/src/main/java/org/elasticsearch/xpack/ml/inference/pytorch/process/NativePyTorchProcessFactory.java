/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class NativePyTorchProcessFactory implements PyTorchProcessFactory {

    private static final Logger logger = LogManager.getLogger(NativePyTorchProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private final String nodeName;
    private volatile Duration processConnectTimeout;

    public NativePyTorchProcessFactory(Environment env, NativeController nativeController, ClusterService clusterService) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.nodeName = clusterService.getNodeName();
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(env.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearning.PROCESS_CONNECT_TIMEOUT, this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public NativePyTorchProcess createProcess(
        TrainedModelDeploymentTask task,
        ExecutorService executorService,
        Consumer<String> onProcessCrash
    ) {
        ProcessPipes processPipes = new ProcessPipes(
            env,
            NAMED_PIPE_HELPER,
            processConnectTimeout,
            PyTorchBuilder.PROCESS_NAME,
            task.getModelId(),
            null,
            false,
            true,
            true,
            true,
            false // We do not need a persist pipe. This is also why we use 3 threads per model assignment in the pytorch thread pool.
        );

        executeProcess(processPipes, task);

        NativePyTorchProcess process = new NativePyTorchProcess(
            task.getModelId(),
            nativeController,
            processPipes,
            0,
            Collections.emptyList(),
            onProcessCrash
        );

        try {
            process.start(executorService);
        } catch (IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to pytorch process for job " + task.getModelId();
            logger.error(msg);
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                logger.error("Can't close pytorch process", ioe);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
        return process;
    }

    private void executeProcess(ProcessPipes processPipes, TrainedModelDeploymentTask task) {
        PyTorchBuilder pyTorchBuilder = new PyTorchBuilder(
            nativeController,
            processPipes,
            task.getParams().getThreadsPerAllocation(),
            task.getParams().getNumberOfAllocations(),
            task.getParams().getCacheSizeBytes()
        );
        try {
            pyTorchBuilder.build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while launching PyTorch process");
        } catch (IOException e) {
            String msg = "Failed to launch PyTorch process";
            logger.error(msg);
            throw ExceptionsHelper.serverError(msg + " on [" + nodeName + "]", e);
        }
    }

}
