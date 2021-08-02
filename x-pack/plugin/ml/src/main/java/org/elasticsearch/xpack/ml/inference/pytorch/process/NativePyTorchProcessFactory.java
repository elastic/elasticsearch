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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
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
import java.util.function.Consumer;

public class NativePyTorchProcessFactory implements PyTorchProcessFactory {

    private static final Logger logger = LogManager.getLogger(NativePyTorchProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private final ClusterService clusterService;
    private volatile Duration processConnectTimeout;

    public NativePyTorchProcessFactory(Environment env,
                                       NativeController nativeController,
                                       ClusterService clusterService) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.clusterService = Objects.requireNonNull(clusterService);
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(env.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.PROCESS_CONNECT_TIMEOUT,
            this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public NativePyTorchProcess createProcess(String modelId, ExecutorService executorService, Consumer<String> onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(
            env,
            NAMED_PIPE_HELPER,
            processConnectTimeout,
            PyTorchBuilder.PROCESS_NAME,
            modelId,
            null,
            false,
            true,
            true,
            true,
            false
        );

        executeProcess(processPipes, filesToDelete);

        NativePyTorchProcess process = new NativePyTorchProcess(modelId, nativeController, processPipes, 0, filesToDelete, onProcessCrash);

        try {
            process.start(executorService);
        } catch(IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to pytorch process for job " + modelId;
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

    private void executeProcess(ProcessPipes processPipes, List<Path> filesToDelete) {
        PyTorchBuilder pyTorchBuilder = new PyTorchBuilder(env::tmpFile, nativeController, processPipes, filesToDelete);
        try {
            pyTorchBuilder.build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while launching PyTorch process");
        } catch (IOException e) {
            String msg = "Failed to launch PyTorch process";
            logger.error(msg);
            throw ExceptionsHelper.serverError(msg + " on [" + clusterService.getNodeName() + "]", e);
        }
    }

}
