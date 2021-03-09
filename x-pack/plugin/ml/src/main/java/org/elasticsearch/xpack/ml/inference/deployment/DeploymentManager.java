/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class DeploymentManager {

    private static final Logger logger = LogManager.getLogger(DeploymentManager.class);

    private final PyTorchProcessFactory pyTorchProcessFactory;
    private final ExecutorService executorServiceForProcess;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public DeploymentManager(ThreadPool threadPool, PyTorchProcessFactory pyTorchProcessFactory) {
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
    }

    public void deployModel(DeployTrainedModelTask task) {
        logger.info("[{}] Deploying model", task.getModelId());

        ProcessContext processContext = new ProcessContext(task.getModelId());

        if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
            throw ExceptionsHelper.serverError("[{}] Could not create process as one already exists", task.getModelId());
        }

        processContext.startProcess();
    }

    public void undeployModel(DeployTrainedModelTask task) {
        ProcessContext processContext;
        synchronized (processContextByAllocation) {
            processContext = processContextByAllocation.get(task.getAllocationId());
        }
        if (processContext != null) {
            logger.debug("[{}] Undeploying model", task.getModelId());
            processContext.killProcess();
        } else {
            logger.debug("[{}] No process context to stop", task.getModelId());
        }
    }

    class ProcessContext {

        private final String modelId;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();

        ProcessContext(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        synchronized void startProcess() {
            process.set(pyTorchProcessFactory.createProcess(modelId, executorServiceForProcess, onProcessCrash()));
        }

        synchronized void killProcess() {
            if (process.get() == null) {
                return;
            }
            try {
                process.get().kill(true);
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Failed to kill process", modelId), e);
            }
        }

        private Consumer<String> onProcessCrash() {
            return reason -> {
                logger.error("[{}] process crashed due to reason [{}]", modelId, reason);
            };
        }
    }
}
