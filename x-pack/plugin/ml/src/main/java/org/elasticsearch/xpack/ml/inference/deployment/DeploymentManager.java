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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentState;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentTaskState;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch.ModelStorage;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class DeploymentManager {

    private static final Logger logger = LogManager.getLogger(DeploymentManager.class);

    private final Client client;
    private final PyTorchProcessFactory pyTorchProcessFactory;
    private final ExecutorService executorServiceForDeployment;
    private final ExecutorService executorServiceForProcess;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public DeploymentManager(Client client, ThreadPool threadPool, PyTorchProcessFactory pyTorchProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.executorServiceForDeployment = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
    }

    public void startDeployment(TrainedModelDeploymentTask task) {
        executorServiceForDeployment.execute(() -> doStartDeployment(task));
    }

    private void doStartDeployment(TrainedModelDeploymentTask task) {
        logger.debug("[{}] Starting model deployment", task.getModelId());

        ProcessContext processContext = new ProcessContext(task.getModelId());

        if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
            throw ExceptionsHelper.serverError("[{}] Could not create process as one already exists", task.getModelId());
        }

        loadModelStorage(task.getModelId(), ActionListener.wrap(
            modelStorage -> {
                processContext.startProcess();
                try {
                    processContext.loadModel(modelStorage);
                } catch (Exception e) {
                    failTask(task, e);
                    return;
                }

                executorServiceForProcess.execute(() -> processContext.resultProcessor.process(processContext.process.get()));

                TrainedModelDeploymentTaskState startedState = new TrainedModelDeploymentTaskState(
                    TrainedModelDeploymentState.STARTED, task.getAllocationId(), null);
                task.updatePersistentTaskState(startedState, ActionListener.wrap(
                    response -> logger.info("[{}] trained model deployment started", task.getModelId()),
                    task::markAsFailed
                ));
            },
            e -> failTask(task, e)
        ));
    }

    public void stopDeployment(TrainedModelDeploymentTask task) {
        ProcessContext processContext;
        synchronized (processContextByAllocation) {
            processContext = processContextByAllocation.get(task.getAllocationId());
        }
        if (processContext != null) {
            logger.debug("[{}] Stopping deployment", task.getModelId());
            processContext.stopProcess();
        } else {
            logger.debug("[{}] No process context to stop", task.getModelId());
        }
    }

    public void infer(TrainedModelDeploymentTask task, double[] inputs, ActionListener<PyTorchResult> listener) {
        ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
        executorServiceForProcess.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                try {
                    String requestId = processContext.process.get().writeInferenceRequest(inputs);
                    waitForResult(processContext, requestId, listener);
                } catch (IOException e) {
                    logger.error(new ParameterizedMessage("[{}] error writing to process", processContext.modelId), e);
                    onFailure(ExceptionsHelper.serverError("error writing to process", e));
                }
            }
        });
    }

    private void waitForResult(ProcessContext processContext, String requestId, ActionListener<PyTorchResult> listener) {
        try {
            // TODO the timeout value should come from the action
            TimeValue timeout = TimeValue.timeValueSeconds(5);
            PyTorchResult pyTorchResult = processContext.resultProcessor.waitForResult(requestId, timeout);
            if (pyTorchResult == null) {
                listener.onFailure(new ElasticsearchStatusException("timeout [{}] waiting for inference result",
                    RestStatus.TOO_MANY_REQUESTS, timeout));
            } else {
                listener.onResponse(pyTorchResult);
            }
        } catch (InterruptedException e) {
            listener.onFailure(e);
        }
    }

    private void failTask(TrainedModelDeploymentTask task, Exception e) {
        logger.error(new ParameterizedMessage("[{}] failing model deployment task with error: ", task.getModelId()), e);
        task.markAsFailed(e);
    }

    private void loadModelStorage(String modelId, ActionListener<ModelStorage> listener) {

        String docId = ModelStorage.docIdFromModelId(modelId);

        client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(docId))
            .execute(ActionListener.wrap(
                searchResponse -> {
                    if (searchResponse.getHits().getHits().length == 0) {
                        logger.error("found no model storage for model [{}]", modelId);
                        listener.onResponse(null);
                        return;
                    }
                    ModelStorage ms = parseModelStorageLenientlyFromSource(
                        searchResponse.getHits().getHits()[0].getSourceRef());
                    listener.onResponse(ms);
                },
                listener::onFailure
            ));
    }

    private ModelStorage parseModelStorageLenientlyFromSource(BytesReference source) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return ModelStorage.LENIENT_PARSER.apply(parser, null);
        }
    }

    class ProcessContext {

        private final String modelId;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;

        ProcessContext(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
            resultProcessor = new PyTorchResultProcessor(modelId);
            this.stateStreamer = new PyTorchStateStreamer(client);
        }

        synchronized void startProcess() {
            process.set(pyTorchProcessFactory.createProcess(modelId, executorServiceForProcess, onProcessCrash()));
        }

        synchronized void stopProcess() {
            resultProcessor.stop();
            if (process.get() == null) {
                return;
            }
            try {
                stateStreamer.cancel();
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

        void loadModel(ModelStorage storage) throws IOException {
            process.get().loadModel(stateStreamer, storage);
        }
    }

}
