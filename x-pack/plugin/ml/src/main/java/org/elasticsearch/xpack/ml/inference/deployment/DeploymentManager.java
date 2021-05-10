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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentState;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentTaskState;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.NlpPipeline;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.PipelineConfig;
import org.elasticsearch.xpack.ml.inference.pytorch.process.NativePyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DeploymentManager {

    private static final Logger logger = LogManager.getLogger(DeploymentManager.class);
    private static final AtomicLong requestIdCounter = new AtomicLong(1);

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final PyTorchProcessFactory pyTorchProcessFactory;
    private final ExecutorService executorServiceForDeployment;
    private final ExecutorService executorServiceForProcess;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public DeploymentManager(Client client, NamedXContentRegistry xContentRegistry,
                             ThreadPool threadPool, PyTorchProcessFactory pyTorchProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.executorServiceForDeployment = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
    }

    public void startDeployment(TrainedModelDeploymentTask task) {
        executorServiceForDeployment.execute(() -> doStartDeployment(task));
    }

    private void doStartDeployment(TrainedModelDeploymentTask task) {
        logger.debug("[{}] Starting model deployment", task.getModelId());

        ProcessContext processContext = new ProcessContext(task.getModelId(), task.getIndex(), executorServiceForProcess);

        if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
            throw ExceptionsHelper.serverError("[{}] Could not create process as one already exists", task.getModelId());
        }

        ActionListener<Boolean> modelLoadedListener = ActionListener.wrap(
            success -> {
                executorServiceForProcess.execute(() -> processContext.resultProcessor.process(processContext.process.get()));

                TrainedModelDeploymentTaskState startedState = new TrainedModelDeploymentTaskState(
                    TrainedModelDeploymentState.STARTED, task.getAllocationId(), null);
                task.updatePersistentTaskState(startedState, ActionListener.wrap(
                    response -> logger.info("[{}] trained model loaded", task.getModelId()),
                    task::markAsFailed
                ));
            },
            e -> failTask(task, e)
        );

        ActionListener<SearchResponse> configListener = ActionListener.wrap(
            searchResponse -> {
                logger.info("search response");
                if (searchResponse.getHits().getHits().length == 0) {
                    failTask(task, new ResourceNotFoundException(
                        Messages.getMessage(Messages.PIPELINE_CONFIG_NOT_FOUND, task.getModelId())));
                    return;
                }

                PipelineConfig config = parseModelDefinitionDocLeniently(searchResponse.getHits().getAt(0));
                NlpPipeline pipeline = NlpPipeline.fromConfig(config);
                logger.info("loaded pipeline");
                processContext.pipeline.set(pipeline);
                processContext.startProcess();
                processContext.loadModel(modelLoadedListener);

            },
            e -> failTask(task, e)
        );

        logger.info("looking for config " + PipelineConfig.documentId(task.getModelId()));
        SearchRequest searchRequest = pipelineConfigSearchRequest(task.getModelId(), task.getIndex());
        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, configListener);
    }

    private SearchRequest pipelineConfigSearchRequest(String modelId, String index) {
        return client.prepareSearch(index)
            .setQuery(new IdsQueryBuilder().addIds(PipelineConfig.documentId(modelId)))
            .setSize(1)
            .setTrackTotalHits(false)
            .request();
    }


    public PipelineConfig parseModelDefinitionDocLeniently(SearchHit hit) throws IOException {

        try (InputStream stream = hit.getSourceRef().streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return PipelineConfig.fromXContent(parser, true);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("failed to parse pipeline config [{}]", hit.getId()), e);
            throw e;
        }
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

    public void infer(TrainedModelDeploymentTask task, String inputs, ActionListener<InferenceResults> listener) {
        ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());

        final String requestId = String.valueOf(requestIdCounter.getAndIncrement());

        executorServiceForProcess.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                try {
                    NlpPipeline.Processor processor = processContext.pipeline.get().createProcessor();

                    logger.info("tokenizing input [{}]",  inputs);
                    BytesReference request = processor.getRequestBuilder().buildRequest(inputs, requestId);
                    logger.info("Inference Request "+ request.utf8ToString());
                    processContext.process.get().writeInferenceRequest(request);

                    waitForResult(processContext, requestId, processor.getResultProcessor(), listener);
                } catch (IOException e) {
                    logger.error(new ParameterizedMessage("[{}] error writing to process", processContext.modelId), e);
                    onFailure(ExceptionsHelper.serverError("error writing to process", e));
                }
            }
        });
    }

    private void waitForResult(ProcessContext processContext,
                               String requestId,
                               NlpPipeline.ResultProcessor inferenceResultsProcessor,
                               ActionListener<InferenceResults> listener) {
        try {
            // TODO the timeout value should come from the action
            TimeValue timeout = TimeValue.timeValueSeconds(5);
            PyTorchResult pyTorchResult = processContext.resultProcessor.waitForResult(requestId, timeout);
            if (pyTorchResult == null) {
                listener.onFailure(new ElasticsearchStatusException("timeout [{}] waiting for inference result",
                    RestStatus.TOO_MANY_REQUESTS, timeout));
            } else {
                listener.onResponse(inferenceResultsProcessor.processResult(pyTorchResult));
            }
        } catch (InterruptedException e) {
            listener.onFailure(e);
        }
    }

    private void failTask(TrainedModelDeploymentTask task, Exception e) {
        logger.error(new ParameterizedMessage("[{}] failing model deployment task with error: ", task.getModelId()), e);
        task.markAsFailed(e);
    }

    class ProcessContext {

        private final String modelId;
        private final String index;
        private final SetOnce<NativePyTorchProcess> process = new SetOnce<>();
        private final SetOnce<NlpPipeline> pipeline = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;

        ProcessContext(String modelId, String index, ExecutorService executorService) {
            this.modelId = Objects.requireNonNull(modelId);
            this.index = Objects.requireNonNull(index);
            resultProcessor = new PyTorchResultProcessor(modelId);
            this.stateStreamer = new PyTorchStateStreamer(client, executorService, xContentRegistry);
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
            return reason -> logger.error("[{}] process crashed due to reason [{}]", modelId, reason);
        }

        void loadModel(ActionListener<Boolean> listener) {
            process.get().loadModel(modelId, index, stateStreamer, listener);
        }
    }
}
