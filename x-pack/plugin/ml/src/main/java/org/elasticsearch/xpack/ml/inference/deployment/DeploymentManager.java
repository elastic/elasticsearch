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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.pytorch.process.NativePyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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

    public void startDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> listener) {
        doStartDeployment(task, listener);
    }

    public ModelStats getStats(TrainedModelDeploymentTask task) {
        ProcessContext processContext = processContextByAllocation.get(task.getId());
        if (processContext == null) {
            throw new IllegalStateException("[" + task.getModelId() + "] process context missing for stats");
        }

        Long modelSizeBytes = processContext.getModelSizeBytes() < 0 ? null : (long) processContext.getModelSizeBytes();
        return new ModelStats(processContext.resultProcessor.getTimingStats(),
            processContext.resultProcessor.getLastUsed(),
            modelSizeBytes);
    }

    private void doStartDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> finalListener) {
        logger.debug("[{}] Starting model deployment", task.getModelId());

        ProcessContext processContext = new ProcessContext(task.getModelId(), executorServiceForProcess, task.getId());

        if (processContextByAllocation.putIfAbsent(task.getId(), processContext) != null) {
            finalListener.onFailure(ExceptionsHelper.serverError("[{}] Could not create process as one already exists", task.getModelId()));
            return;
        }

        ActionListener<TrainedModelDeploymentTask> listener = ActionListener.wrap(
            finalListener::onResponse,
            failure -> {
                processContextByAllocation.remove(task.getId());
                finalListener.onFailure(failure);
            }
        );

        ActionListener<Boolean> modelLoadedListener = ActionListener.wrap(
            success -> {
                executorServiceForProcess.execute(() -> processContext.resultProcessor.process(processContext.process.get()));
                listener.onResponse(task);
            },
            listener::onFailure
        );

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(
            getModelResponse -> {
                assert getModelResponse.getResources().results().size() == 1;
                TrainedModelConfig modelConfig = getModelResponse.getResources().results().get(0);
                processContext.modelInput.set(modelConfig.getInput());

                assert modelConfig.getInferenceConfig() instanceof NlpConfig;
                NlpConfig nlpConfig = (NlpConfig) modelConfig.getInferenceConfig();

                SearchRequest searchRequest = vocabSearchRequest(nlpConfig.getVocabularyConfig());
                executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
                    searchVocabResponse -> {
                        if (searchVocabResponse.getHits().getHits().length == 0) {
                            listener.onFailure(new ResourceNotFoundException(Messages.getMessage(
                                Messages.VOCABULARY_NOT_FOUND, task.getModelId(), nlpConfig.getVocabularyConfig().getId())));
                            return;
                        }

                        Vocabulary vocabulary = parseVocabularyDocLeniently(searchVocabResponse.getHits().getAt(0));
                        NlpTask nlpTask = new NlpTask(nlpConfig, vocabulary);
                        NlpTask.Processor processor = nlpTask.createProcessor();
                        processContext.nlpTaskProcessor.set(processor);
                        // here, we are being called back on the searching thread, which MAY be a network thread
                        // `startAndLoad` creates named pipes, blocking the calling thread, better to execute that in our utility
                        // executor.
                        executorServiceForDeployment.execute(
                            () -> startAndLoad(processContext,  modelConfig.getLocation(), modelLoadedListener));
                    },
                    listener::onFailure
                ));
            },
            listener::onFailure
        );

        executeAsyncWithOrigin(client, ML_ORIGIN, GetTrainedModelsAction.INSTANCE, new GetTrainedModelsAction.Request(task.getModelId()),
            getModelListener);
    }

    private SearchRequest vocabSearchRequest(VocabularyConfig vocabularyConfig) {
        return client.prepareSearch(vocabularyConfig.getIndex())
            .setQuery(new IdsQueryBuilder().addIds(vocabularyConfig.getId()))
            .setSize(1)
            .setTrackTotalHits(false)
            .request();
    }

    Vocabulary parseVocabularyDocLeniently(SearchHit hit) throws IOException {
        try (InputStream stream = hit.getSourceRef().streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return Vocabulary.createParser(true).apply(parser, null);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("failed to parse vocabulary [{}]", hit.getId()), e);
            throw e;
        }
    }

    private void startAndLoad(ProcessContext processContext, TrainedModelLocation modelLocation, ActionListener<Boolean> loadedListener) {
        try {
            processContext.startProcess();
            processContext.loadModel(modelLocation, loadedListener);
        } catch (Exception e) {
            loadedListener.onFailure(e);
        }
    }

    public void stopDeployment(TrainedModelDeploymentTask task) {
        ProcessContext processContext;
        synchronized (processContextByAllocation) {
            processContext = processContextByAllocation.get(task.getId());
        }
        if (processContext != null) {
            logger.info("[{}] Stopping deployment", task.getModelId());
            processContext.stopProcess();
        } else {
            logger.debug("[{}] No process context to stop", task.getModelId());
        }
    }

    public void infer(TrainedModelDeploymentTask task,
                      Map<String, Object> doc, TimeValue timeout,
                      ActionListener<InferenceResults> listener) {
        if (task.isStopped()) {
            listener.onFailure(
                new IllegalStateException("["
                    + task.getModelId()
                    + "] is stopping or stopped due to ["
                    + task.stoppedReason().orElse("")
                    + "]"
                )
            );
            return;
        }

        ProcessContext processContext = processContextByAllocation.get(task.getId());
        if (processContext == null) {
            listener.onFailure(new IllegalStateException("[" + task.getModelId() + "] process context missing"));
            return;
        }

        final String requestId = String.valueOf(requestIdCounter.getAndIncrement());

        executorServiceForProcess.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                try {
                    String text = NlpTask.extractInput(processContext.modelInput.get(), doc);
                    NlpTask.Processor processor = processContext.nlpTaskProcessor.get();
                    processor.validateInputs(text);
                    NlpTask.Request request = processor.getRequestBuilder().buildRequest(text, requestId);
                    logger.trace(() -> "Inference Request "+ request.processInput.utf8ToString());
                    PyTorchResultProcessor.PendingResult pendingResult = processContext.resultProcessor.registerRequest(requestId);
                    processContext.process.get().writeInferenceRequest(request.processInput);
                    waitForResult(processContext, pendingResult, request.tokenization, requestId, timeout, processor.getResultProcessor(),
                        listener);
                } catch (IOException e) {
                    logger.error(new ParameterizedMessage("[{}] error writing to process", processContext.modelId), e);
                    onFailure(ExceptionsHelper.serverError("error writing to process", e));
                } catch (Exception e) {
                    onFailure(e);
                } finally {
                    processContext.resultProcessor.requestAccepted(requestId);
                }
            }
        });
    }

    private void waitForResult(ProcessContext processContext,
                               PyTorchResultProcessor.PendingResult pendingResult,
                               BertTokenizer.TokenizationResult tokenization,
                               String requestId,
                               TimeValue timeout,
                               NlpTask.ResultProcessor inferenceResultsProcessor,
                               ActionListener<InferenceResults> listener) {
        try {
            PyTorchResult pyTorchResult = processContext.resultProcessor.waitForResult(
                processContext.process.get(),
                requestId,
                pendingResult,
                timeout
            );
            if (pyTorchResult == null) {
                listener.onFailure(new ElasticsearchStatusException("timeout [{}] waiting for inference result",
                    RestStatus.TOO_MANY_REQUESTS, timeout));
                return;
            }

            if (pyTorchResult.isError()) {
                listener.onFailure(new ElasticsearchStatusException(pyTorchResult.getError(),
                    RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            logger.debug(() -> new ParameterizedMessage("[{}] retrieved result for request [{}]", processContext.modelId, requestId));
            InferenceResults results = inferenceResultsProcessor.processResult(tokenization, pyTorchResult);
            logger.debug(() -> new ParameterizedMessage("[{}] processed result for request [{}]", processContext.modelId, requestId));
            listener.onResponse(results);
        } catch (InterruptedException e) {
            listener.onFailure(e);
        }
    }

    class ProcessContext {

        private final String modelId;
        private final long taskId;
        private final SetOnce<NativePyTorchProcess> process = new SetOnce<>();
        private final SetOnce<NlpTask.Processor> nlpTaskProcessor = new SetOnce<>();
        private final SetOnce<TrainedModelInput> modelInput = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;

        ProcessContext(String modelId, ExecutorService executorService, long taskId) {
            this.modelId = Objects.requireNonNull(modelId);
            resultProcessor = new PyTorchResultProcessor(modelId);
            this.stateStreamer = new PyTorchStateStreamer(client, executorService, xContentRegistry);
            this.taskId = taskId;
        }

        /**
         * A value of -1 means the size is unknown. Most likely
         * because the model has not been loaded yet or the load
         * failed.
         * @return size in bytes or -1
         */
        int getModelSizeBytes() {
            return stateStreamer.getModelSize();
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
                processContextByAllocation.remove(taskId);
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Failed to kill process", modelId), e);
            }
        }

        private Consumer<String> onProcessCrash() {
            return reason -> {
                logger.error("[{}] process crashed due to reason [{}]", modelId, reason);
                processContextByAllocation.remove(taskId);
            };
        }

        void loadModel(TrainedModelLocation modelLocation, ActionListener<Boolean> listener) {
            if (modelLocation instanceof IndexLocation) {
                process.get().loadModel(modelId, ((IndexLocation) modelLocation).getIndexName(), stateStreamer, listener);
            } else {
                throw new IllegalStateException("unsupported trained model location [" + modelLocation.getClass().getSimpleName() + "]");
            }
        }
    }
}
