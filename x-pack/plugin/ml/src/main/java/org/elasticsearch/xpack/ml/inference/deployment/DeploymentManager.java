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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;
import org.elasticsearch.xpack.ml.job.process.ProcessWorkerExecutorService;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final ThreadPool threadPool;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public DeploymentManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ThreadPool threadPool,
        PyTorchProcessFactory pyTorchProcessFactory
    ) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executorServiceForDeployment = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
    }

    public void startDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> listener) {
        doStartDeployment(task, listener);
    }

    public Optional<ModelStats> getStats(TrainedModelDeploymentTask task) {
        return Optional.ofNullable(processContextByAllocation.get(task.getId())).map(processContext -> {
            var stats = processContext.getResultProcessor().getResultStats();
            return new ModelStats(
                processContext.startTime,
                stats.timingStats(),
                stats.lastUsed(),
                processContext.executorService.queueSize() + stats.numberOfPendingResults(),
                stats.errorCount(),
                processContext.rejectedExecutionCount.intValue(),
                processContext.timeoutCount.intValue(),
                processContext.inferenceThreads,
                processContext.modelThreads,
                stats.peakThroughput(),
                stats.recentStats().requestsProcessed(),
                stats.recentStats().avgInferenceTime()
            );
        });
    }

    // function exposed for testing
    ProcessContext addProcessContext(Long id, ProcessContext processContext) {
        return processContextByAllocation.putIfAbsent(id, processContext);
    }

    private void doStartDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> finalListener) {
        logger.info("[{}] Starting model deployment", task.getModelId());

        ProcessContext processContext = new ProcessContext(task, executorServiceForProcess);
        if (addProcessContext(task.getId(), processContext) != null) {
            finalListener.onFailure(
                ExceptionsHelper.serverError("[{}] Could not create inference process as one already exists", task.getModelId())
            );
            return;
        }

        ActionListener<TrainedModelDeploymentTask> listener = ActionListener.wrap(finalListener::onResponse, failure -> {
            processContextByAllocation.remove(task.getId());
            finalListener.onFailure(failure);
        });

        ActionListener<Boolean> modelLoadedListener = ActionListener.wrap(success -> {
            executorServiceForProcess.execute(() -> processContext.getResultProcessor().process(processContext.process.get()));
            listener.onResponse(task);
        }, listener::onFailure);

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(getModelResponse -> {
            assert getModelResponse.getResources().results().size() == 1;
            TrainedModelConfig modelConfig = getModelResponse.getResources().results().get(0);
            processContext.modelInput.set(modelConfig.getInput());

            assert modelConfig.getInferenceConfig() instanceof NlpConfig;
            NlpConfig nlpConfig = (NlpConfig) modelConfig.getInferenceConfig();
            task.init(nlpConfig);

            SearchRequest searchRequest = vocabSearchRequest(nlpConfig.getVocabularyConfig(), modelConfig.getModelId());
            executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(searchVocabResponse -> {
                if (searchVocabResponse.getHits().getHits().length == 0) {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            Messages.getMessage(
                                Messages.VOCABULARY_NOT_FOUND,
                                task.getModelId(),
                                VocabularyConfig.docId(modelConfig.getModelId())
                            )
                        )
                    );
                    return;
                }

                Vocabulary vocabulary = parseVocabularyDocLeniently(searchVocabResponse.getHits().getAt(0));
                NlpTask nlpTask = new NlpTask(nlpConfig, vocabulary);
                NlpTask.Processor processor = nlpTask.createProcessor();
                processContext.nlpTaskProcessor.set(processor);
                // here, we are being called back on the searching thread, which MAY be a network thread
                // `startAndLoad` creates named pipes, blocking the calling thread, better to execute that in our utility
                // executor.
                executorServiceForDeployment.execute(() -> startAndLoad(processContext, modelConfig.getLocation(), modelLoadedListener));
            }, listener::onFailure));
        }, listener::onFailure);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            GetTrainedModelsAction.INSTANCE,
            new GetTrainedModelsAction.Request(task.getModelId()),
            getModelListener
        );
    }

    private SearchRequest vocabSearchRequest(VocabularyConfig vocabularyConfig, String modelId) {
        return client.prepareSearch(vocabularyConfig.getIndex())
            .setQuery(new IdsQueryBuilder().addIds(VocabularyConfig.docId(modelId)))
            .setSize(1)
            .setTrackTotalHits(false)
            .request();
    }

    Vocabulary parseVocabularyDocLeniently(SearchHit hit) throws IOException {
        try (
            InputStream stream = hit.getSourceRef().streamInput();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
                        .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                    stream
                )
        ) {
            return Vocabulary.createParser(true).apply(parser, null);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("failed to parse trained model vocabulary [{}]", hit.getId()), e);
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
            logger.warn("[{}] No process context to stop", task.getModelId());
        }
    }

    public void infer(
        TrainedModelDeploymentTask task,
        InferenceConfig config,
        Map<String, Object> doc,
        TimeValue timeout,
        ActionListener<InferenceResults> listener
    ) {
        if (task.isStopped()) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    "[{}] is stopping or stopped due to [{}]",
                    task.getModelId(),
                    task.stoppedReason().orElse("")
                )
            );
            return;
        }

        ProcessContext processContext = processContextByAllocation.get(task.getId());
        if (processContext == null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("[{}] process context missing", task.getModelId()));
            return;
        }

        final long requestId = requestIdCounter.getAndIncrement();
        InferenceAction inferenceAction = new InferenceAction(
            task.getModelId(),
            requestId,
            timeout,
            processContext,
            config,
            doc,
            threadPool,
            listener
        );
        try {
            processContext.getExecutorService().execute(inferenceAction);
        } catch (EsRejectedExecutionException e) {
            processContext.getRejectedExecutionCount().incrementAndGet();
            inferenceAction.onFailure(e);
        } catch (Exception e) {
            inferenceAction.onFailure(e);
        }
    }

    static class InferenceAction extends AbstractRunnable implements ActionListener<InferenceResults> {
        private final String modelId;
        private final long requestId;
        private final TimeValue timeout;
        private final Scheduler.Cancellable timeoutHandler;
        private final ProcessContext processContext;
        private final InferenceConfig config;
        private final Map<String, Object> doc;
        private final ActionListener<InferenceResults> listener;
        private final AtomicBoolean notified = new AtomicBoolean();

        InferenceAction(
            String modelId,
            long requestId,
            TimeValue timeout,
            ProcessContext processContext,
            InferenceConfig config,
            Map<String, Object> doc,
            ThreadPool threadPool,
            ActionListener<InferenceResults> listener
        ) {
            this.modelId = modelId;
            this.requestId = requestId;
            this.timeout = timeout;
            this.processContext = processContext;
            this.config = config;
            this.doc = doc;
            this.listener = listener;
            this.timeoutHandler = threadPool.schedule(
                this::onTimeout,
                ExceptionsHelper.requireNonNull(timeout, "timeout"),
                MachineLearning.UTILITY_THREAD_POOL_NAME
            );
        }

        void onTimeout() {
            if (notified.compareAndSet(false, true)) {
                processContext.getTimeoutCount().incrementAndGet();
                processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
                listener.onFailure(
                    new ElasticsearchStatusException("timeout [{}] waiting for inference result", RestStatus.REQUEST_TIMEOUT, timeout)
                );
                return;
            }
            logger.debug("[{}] request [{}] received timeout after [{}] but listener already alerted", modelId, requestId, timeout);
        }

        @Override
        public void onResponse(InferenceResults inferenceResults) {
            onSuccess(inferenceResults);
        }

        void onSuccess(InferenceResults inferenceResults) {
            timeoutHandler.cancel();
            if (notified.compareAndSet(false, true)) {
                listener.onResponse(inferenceResults);
                return;
            }
            logger.debug("[{}] request [{}] received inference response but listener already notified", modelId, requestId);
        }

        @Override
        public void onFailure(Exception e) {
            timeoutHandler.cancel();
            if (notified.compareAndSet(false, true)) {
                processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
                listener.onFailure(e);
                return;
            }
            logger.debug(
                () -> new ParameterizedMessage("[{}] request [{}] received failure but listener already notified", modelId, requestId),
                e
            );
        }

        @Override
        protected void doRun() throws Exception {
            if (notified.get()) {
                // Should not execute request as it has already timed out while waiting in the queue
                logger.debug(
                    () -> new ParameterizedMessage("[{}] skipping inference on request [{}] as it has timed out", modelId, requestId)
                );
                return;
            }

            final String requestIdStr = String.valueOf(requestId);
            try {
                // The request builder expect a list of inputs which are then batched.
                // TODO batching was implemented for expected use-cases such as zero-shot
                // classification but is not used here.
                List<String> text = Collections.singletonList(NlpTask.extractInput(processContext.modelInput.get(), doc));
                NlpTask.Processor processor = processContext.nlpTaskProcessor.get();
                processor.validateInputs(text);
                assert config instanceof NlpConfig;
                NlpConfig nlpConfig = (NlpConfig) config;
                NlpTask.Request request = processor.getRequestBuilder(nlpConfig)
                    .buildRequest(text, requestIdStr, nlpConfig.getTokenization().getTruncate(), nlpConfig.getTokenization().getSpan());
                logger.debug(() -> "Inference Request " + request.processInput().utf8ToString());
                if (request.tokenization().anyTruncated()) {
                    logger.debug("[{}] [{}] input truncated", modelId, requestId);
                }
                processContext.getResultProcessor()
                    .registerRequest(
                        requestIdStr,
                        ActionListener.wrap(
                            inferenceResult -> processResult(
                                inferenceResult,
                                processContext,
                                request.tokenization(),
                                processor.getResultProcessor((NlpConfig) config),
                                this
                            ),
                            this::onFailure
                        )
                    );
                processContext.process.get().writeInferenceRequest(request.processInput());
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] error writing to inference process", processContext.task.getModelId()), e);
                onFailure(ExceptionsHelper.serverError("Error writing to inference process", e));
            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void processResult(
            PyTorchInferenceResult inferenceResult,
            ProcessContext context,
            TokenizationResult tokenization,
            NlpTask.ResultProcessor inferenceResultsProcessor,
            ActionListener<InferenceResults> resultsListener
        ) {
            if (inferenceResult.isError()) {
                resultsListener.onFailure(
                    new ElasticsearchStatusException(
                        "Error in inference process: [" + inferenceResult.getError() + "]",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
                return;
            }

            logger.debug(() -> new ParameterizedMessage("[{}] retrieved result for request [{}]", context.task.getModelId(), requestId));
            if (notified.get()) {
                // The request has timed out. No need to spend cycles processing the result.
                logger.debug(
                    () -> new ParameterizedMessage(
                        "[{}] skipping result processing for request [{}] as the request has timed out",
                        context.task.getModelId(),
                        requestId
                    )
                );
                return;
            }
            InferenceResults results = inferenceResultsProcessor.processResult(tokenization, inferenceResult);
            logger.debug(() -> new ParameterizedMessage("[{}] processed result for request [{}]", context.task.getModelId(), requestId));
            resultsListener.onResponse(results);
        }
    }

    class ProcessContext {

        private final TrainedModelDeploymentTask task;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();
        private final SetOnce<NlpTask.Processor> nlpTaskProcessor = new SetOnce<>();
        private final SetOnce<TrainedModelInput> modelInput = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;
        private final ProcessWorkerExecutorService executorService;
        private volatile Instant startTime;
        private volatile Integer inferenceThreads;
        private volatile Integer modelThreads;
        private final AtomicInteger rejectedExecutionCount = new AtomicInteger();
        private final AtomicInteger timeoutCount = new AtomicInteger();

        ProcessContext(TrainedModelDeploymentTask task, ExecutorService executorService) {
            this.task = Objects.requireNonNull(task);
            resultProcessor = new PyTorchResultProcessor(task.getModelId(), threadSettings -> {
                this.inferenceThreads = threadSettings.inferenceThreads();
                this.modelThreads = threadSettings.modelThreads();
            });
            this.stateStreamer = new PyTorchStateStreamer(client, executorService, xContentRegistry);
            this.executorService = new ProcessWorkerExecutorService(
                threadPool.getThreadContext(),
                "inference process",
                task.getParams().getQueueCapacity()
            );
        }

        PyTorchResultProcessor getResultProcessor() {
            return resultProcessor;
        }

        synchronized void startProcess() {
            process.set(pyTorchProcessFactory.createProcess(task, executorServiceForProcess, onProcessCrash()));
            startTime = Instant.now();
            executorServiceForProcess.submit(executorService::start);
        }

        synchronized void stopProcess() {
            resultProcessor.stop();
            executorService.shutdown();
            try {
                if (process.get() == null) {
                    return;
                }
                stateStreamer.cancel();
                process.get().kill(true);
                processContextByAllocation.remove(task.getId());
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Failed to kill process", task.getModelId()), e);
            } finally {
                if (nlpTaskProcessor.get() != null) {
                    nlpTaskProcessor.get().close();
                }
            }
        }

        private Consumer<String> onProcessCrash() {
            return reason -> {
                logger.error("[{}] inference process crashed due to reason [{}]", task.getModelId(), reason);
                resultProcessor.stop();
                executorService.shutdownWithError(new IllegalStateException(reason));
                processContextByAllocation.remove(task.getId());
                if (nlpTaskProcessor.get() != null) {
                    nlpTaskProcessor.get().close();
                }
                task.setFailed("inference process crashed due to reason [" + reason + "]");
            };
        }

        void loadModel(TrainedModelLocation modelLocation, ActionListener<Boolean> listener) {
            if (modelLocation instanceof IndexLocation) {
                process.get().loadModel(task.getModelId(), ((IndexLocation) modelLocation).getIndexName(), stateStreamer, listener);
            } else {
                throw new IllegalStateException("unsupported trained model location [" + modelLocation.getClass().getSimpleName() + "]");
            }
        }

        // accessor used for mocking in tests
        AtomicInteger getTimeoutCount() {
            return timeoutCount;
        }

        // accessor used for mocking in tests
        ExecutorService getExecutorService() {
            return executorService;
        }

        // accessor used for mocking in tests
        AtomicInteger getRejectedExecutionCount() {
            return rejectedExecutionCount;
        }
    }
}
