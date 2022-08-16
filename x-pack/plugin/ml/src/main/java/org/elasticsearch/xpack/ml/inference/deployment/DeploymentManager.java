/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
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
import org.elasticsearch.xpack.ml.inference.pytorch.PriorityProcessWorkerExecutorService;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
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
        this.executorServiceForProcess = threadPool.executor(MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME);
    }

    public void startDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> listener) {
        doStartDeployment(task, listener);
    }

    public Optional<ModelStats> getStats(TrainedModelDeploymentTask task) {
        return Optional.ofNullable(processContextByAllocation.get(task.getId())).map(processContext -> {
            var stats = processContext.getResultProcessor().getResultStats();
            var recentStats = stats.recentStats();
            return new ModelStats(
                processContext.startTime,
                stats.timingStats(),
                stats.lastUsed(),
                processContext.executorService.queueSize() + stats.numberOfPendingResults(),
                stats.errorCount(),
                stats.cacheHitCount(),
                processContext.rejectedExecutionCount.intValue(),
                processContext.timeoutCount.intValue(),
                processContext.numThreadsPerAllocation,
                processContext.numAllocations,
                stats.peakThroughput(),
                recentStats.requestsProcessed(),
                recentStats.avgInferenceTime(),
                recentStats.cacheHitCount()
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

            if (modelConfig.getInferenceConfig()instanceof NlpConfig nlpConfig) {
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
                    executorServiceForDeployment.execute(
                        () -> startAndLoad(processContext, modelConfig.getLocation(), modelLoadedListener)
                    );
                }, listener::onFailure));
            } else {
                listener.onFailure(
                    new IllegalArgumentException(
                        format(
                            "[%s] must be a pytorch model; found inference config of kind [%s]",
                            modelConfig.getModelId(),
                            modelConfig.getInferenceConfig().getWriteableName()
                        )
                    )
                );
            }
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
            return Vocabulary.PARSER.apply(parser, null);
        } catch (IOException e) {
            logger.error(() -> "failed to parse trained model vocabulary [" + hit.getId() + "]", e);
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
            logger.info("[{}] Stopping deployment, reason [{}]", task.getModelId(), task.stoppedReason().orElse("unknown"));
            processContext.stopProcess();
        } else {
            logger.warn("[{}] No process context to stop", task.getModelId());
        }
    }

    public void infer(
        TrainedModelDeploymentTask task,
        InferenceConfig config,
        Map<String, Object> doc,
        boolean skipQueue,
        TimeValue timeout,
        Task parentActionTask,
        ActionListener<InferenceResults> listener
    ) {
        var processContext = getProcessContext(task, listener::onFailure);
        if (processContext == null) {
            // error reporting handled in the call to getProcessContext
            return;
        }

        final long requestId = requestIdCounter.getAndIncrement();
        InferencePyTorchAction inferenceAction = new InferencePyTorchAction(
            task.getModelId(),
            requestId,
            timeout,
            processContext,
            config,
            doc,
            threadPool,
            parentActionTask,
            listener
        );

        PriorityProcessWorkerExecutorService.RequestPriority priority = skipQueue
            ? PriorityProcessWorkerExecutorService.RequestPriority.HIGH
            : PriorityProcessWorkerExecutorService.RequestPriority.NORMAL;

        executePyTorchAction(processContext, priority, inferenceAction);
    }

    public void updateNumAllocations(
        TrainedModelDeploymentTask task,
        int numAllocationThreads,
        TimeValue timeout,
        ActionListener<ThreadSettings> listener
    ) {
        var processContext = getProcessContext(task, listener::onFailure);
        if (processContext == null) {
            // error reporting handled in the call to getProcessContext
            return;
        }

        final long requestId = requestIdCounter.getAndIncrement();
        ThreadSettingsControlMessagePytorchAction controlMessageAction = new ThreadSettingsControlMessagePytorchAction(
            task.getModelId(),
            requestId,
            numAllocationThreads,
            timeout,
            processContext,
            threadPool,
            listener
        );

        executePyTorchAction(processContext, PriorityProcessWorkerExecutorService.RequestPriority.HIGHEST, controlMessageAction);
    }

    public void clearCache(TrainedModelDeploymentTask task, TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        var processContext = getProcessContext(task, listener::onFailure);
        if (processContext == null) {
            // error reporting handled in the call to getProcessContext
            return;
        }

        final long requestId = requestIdCounter.getAndIncrement();
        ClearCacheControlMessagePytorchAction controlMessageAction = new ClearCacheControlMessagePytorchAction(
            task.getModelId(),
            requestId,
            timeout,
            processContext,
            threadPool,
            ActionListener.wrap(b -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
        );

        executePyTorchAction(processContext, PriorityProcessWorkerExecutorService.RequestPriority.HIGHEST, controlMessageAction);
    }

    public void executePyTorchAction(
        ProcessContext processContext,
        PriorityProcessWorkerExecutorService.RequestPriority priority,
        AbstractPyTorchAction<?> action
    ) {
        try {
            processContext.getExecutorService().executeWithPriority(action, priority, action.getRequestId());
        } catch (EsRejectedExecutionException e) {
            processContext.getRejectedExecutionCount().incrementAndGet();
            action.onFailure(e);
        } catch (Exception e) {
            action.onFailure(e);
        }
    }

    private ProcessContext getProcessContext(TrainedModelDeploymentTask task, Consumer<Exception> errorConsumer) {
        if (task.isStopped()) {
            errorConsumer.accept(
                ExceptionsHelper.conflictStatusException(
                    "[{}] is stopping or stopped due to [{}]",
                    task.getModelId(),
                    task.stoppedReason().orElse("")
                )
            );
            return null;
        }

        ProcessContext processContext = processContextByAllocation.get(task.getId());
        if (processContext == null) {
            errorConsumer.accept(ExceptionsHelper.conflictStatusException("[{}] process context missing", task.getModelId()));
            return null;
        }

        return processContext;
    }

    class ProcessContext {

        private final TrainedModelDeploymentTask task;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();
        private final SetOnce<NlpTask.Processor> nlpTaskProcessor = new SetOnce<>();
        private final SetOnce<TrainedModelInput> modelInput = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;
        private final PriorityProcessWorkerExecutorService executorService;
        private volatile Instant startTime;
        private volatile Integer numThreadsPerAllocation;
        private volatile Integer numAllocations;
        private final AtomicInteger rejectedExecutionCount = new AtomicInteger();
        private final AtomicInteger timeoutCount = new AtomicInteger();

        ProcessContext(TrainedModelDeploymentTask task, ExecutorService executorService) {
            this.task = Objects.requireNonNull(task);
            resultProcessor = new PyTorchResultProcessor(task.getModelId(), threadSettings -> {
                this.numThreadsPerAllocation = threadSettings.numThreadsPerAllocation();
                this.numAllocations = threadSettings.numAllocations();
            });
            this.stateStreamer = new PyTorchStateStreamer(client, executorService, xContentRegistry);
            this.executorService = new PriorityProcessWorkerExecutorService(
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
                logger.error(() -> "[" + task.getModelId() + "] Failed to kill process", e);
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
            if (modelLocation instanceof IndexLocation indexLocation) {
                process.get().loadModel(task.getModelId(), indexLocation.getIndexName(), stateStreamer, listener);
            } else {
                listener.onFailure(
                    new IllegalStateException("unsupported trained model location [" + modelLocation.getClass().getSimpleName() + "]")
                );
            }
        }

        // accessor used for mocking in tests
        AtomicInteger getTimeoutCount() {
            return timeoutCount;
        }

        // accessor used for mocking in tests
        PriorityProcessWorkerExecutorService getExecutorService() {
            return executorService;
        }

        // accessor used for mocking in tests
        AtomicInteger getRejectedExecutionCount() {
            return rejectedExecutionCount;
        }

        SetOnce<TrainedModelInput> getModelInput() {
            return modelInput;
        }

        SetOnce<PyTorchProcess> getProcess() {
            return process;
        }

        SetOnce<NlpTask.Processor> getNlpTaskProcessor() {
            return nlpTaskProcessor;
        }
    }
}
