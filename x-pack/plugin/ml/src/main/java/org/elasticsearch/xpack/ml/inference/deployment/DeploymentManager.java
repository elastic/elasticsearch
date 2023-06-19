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
import org.elasticsearch.tasks.CancellableTask;
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
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
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
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;

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
    private final int maxProcesses;
    private final InferenceAuditor auditor;

    public DeploymentManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ThreadPool threadPool,
        PyTorchProcessFactory pyTorchProcessFactory,
        int maxProcesses,
        InferenceAuditor auditor
    ) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executorServiceForDeployment = threadPool.executor(UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME);
        this.maxProcesses = maxProcesses;
        this.auditor = Objects.requireNonNull(auditor);
    }

    public Optional<ModelStats> getStats(TrainedModelDeploymentTask task) {
        return Optional.ofNullable(processContextByAllocation.get(task.getId())).map(processContext -> {
            var stats = processContext.getResultProcessor().getResultStats();
            var recentStats = stats.recentStats();
            return new ModelStats(
                processContext.startTime,
                stats.timingStats().getCount(),
                stats.timingStats().getAverage(),
                stats.timingStatsExcludingCacheHits().getAverage(),
                stats.lastUsed(),
                processContext.priorityProcessWorker.queueSize() + stats.numberOfPendingResults(),
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

    public void startDeployment(TrainedModelDeploymentTask task, ActionListener<TrainedModelDeploymentTask> finalListener) {
        logger.info("[{}] Starting model deployment", task.getDeploymentId());

        if (processContextByAllocation.size() >= maxProcesses) {
            finalListener.onFailure(
                ExceptionsHelper.serverError(
                    "[{}] Could not start inference process as the node reached the max number [{}] of processes",
                    task.getDeploymentId(),
                    maxProcesses
                )
            );
            return;
        }

        ProcessContext processContext = new ProcessContext(task);
        if (addProcessContext(task.getId(), processContext) != null) {
            finalListener.onFailure(
                ExceptionsHelper.serverError("[{}] Could not create inference process as one already exists", task.getDeploymentId())
            );
            return;
        }

        ActionListener<TrainedModelDeploymentTask> failedDeploymentListener = ActionListener.wrap(finalListener::onResponse, failure -> {
            ProcessContext failedContext = processContextByAllocation.remove(task.getId());
            if (failedContext != null) {
                failedContext.stopProcess();
            }
            finalListener.onFailure(failure);
        });

        ActionListener<Boolean> modelLoadedListener = ActionListener.wrap(success -> {
            executorServiceForProcess.execute(() -> processContext.getResultProcessor().process(processContext.process.get()));
            finalListener.onResponse(task);
        }, failedDeploymentListener::onFailure);

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(getModelResponse -> {
            assert getModelResponse.getResources().results().size() == 1;
            TrainedModelConfig modelConfig = getModelResponse.getResources().results().get(0);
            processContext.modelInput.set(modelConfig.getInput());

            if (modelConfig.getInferenceConfig() instanceof NlpConfig nlpConfig) {
                task.init(nlpConfig);

                SearchRequest searchRequest = vocabSearchRequest(nlpConfig.getVocabularyConfig(), modelConfig.getModelId());
                executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(searchVocabResponse -> {
                    if (searchVocabResponse.getHits().getHits().length == 0) {
                        failedDeploymentListener.onFailure(
                            new ResourceNotFoundException(
                                Messages.getMessage(
                                    Messages.VOCABULARY_NOT_FOUND,
                                    modelConfig.getModelId(),
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
                    executorServiceForDeployment.execute(() -> processContext.startAndLoad(modelConfig.getLocation(), modelLoadedListener));
                }, failedDeploymentListener::onFailure));
            } else {
                failedDeploymentListener.onFailure(
                    new IllegalArgumentException(
                        format(
                            "[%s] must be a pytorch model; found inference config of kind [%s]",
                            modelConfig.getModelId(),
                            modelConfig.getInferenceConfig().getWriteableName()
                        )
                    )
                );
            }
        }, failedDeploymentListener::onFailure);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            GetTrainedModelsAction.INSTANCE,
            new GetTrainedModelsAction.Request(task.getParams().getModelId()),
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

    public void stopDeployment(TrainedModelDeploymentTask task) {
        ProcessContext processContext = processContextByAllocation.remove(task.getId());
        if (processContext != null) {
            logger.info("[{}] Stopping deployment, reason [{}]", task.getDeploymentId(), task.stoppedReason().orElse("unknown"));
            processContext.stopProcess();
        } else {
            logger.warn("[{}] No process context to stop", task.getDeploymentId());
        }
    }

    public void infer(
        TrainedModelDeploymentTask task,
        InferenceConfig config,
        NlpInferenceInput input,
        boolean skipQueue,
        TimeValue timeout,
        CancellableTask parentActionTask,
        ActionListener<InferenceResults> listener
    ) {
        var processContext = getProcessContext(task, listener::onFailure);
        if (processContext == null) {
            // error reporting handled in the call to getProcessContext
            return;
        }

        final long requestId = requestIdCounter.getAndIncrement();
        InferencePyTorchAction inferenceAction = new InferencePyTorchAction(
            task.getDeploymentId(),
            requestId,
            timeout,
            processContext,
            config,
            input,
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
            task.getDeploymentId(),
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
            task.getDeploymentId(),
            requestId,
            timeout,
            processContext,
            threadPool,
            ActionListener.wrap(b -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
        );

        executePyTorchAction(processContext, PriorityProcessWorkerExecutorService.RequestPriority.HIGHEST, controlMessageAction);
    }

    void executePyTorchAction(
        ProcessContext processContext,
        PriorityProcessWorkerExecutorService.RequestPriority priority,
        AbstractPyTorchAction<?> action
    ) {
        try {
            processContext.getPriorityProcessWorker().executeWithPriority(action, priority, action.getRequestId());
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
                    task.getDeploymentId(),
                    task.stoppedReason().orElse("")
                )
            );
            return null;
        }

        ProcessContext processContext = processContextByAllocation.get(task.getId());
        if (processContext == null) {
            errorConsumer.accept(ExceptionsHelper.conflictStatusException("[{}] process context missing", task.getDeploymentId()));
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
        private final PriorityProcessWorkerExecutorService priorityProcessWorker;
        private volatile Instant startTime;
        private volatile Integer numThreadsPerAllocation;
        private volatile Integer numAllocations;
        private final AtomicInteger rejectedExecutionCount = new AtomicInteger();
        private final AtomicInteger timeoutCount = new AtomicInteger();
        private volatile boolean isStopped;

        ProcessContext(TrainedModelDeploymentTask task) {
            this.task = Objects.requireNonNull(task);
            resultProcessor = new PyTorchResultProcessor(task.getDeploymentId(), threadSettings -> {
                this.numThreadsPerAllocation = threadSettings.numThreadsPerAllocation();
                this.numAllocations = threadSettings.numAllocations();
            });
            // We want to use the inference thread pool to load the model as it is a possibly long operation
            // and knowing it is an inference thread would enable better understanding during debugging.
            // Even though we account for 3 threads per process in the thread pool, loading the model
            // happens before we start input/output so it should be ok to use a thread from that pool for loading
            // the model.
            this.stateStreamer = new PyTorchStateStreamer(client, executorServiceForProcess, xContentRegistry);
            this.priorityProcessWorker = new PriorityProcessWorkerExecutorService(
                threadPool.getThreadContext(),
                "inference process",
                task.getParams().getQueueCapacity()
            );
        }

        PyTorchResultProcessor getResultProcessor() {
            return resultProcessor;
        }

        synchronized void startAndLoad(TrainedModelLocation modelLocation, ActionListener<Boolean> loadedListener) {
            assert Thread.currentThread().getName().contains(UTILITY_THREAD_POOL_NAME)
                : format("Must execute from [%s] but thread is [%s]", UTILITY_THREAD_POOL_NAME, Thread.currentThread().getName());

            if (isStopped) {
                logger.debug("[{}] model stopped before it is started", task.getDeploymentId());
                loadedListener.onFailure(new IllegalArgumentException("model stopped before it is started"));
                return;
            }

            logger.debug("[{}] start and load", task.getDeploymentId());
            process.set(pyTorchProcessFactory.createProcess(task, executorServiceForProcess, this::onProcessCrash));
            startTime = Instant.now();
            logger.debug("[{}] process started", task.getDeploymentId());
            try {
                loadModel(modelLocation, ActionListener.wrap(success -> {
                    if (isStopped) {
                        logger.debug("[{}] model loaded but process is stopped", task.getDeploymentId());
                        killProcessIfPresent();
                        loadedListener.onFailure(new IllegalStateException("model loaded but process is stopped"));
                        return;
                    }

                    logger.debug("[{}] model loaded, starting priority process worker thread", task.getDeploymentId());
                    startPriorityProcessWorker();
                    loadedListener.onResponse(success);
                }, loadedListener::onFailure));
            } catch (Exception e) {
                loadedListener.onFailure(e);
            }
        }

        void startPriorityProcessWorker() {
            executorServiceForProcess.submit(priorityProcessWorker::start);
        }

        synchronized void stopProcess() {
            isStopped = true;
            resultProcessor.stop();
            stateStreamer.cancel();
            priorityProcessWorker.shutdown();
            killProcessIfPresent();
            if (nlpTaskProcessor.get() != null) {
                nlpTaskProcessor.get().close();
            }
        }

        private void killProcessIfPresent() {
            try {
                if (process.get() == null) {
                    return;
                }
                process.get().kill(true);
            } catch (IOException e) {
                logger.error(() -> "[" + task.getDeploymentId() + "] Failed to kill process", e);
            }
        }

        private void onProcessCrash(String reason) {
            String msg = "inference process crashed due to reason [" + reason + "]";
            logger.error("[{}] {}", task.getDeploymentId(), msg);
            processContextByAllocation.remove(task.getId());
            isStopped = true;
            resultProcessor.stop();
            stateStreamer.cancel();
            priorityProcessWorker.shutdownWithError(new IllegalStateException(reason));
            if (nlpTaskProcessor.get() != null) {
                nlpTaskProcessor.get().close();
            }
            task.setFailed(msg);
            auditor.error(task.getDeploymentId(), "inference process crashed");
        }

        void loadModel(TrainedModelLocation modelLocation, ActionListener<Boolean> listener) {
            if (isStopped) {
                listener.onFailure(new IllegalArgumentException("Process has stopped, model loading canceled"));
                return;
            }
            if (modelLocation instanceof IndexLocation indexLocation) {
                // Loading the model happens on the inference thread pool but when we get the callback
                // we need to return to the utility thread pool to avoid leaking the thread we used.
                process.get()
                    .loadModel(
                        task.getParams().getModelId(),
                        indexLocation.getIndexName(),
                        stateStreamer,
                        ActionListener.wrap(
                            r -> executorServiceForDeployment.submit(() -> listener.onResponse(r)),
                            e -> executorServiceForDeployment.submit(() -> listener.onFailure(e))
                        )
                    );
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
        PriorityProcessWorkerExecutorService getPriorityProcessWorker() {
            return priorityProcessWorker;
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
