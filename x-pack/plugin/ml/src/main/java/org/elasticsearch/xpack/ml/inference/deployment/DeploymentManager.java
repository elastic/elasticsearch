/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlPlatformArchitecturesUtil;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    public static final int NUM_RESTART_ATTEMPTS = 3;

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final PyTorchProcessFactory pyTorchProcessFactory;
    private final ExecutorService executorServiceForDeployment;
    private final ExecutorService executorServiceForProcess;
    private final ThreadPool threadPool;
    private final InferenceAuditor inferenceAuditor;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();
    private final int maxProcesses;

    public DeploymentManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ThreadPool threadPool,
        PyTorchProcessFactory pyTorchProcessFactory,
        int maxProcesses,
        InferenceAuditor inferenceAuditor
    ) {
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.inferenceAuditor = Objects.requireNonNull(inferenceAuditor);
        this.executorServiceForDeployment = threadPool.executor(UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME);
        this.maxProcesses = maxProcesses;
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
        startDeployment(task, null, finalListener);
    }

    public void startDeployment(
        TrainedModelDeploymentTask task,
        Integer startsCount,
        ActionListener<TrainedModelDeploymentTask> finalListener
    ) {
        logger.info("[{}] Starting model deployment of model [{}]", task.getDeploymentId(), task.getModelId());

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

        ProcessContext processContext = new ProcessContext(task, startsCount);
        if (addProcessContext(task.getId(), processContext) != null) {
            finalListener.onFailure(
                ExceptionsHelper.serverError("[{}] Could not create inference process as one already exists", task.getDeploymentId())
            );
            return;
        }

        ActionListener<TrainedModelDeploymentTask> failedDeploymentListener = ActionListener.wrap(finalListener::onResponse, failure -> {
            ProcessContext failedContext = processContextByAllocation.remove(task.getId());
            if (failedContext != null) {
                failedContext.forcefullyStopProcess();
            }
            finalListener.onFailure(failure);
        });

        ActionListener<Boolean> modelLoadedListener = ActionListener.wrap(success -> {
            executorServiceForProcess.execute(() -> processContext.getResultProcessor().process(processContext.process.get()));
            finalListener.onResponse(task);
        }, failedDeploymentListener::onFailure);

        ActionListener<TrainedModelConfig> getVerifiedModel = ActionListener.wrap((modelConfig) -> {
            processContext.modelInput.set(modelConfig.getInput());
            processContext.prefixes.set(modelConfig.getPrefixStrings());

            if (modelConfig.getInferenceConfig() instanceof NlpConfig nlpConfig) {
                task.init(nlpConfig);

                SearchRequest searchRequest = vocabSearchRequest(nlpConfig.getVocabularyConfig(), modelConfig.getModelId());
                executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    TransportSearchAction.TYPE,
                    searchRequest,
                    ActionListener.wrap(searchVocabResponse -> {
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
                        executorServiceForDeployment.execute(new AbstractRunnable() {

                            @Override
                            public void onFailure(Exception e) {
                                failedDeploymentListener.onFailure(e);
                            }

                            @Override
                            protected void doRun() {
                                processContext.startAndLoad(modelConfig.getLocation(), modelLoadedListener);
                            }
                        });
                    }, failedDeploymentListener::onFailure)
                );
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

        ActionListener<GetTrainedModelsAction.Response> verifyModelAndClusterArchitecturesListener = ActionListener.wrap(
            getModelResponse -> {
                assert getModelResponse.getResources().results().size() == 1;
                TrainedModelConfig modelConfig = getModelResponse.getResources().results().get(0);

                verifyMlNodesAndModelArchitectures(modelConfig, client, threadPool, getVerifiedModel);

            },
            failedDeploymentListener::onFailure
        );

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            GetTrainedModelsAction.INSTANCE,
            new GetTrainedModelsAction.Request(task.getParams().getModelId()),
            verifyModelAndClusterArchitecturesListener
        );
    }

    void verifyMlNodesAndModelArchitectures(
        TrainedModelConfig configToReturn,
        Client client,
        ThreadPool threadPool,
        ActionListener<TrainedModelConfig> configToReturnListener
    ) {
        ActionListener<TrainedModelConfig> verifyConfigListener = new ActionListener<TrainedModelConfig>() {
            @Override
            public void onResponse(TrainedModelConfig config) {
                assert Objects.equals(config, configToReturn);
                configToReturnListener.onResponse(configToReturn);
            }

            @Override
            public void onFailure(Exception e) {
                configToReturnListener.onFailure(e);
            }
        };

        callVerifyMlNodesAndModelArchitectures(configToReturn, verifyConfigListener, client, threadPool);
    }

    void callVerifyMlNodesAndModelArchitectures(
        TrainedModelConfig configToReturn,
        ActionListener<TrainedModelConfig> configToReturnListener,
        Client client,
        ThreadPool threadPool
    ) {
        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(
            configToReturnListener,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            configToReturn
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
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(xContentRegistry),
                hit.getSourceRef(),
                XContentType.JSON
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
            processContext.forcefullyStopProcess();
        } else {
            logger.warn("[{}] No process context to stop", task.getDeploymentId());
        }
    }

    public void stopAfterCompletingPendingWork(TrainedModelDeploymentTask task) {
        ProcessContext processContext = processContextByAllocation.remove(task.getId());
        if (processContext != null) {
            logger.info(
                "[{}] Stopping deployment after completing pending tasks, reason [{}]",
                task.getDeploymentId(),
                task.stoppedReason().orElse("unknown")
            );
            processContext.stopProcessAfterCompletingPendingWork();
        } else {
            logger.warn("[{}] No process context to stop gracefully", task.getDeploymentId());
        }
    }

    public void infer(
        TrainedModelDeploymentTask task,
        InferenceConfig config,
        NlpInferenceInput input,
        boolean skipQueue,
        TimeValue timeout,
        TrainedModelPrefixStrings.PrefixType prefixType,
        CancellableTask parentActionTask,
        boolean chunkResponse,
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
            prefixType,
            threadPool,
            parentActionTask,
            chunkResponse,
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
            listener.delegateFailureAndWrap((l, b) -> l.onResponse(AcknowledgedResponse.TRUE))
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

        private static final String PROCESS_NAME = "inference process";
        private static final TimeValue COMPLETION_TIMEOUT = TimeValue.timeValueMinutes(3);

        private final TrainedModelDeploymentTask task;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();
        private final SetOnce<NlpTask.Processor> nlpTaskProcessor = new SetOnce<>();
        private final SetOnce<TrainedModelInput> modelInput = new SetOnce<>();
        private final SetOnce<TrainedModelPrefixStrings> prefixes = new SetOnce<>();
        private final PyTorchResultProcessor resultProcessor;
        private final PyTorchStateStreamer stateStreamer;
        private final PriorityProcessWorkerExecutorService priorityProcessWorker;
        private final AtomicInteger rejectedExecutionCount = new AtomicInteger();
        private final AtomicInteger timeoutCount = new AtomicInteger();
        private final AtomicInteger startsCount = new AtomicInteger();
        private volatile Instant startTime;
        private volatile Integer numThreadsPerAllocation;
        private volatile Integer numAllocations;
        private volatile boolean isStopped;

        ProcessContext(TrainedModelDeploymentTask task, Integer startsCount) {
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
                PROCESS_NAME,
                task.getParams().getQueueCapacity()
            );
            this.startsCount.set(startsCount == null ? 1 : startsCount);
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
            process.set(
                pyTorchProcessFactory.createProcess(
                    task,
                    executorServiceForProcess,
                    () -> resultProcessor.awaitCompletion(COMPLETION_TIMEOUT.getMinutes(), TimeUnit.MINUTES),
                    onProcessCrashHandleRestarts(startsCount, task.getDeploymentId())
                )
            );
            startTime = Instant.now();
            logger.debug("[{}] process started", task.getDeploymentId());
            try {
                loadModel(modelLocation, loadedListener.delegateFailureAndWrap((delegate, success) -> {
                    if (isStopped) {
                        logger.debug("[{}] model loaded but process is stopped", task.getDeploymentId());
                        killProcessIfPresent();
                        delegate.onFailure(new IllegalStateException("model loaded but process is stopped"));
                        return;
                    }

                    logger.debug("[{}] model loaded, starting priority process worker thread", task.getDeploymentId());
                    startPriorityProcessWorker();
                    delegate.onResponse(success);
                }));
            } catch (Exception e) {
                loadedListener.onFailure(e);
            }
        }

        private Consumer<String> onProcessCrashHandleRestarts(AtomicInteger startsCount, String deploymentId) {
            return (reason) -> {
                if (isThisProcessOlderThan1Day()) {
                    startsCount.set(1);
                    {
                        String logMessage = "["
                            + task.getDeploymentId()
                            + "] inference process crashed due to reason ["
                            + reason
                            + "]. This process was started more than 24 hours ago; "
                            + "the starts count is reset to 1.";
                        logger.error(logMessage);
                    }
                } else {
                    logger.error("[{}] inference process crashed due to reason [{}]", task.getDeploymentId(), reason);
                }

                processContextByAllocation.remove(task.getId());
                isStopped = true;
                resultProcessor.stop();
                stateStreamer.cancel();

                if (startsCount.get() <= NUM_RESTART_ATTEMPTS) {
                    {
                        String logAndAuditMessage = "Inference process ["
                            + task.getDeploymentId()
                            + "] failed due to ["
                            + reason
                            + "]. This is the ["
                            + startsCount.get()
                            + "] failure in 24 hours, and the process will be restarted.";
                        logger.info(logAndAuditMessage);
                        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                            .execute(() -> inferenceAuditor.warning(deploymentId, logAndAuditMessage));
                    }
                    priorityProcessWorker.shutdownNow(); // TODO what to do with these tasks?
                    ActionListener<TrainedModelDeploymentTask> errorListener = ActionListener.wrap((trainedModelDeploymentTask -> {
                        logger.debug("Completed restart of inference process, the [{}] start", startsCount);
                    }),
                        (e) -> finishClosingProcess(
                            startsCount,
                            "Failed to restart inference process because of error [" + e.getMessage() + "]",
                            deploymentId
                        )
                    );

                    startDeployment(task, startsCount.incrementAndGet(), errorListener);
                } else {
                    finishClosingProcess(startsCount, reason, deploymentId);
                }
            };
        }

        private boolean isThisProcessOlderThan1Day() {
            return startTime.isBefore(Instant.now().minus(Duration.ofDays(1)));
        }

        private void finishClosingProcess(AtomicInteger startsCount, String reason, String deploymentId) {
            String logAndAuditMessage = "["
                + task.getDeploymentId()
                + "] inference process failed after ["
                + startsCount.get()
                + "] starts in 24 hours, not restarting again.";
            logger.warn(logAndAuditMessage);
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                .execute(() -> inferenceAuditor.error(deploymentId, logAndAuditMessage));
            priorityProcessWorker.shutdownNowWithError(new IllegalStateException(reason));
            if (nlpTaskProcessor.get() != null) {
                nlpTaskProcessor.get().close();
            }
            task.setFailed("inference process crashed due to reason [" + reason + "]");
        }

        void startPriorityProcessWorker() {
            executorServiceForProcess.submit(priorityProcessWorker::start);
        }

        synchronized void forcefullyStopProcess() {
            logger.debug(() -> format("[%s] Forcefully stopping process", task.getDeploymentId()));
            prepareInternalStateForShutdown();

            priorityProcessWorker.shutdownNow();
            try {
                // wait for any currently executing work to finish
                if (priorityProcessWorker.awaitTermination(10L, TimeUnit.SECONDS)) {
                    priorityProcessWorker.notifyQueueRunnables();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info(Strings.format("[%s] Interrupted waiting for process worker after shutdownNow", PROCESS_NAME));
            }

            killProcessIfPresent();
            closeNlpTaskProcessor();
        }

        private void prepareInternalStateForShutdown() {
            isStopped = true;
            resultProcessor.stop();
            stateStreamer.cancel();
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

        private void closeNlpTaskProcessor() {
            if (nlpTaskProcessor.get() != null) {
                nlpTaskProcessor.get().close();
            }
        }

        private synchronized void stopProcessAfterCompletingPendingWork() {
            logger.debug(() -> format("[%s] Stopping process after completing its pending work", task.getDeploymentId()));
            prepareInternalStateForShutdown();
            signalAndWaitForWorkerTermination();
            stopProcessGracefully();
            closeNlpTaskProcessor();
        }

        private void signalAndWaitForWorkerTermination() {
            try {
                awaitTerminationAfterCompletingWork();
            } catch (TimeoutException e) {
                logger.warn(format("[%s] Timed out waiting for process worker to complete, forcing a shutdown", task.getDeploymentId()), e);
                // The process failed to stop in the time period allotted, so we'll mark it for shut down
                priorityProcessWorker.shutdown();
                priorityProcessWorker.notifyQueueRunnables();
            }
        }

        private void awaitTerminationAfterCompletingWork() throws TimeoutException {
            try {
                priorityProcessWorker.shutdown();

                if (priorityProcessWorker.awaitTermination(COMPLETION_TIMEOUT.getMinutes(), TimeUnit.MINUTES) == false) {
                    throw new TimeoutException(
                        Strings.format("Timed out waiting for process worker to complete for process %s", PROCESS_NAME)
                    );
                } else {
                    priorityProcessWorker.notifyQueueRunnables();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info(Strings.format("[%s] Interrupted waiting for process worker to complete", PROCESS_NAME));
            }
        }

        private void stopProcessGracefully() {
            try {
                closeProcessIfPresent();
                resultProcessor.awaitCompletion(COMPLETION_TIMEOUT.getMinutes(), TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                logger.warn(format("[%s] Timed out waiting for results processor to stop", task.getDeploymentId()), e);
            }
        }

        private void closeProcessIfPresent() {
            try {
                if (process.get() == null) {
                    return;
                }

                process.get().close();
            } catch (IOException e) {
                logger.error(format("[%s] Failed to stop process gracefully, attempting to kill it", task.getDeploymentId()), e);
                killProcessIfPresent();
            }
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

        SetOnce<TrainedModelPrefixStrings> getPrefixStrings() {
            return prefixes;
        }
    }
}
