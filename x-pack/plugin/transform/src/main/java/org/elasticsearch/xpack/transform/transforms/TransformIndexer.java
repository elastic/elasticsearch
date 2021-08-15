/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.RetentionPolicyToDeleteByQueryRequestConverter.RetentionPolicyException;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public abstract class TransformIndexer extends AsyncTwoPhaseIndexer<TransformIndexerPosition, TransformIndexerStats> {

    private static final int PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC = 5;

    /**
     * RunState is an internal (non-persisted) state that controls the internal logic
     * which query filters to run and which index requests to send
     */
    private enum RunState {
        // apply results
        APPLY_RESULTS,

        // identify changes, used for continuous transform
        IDENTIFY_CHANGES,
    }

    public static final int MINIMUM_PAGE_SIZE = 10;

    private static final Logger logger = LogManager.getLogger(TransformIndexer.class);

    // constant for checkpoint retention, static for now
    private static final long NUMBER_OF_CHECKPOINTS_TO_KEEP = 10;
    private static final long RETENTION_OF_CHECKPOINTS_MS = 864000000L; // 10 days
    private static final long CHECKPOINT_CLEANUP_INTERVAL = 100L; // every 100 checkpoints

    protected final TransformConfigManager transformsConfigManager;
    private final CheckpointProvider checkpointProvider;
    private volatile float docsPerSecond = -1;

    protected final TransformAuditor auditor;
    protected final TransformContext context;

    protected volatile TransformConfig transformConfig;
    private volatile TransformProgress progress;
    // Indicates that the source has changed for the current run
    protected volatile boolean hasSourceChanged = true;

    protected final AtomicReference<Collection<ActionListener<Void>>> saveStateListeners = new AtomicReference<>();

    private volatile Map<String, String> fieldMappings;

    // the function of the transform, e.g. pivot or latest
    private Function function;

    // collects changes for continuous mode
    private ChangeCollector changeCollector;

    // position of the change collector, in flux (not yet persisted as we haven't processed changes yet)
    private Map<String, Object> nextChangeCollectorBucketPosition = null;

    private volatile Integer initialConfiguredPageSize;
    private volatile int pageSize = 0;
    private volatile long logEvery = 1;
    private volatile long logCount = 0;
    private volatile TransformCheckpoint lastCheckpoint;
    private volatile TransformCheckpoint nextCheckpoint;

    // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
    private volatile String lastAuditedExceptionMessage = null;
    private volatile RunState runState;

    private volatile long lastCheckpointCleanup = 0L;

    protected volatile boolean indexerThreadShuttingDown = false;
    protected volatile boolean saveStateRequestedDuringIndexerThreadShutdown = false;

    public TransformIndexer(
        ThreadPool threadPool,
        TransformServices transformServices,
        CheckpointProvider checkpointProvider,
        TransformConfig transformConfig,
        AtomicReference<IndexerState> initialState,
        TransformIndexerPosition initialPosition,
        TransformIndexerStats jobStats,
        TransformProgress transformProgress,
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        TransformContext context
    ) {
        super(threadPool, initialState, initialPosition, jobStats);
        ExceptionsHelper.requireNonNull(transformServices, "transformServices");
        this.transformsConfigManager = transformServices.getConfigManager();
        this.checkpointProvider = ExceptionsHelper.requireNonNull(checkpointProvider, "checkpointProvider");
        this.auditor = transformServices.getAuditor();
        this.transformConfig = ExceptionsHelper.requireNonNull(transformConfig, "transformConfig");
        this.progress = transformProgress != null ? transformProgress : new TransformProgress();
        this.lastCheckpoint = ExceptionsHelper.requireNonNull(lastCheckpoint, "lastCheckpoint");
        this.nextCheckpoint = ExceptionsHelper.requireNonNull(nextCheckpoint, "nextCheckpoint");
        this.context = ExceptionsHelper.requireNonNull(context, "context");
        // give runState a default
        this.runState = RunState.APPLY_RESULTS;

        if (transformConfig.getSettings() != null && transformConfig.getSettings().getDocsPerSecond() != null) {
            docsPerSecond = transformConfig.getSettings().getDocsPerSecond();
        }
    }

    abstract void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener);

    abstract void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener);

    abstract void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener);

    abstract void refreshDestinationIndex(ActionListener<RefreshResponse> responseListener);

    public int getPageSize() {
        return pageSize;
    }

    @Override
    protected String getJobId() {
        return transformConfig.getId();
    }

    @Override
    protected float getMaxDocsPerSecond() {
        return docsPerSecond;
    }

    @Override
    protected boolean triggerSaveState() {
        // trigger in case of listeners waiting for state being saved
        return saveStateListeners.get() != null || super.triggerSaveState();
    }

    public TransformConfig getConfig() {
        return transformConfig;
    }

    public boolean isContinuous() {
        return getConfig().getSyncConfig() != null;
    }

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public TransformProgress getProgress() {
        return progress;
    }

    public TransformCheckpoint getLastCheckpoint() {
        return lastCheckpoint;
    }

    public TransformCheckpoint getNextCheckpoint() {
        return nextCheckpoint;
    }

    public CheckpointProvider getCheckpointProvider() {
        return checkpointProvider;
    }

    /**
     * Request a checkpoint
     */
    protected void createCheckpoint(ActionListener<TransformCheckpoint> listener) {
        checkpointProvider.createNextCheckpoint(
            getLastCheckpoint(),
            ActionListener.wrap(
                checkpoint -> transformsConfigManager.putTransformCheckpoint(
                    checkpoint,
                    ActionListener.wrap(putCheckPointResponse -> listener.onResponse(checkpoint), createCheckpointException -> {
                        logger.warn(new ParameterizedMessage("[{}] failed to create checkpoint.", getJobId()), createCheckpointException);
                        listener.onFailure(
                            new RuntimeException(
                                "Failed to create checkpoint due to: " + createCheckpointException.getMessage(),
                                createCheckpointException
                            )
                        );
                    })
                ),
                getCheckPointException -> {
                    logger.warn(new ParameterizedMessage("[{}] failed to retrieve checkpoint.", getJobId()), getCheckPointException);
                    listener.onFailure(
                        new RuntimeException(
                            "Failed to retrieve checkpoint due to: " + getCheckPointException.getMessage(),
                            getCheckPointException
                        )
                    );
                }
            )
        );
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to start while failed.", getJobId());
            listener.onFailure(new ElasticsearchException("Attempted to start a failed transform [{}].", getJobId()));
            return;
        }

        ActionListener<Void> finalListener = ActionListener.wrap(r -> {
            try {
                // if we haven't set the page size yet, if it is set we might have reduced it after running into an out of memory
                if (pageSize == 0) {
                    configurePageSize(getConfig().getSettings().getMaxPageSearchSize());
                }

                runState = determineRunStateAtStart();
                listener.onResponse(true);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }, listener::onFailure);

        // On each run, we need to get the total number of docs and reset the count of processed docs
        // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
        // the progress here, and not in the executor.
        ActionListener<Void> configurationReadyListener = ActionListener.wrap(r -> {
            initializeFunction();

            if (initialRun()) {
                createCheckpoint(ActionListener.wrap(cp -> {
                    nextCheckpoint = cp;
                    // If nextCheckpoint > 1, this means that we are now on the checkpoint AFTER the batch checkpoint
                    // Consequently, the idea of percent complete no longer makes sense.
                    if (nextCheckpoint.getCheckpoint() > 1) {
                        progress = new TransformProgress(null, 0L, 0L);
                        finalListener.onResponse(null);
                        return;
                    }

                    // get progress information
                    SearchRequest request = new SearchRequest(transformConfig.getSource().getIndex());
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

                    function.buildSearchQueryForInitialProgress(searchSourceBuilder);
                    searchSourceBuilder.query(QueryBuilders.boolQuery().filter(buildFilterQuery()).filter(searchSourceBuilder.query()));
                    request.allowPartialSearchResults(false).source(searchSourceBuilder);

                    doGetInitialProgress(request, ActionListener.wrap(response -> {
                        function.getInitialProgressFromResponse(response, ActionListener.wrap(newProgress -> {
                            logger.trace("[{}] reset the progress from [{}] to [{}].", getJobId(), progress, newProgress);
                            progress = newProgress != null ? newProgress : new TransformProgress();
                            finalListener.onResponse(null);
                        }, failure -> {
                            progress = new TransformProgress();
                            logger.warn(
                                new ParameterizedMessage("[{}] unable to load progress information for task.", getJobId()),
                                failure
                            );
                            finalListener.onResponse(null);
                        }));
                    }, failure -> {
                        progress = new TransformProgress();
                        logger.warn(new ParameterizedMessage("[{}] unable to load progress information for task.", getJobId()), failure);
                        finalListener.onResponse(null);
                    }));
                }, listener::onFailure));
            } else {
                finalListener.onResponse(null);
            }
        }, listener::onFailure);

        ActionListener<Map<String, String>> fieldMappingsListener = ActionListener.wrap(fieldMappings -> {
            this.fieldMappings = fieldMappings;
            configurationReadyListener.onResponse(null);
        }, listener::onFailure);

        ActionListener<Void> reLoadFieldMappingsListener = ActionListener.wrap(
            updateConfigResponse -> { doGetFieldMappings(fieldMappingsListener); },
            listener::onFailure
        );

        // If we are continuous, we will want to verify we have the latest stored configuration
        ActionListener<Void> changedSourceListener = ActionListener.wrap(r -> {
            if (isContinuous()) {
                transformsConfigManager.getTransformConfiguration(getJobId(), ActionListener.wrap(config -> {
                    if (transformConfig.equals(config) && fieldMappings != null) {
                        logger.trace("[{}] transform config has not changed.", getJobId());
                        configurationReadyListener.onResponse(null);
                    } else {
                        transformConfig = config;
                        logger.debug("[{}] successfully refreshed transform config from index.", getJobId());
                        reLoadFieldMappingsListener.onResponse(null);
                    }
                }, failure -> {
                    String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION, getJobId());
                    logger.error(msg, failure);
                    // If the transform config index or the transform config is gone, something serious occurred
                    // We are in an unknown state and should fail out
                    if (failure instanceof ResourceNotFoundException) {
                        reLoadFieldMappingsListener.onFailure(new TransformConfigLostOnReloadException(msg, failure));
                    } else {
                        auditor.warning(getJobId(), msg);
                        reLoadFieldMappingsListener.onResponse(null);
                    }
                }));
            } else {
                reLoadFieldMappingsListener.onResponse(null);
            }
        }, listener::onFailure);

        Instant instantOfTrigger = Instant.ofEpochMilli(now);
        // If we are not on the initial batch checkpoint and its the first pass of whatever continuous checkpoint we are on,
        // we should verify if there are local changes based on the sync config. If not, do not proceed further and exit.
        if (context.getCheckpoint() > 0 && initialRun()) {
            sourceHasChanged(ActionListener.wrap(hasChanged -> {
                context.setLastSearchTime(instantOfTrigger);
                hasSourceChanged = hasChanged;
                if (hasChanged) {
                    context.setChangesLastDetectedAt(instantOfTrigger);
                    logger.debug("[{}] source has changed, triggering new indexer run.", getJobId());
                    changedSourceListener.onResponse(null);
                } else {
                    logger.trace("[{}] source has not changed, finish indexer early.", getJobId());
                    // No changes, stop executing
                    listener.onResponse(false);
                }
            }, failure -> {
                // If we failed determining if the source changed, it's safer to assume there were changes.
                // We should allow the failure path to complete as normal
                hasSourceChanged = true;
                listener.onFailure(failure);
            }));
        } else {
            hasSourceChanged = true;
            context.setLastSearchTime(instantOfTrigger);
            context.setChangesLastDetectedAt(instantOfTrigger);
            changedSourceListener.onResponse(null);
        }
    }

    protected void initializeFunction() {
        // create the function
        function = FunctionFactory.create(getConfig());
        if (isContinuous()) {
            changeCollector = function.buildChangeCollector(getConfig().getSyncConfig().getField());
        }
    }

    protected boolean initialRun() {
        return getPosition() == null;
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        startIndexerThreadShutdown();

        // This indicates an early exit since no changes were found.
        // So, don't treat this like a checkpoint being completed, as no work was done.
        if (hasSourceChanged == false) {
            if (context.shouldStopAtCheckpoint()) {
                stop();
            }
            listener.onResponse(null);
            return;
        }

        ActionListener<Void> failureHandlingListener = ActionListener.wrap(listener::onResponse, failure -> {
            handleFailure(failure);
            listener.onFailure(failure);
        });

        try {
            refreshDestinationIndex(ActionListener.wrap(response -> {
                if (response.getFailedShards() > 0) {
                    logger.warn(
                        "[{}] failed to refresh transform destination index, not all data might be available after checkpoint.",
                        getJobId()
                    );
                }
                // delete data defined by retention policy
                if (transformConfig.getRetentionPolicyConfig() != null) {
                    executeRetentionPolicy(failureHandlingListener);
                } else {
                    finalizeCheckpoint(failureHandlingListener);
                }
            }, failureHandlingListener::onFailure));
        } catch (Exception e) {
            failureHandlingListener.onFailure(e);
        }
    }

    private void executeRetentionPolicy(ActionListener<Void> listener) {
        DeleteByQueryRequest deleteByQuery = RetentionPolicyToDeleteByQueryRequestConverter.buildDeleteByQueryRequest(
            transformConfig.getRetentionPolicyConfig(),
            transformConfig.getSettings(),
            transformConfig.getDestination(),
            nextCheckpoint
        );

        if (deleteByQuery == null) {
            finalizeCheckpoint(listener);
            return;
        }

        logger.debug(
            () -> new ParameterizedMessage(
                "[{}] Run delete based on retention policy using dbq [{}] with query: [{}]",
                getJobId(),
                deleteByQuery,
                deleteByQuery.getSearchRequest()
            )
        );
        getStats().markStartDelete();

        doDeleteByQuery(deleteByQuery, ActionListener.wrap(bulkByScrollResponse -> {
            logger.trace(() -> new ParameterizedMessage("[{}] dbq response: [{}]", getJobId(), bulkByScrollResponse));

            getStats().markEndDelete();
            getStats().incrementNumDeletedDocuments(bulkByScrollResponse.getDeleted());
            logger.debug("[{}] deleted [{}] documents as part of the retention policy.", getJobId(), bulkByScrollResponse.getDeleted());

            // this should not happen as part of checkpointing
            if (bulkByScrollResponse.getVersionConflicts() > 0) {
                // note: the failure gets logged by the failure handler
                listener.onFailure(
                    new RetentionPolicyException(
                        "found [{}] version conflicts when deleting documents as part of the retention policy.",
                        bulkByScrollResponse.getDeleted()
                    )
                );
                return;
            }
            // paranoia: we are not expecting dbq to fail for other reasons
            if (bulkByScrollResponse.getBulkFailures().size() > 0 || bulkByScrollResponse.getSearchFailures().size() > 0) {
                assert false : "delete by query failed unexpectedly" + bulkByScrollResponse;
                listener.onFailure(
                    new RetentionPolicyException(
                        "found failures when deleting documents as part of the retention policy. Response: [{}]",
                        bulkByScrollResponse
                    )
                );
                return;
            }

            finalizeCheckpoint(listener);
        }, listener::onFailure));
    }

    private void finalizeCheckpoint(ActionListener<Void> listener) {
        try {
            // reset the page size, so we do not memorize a low page size forever
            pageSize = function.getInitialPageSize();
            // reset the changed bucket to free memory
            if (changeCollector != null) {
                changeCollector.clear();
            }

            long checkpoint = context.incrementAndGetCheckpoint();
            lastCheckpoint = getNextCheckpoint();
            nextCheckpoint = null;
            // Reset our failure count as we have finished and may start again with a new checkpoint
            context.resetReasonAndFailureCounter();

            // With bucket_selector we could have read all the buckets and completed the transform
            // but not "see" all the buckets since they were filtered out. Consequently, progress would
            // show less than 100% even though we are done.
            // NOTE: this method is called in the same thread as the processing thread.
            // Theoretically, there should not be a race condition with updating progress here.
            // NOTE 2: getPercentComplete should only NOT be null on the first (batch) checkpoint
            if (progress.getPercentComplete() != null && progress.getPercentComplete() < 100.0) {
                progress.incrementDocsProcessed(progress.getTotalDocs() - progress.getDocumentsProcessed());
            }

            if (lastCheckpoint != null) {
                long docsIndexed = 0;
                long docsProcessed = 0;
                docsIndexed = progress.getDocumentsIndexed();
                docsProcessed = progress.getDocumentsProcessed();

                long durationMs = System.currentTimeMillis() - lastCheckpoint.getTimestamp();
                getStats().incrementCheckpointExponentialAverages(durationMs < 0 ? 0 : durationMs, docsIndexed, docsProcessed);
            }
            if (shouldAuditOnFinish(checkpoint)) {
                auditor.info(getJobId(), "Finished indexing for transform checkpoint [" + checkpoint + "].");
            }
            logger.debug("[{}] finished indexing for transform checkpoint [{}].", getJobId(), checkpoint);
            if (context.shouldStopAtCheckpoint()) {
                stop();
            }

            if (checkpoint - lastCheckpointCleanup > CHECKPOINT_CLEANUP_INTERVAL) {
                // delete old checkpoints, on a failure we keep going
                cleanupOldCheckpoints(listener);
            } else {
                listener.onResponse(null);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void afterFinishOrFailure() {
        finishIndexerThreadShutdown();
    }

    @Override
    protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
        switch (runState) {
            case APPLY_RESULTS:
                return processBuckets(searchResponse);
            case IDENTIFY_CHANGES:
                return processChangedBuckets(searchResponse);

            default:
                // Any other state is a bug, should not happen
                logger.warn("[{}] Encountered unexpected run state [{}]", getJobId(), runState);
                throw new IllegalStateException("Transform indexer job encountered an illegal state [" + runState + "]");
        }
    }

    @Override
    public synchronized boolean maybeTriggerAsyncJob(long now) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getJobId());
            return false;
        }

        // ignore trigger if indexer is running, prevents log spam in A2P indexer
        IndexerState indexerState = getState();
        if (IndexerState.INDEXING.equals(indexerState) || IndexerState.STOPPING.equals(indexerState)) {
            logger.debug("[{}] indexer for transform has state [{}]. Ignoring trigger.", getJobId(), indexerState);
            return false;
        }

        /*
         * ignore if indexer thread is shutting down (after finishing a checkpoint)
         * shutting down means:
         *  - indexer has finished a checkpoint and called onFinish
         *  - indexer state has changed from indexing to started
         *  - state persistence has been called but has _not_ returned yet
         *
         *  If we trigger the indexer in this situation the 2nd indexer thread might
         *  try to save state at the same time, causing a version conflict
         *  see gh#67121
         */
        if (indexerThreadShuttingDown) {
            logger.debug("[{}] indexer thread is shutting down. Ignoring trigger.", getJobId());
            return false;
        }

        return super.maybeTriggerAsyncJob(now);
    }

    /**
     * Handle new settings at runtime, this is triggered by a call to _transform/id/_update
     *
     * @param newSettings The new settings that should be applied
     */
    public void applyNewSettings(SettingsConfig newSettings) {
        auditor.info(transformConfig.getId(), "Transform settings have been updated.");
        logger.info("[{}] transform settings have been updated.", transformConfig.getId());

        docsPerSecond = newSettings.getDocsPerSecond() != null ? newSettings.getDocsPerSecond() : -1;
        if (Objects.equals(newSettings.getMaxPageSearchSize(), initialConfiguredPageSize) == false) {
            configurePageSize(newSettings.getMaxPageSearchSize());
        }
        rethrottle();
    }

    @Override
    protected void onFailure(Exception exc) {
        startIndexerThreadShutdown();
        // the failure handler must not throw an exception due to internal problems
        try {
            handleFailure(exc);
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("[{}] transform encountered an unexpected internal exception: ", getJobId()), e);
        }
    }

    @Override
    protected void onStop() {
        auditor.info(transformConfig.getId(), "Transform has stopped.");
        logger.info("[{}] transform has stopped.", transformConfig.getId());
    }

    @Override
    protected void onAbort() {
        auditor.info(transformConfig.getId(), "Received abort request, stopping transform.");
        logger.info("[{}] transform received abort request. Stopping indexer.", transformConfig.getId());
        context.shutdown();
    }

    /**
     * Let the indexer stop at the next checkpoint and call the listener after the flag has been persisted in state.
     *
     * If the indexer isn't running, persist state if required and call the listener immediately.
     */
    final void setStopAtCheckpoint(boolean shouldStopAtCheckpoint, ActionListener<Void> shouldStopAtCheckpointListener) {
        // this should be called from the generic threadpool
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);

        try {
            if (addSetStopAtCheckpointListener(shouldStopAtCheckpoint, shouldStopAtCheckpointListener) == false) {
                shouldStopAtCheckpointListener.onResponse(null);
            }
        } catch (InterruptedException e) {
            logger.error(
                new ParameterizedMessage(
                    "[{}] Interrupt waiting ({}s) for transform state to be stored.",
                    getJobId(),
                    PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC
                ),
                e
            );

            // the transport wraps this with a REST status code
            shouldStopAtCheckpointListener.onFailure(
                new RuntimeException(
                    "Timed out (" + PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC + "s) waiting for transform state to be stored.",
                    e
                )
            );
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("[{}] failed to persist transform state.", getJobId()), e);
            shouldStopAtCheckpointListener.onFailure(e);
        }
    }

    private synchronized boolean addSetStopAtCheckpointListener(
        boolean shouldStopAtCheckpoint,
        ActionListener<Void> shouldStopAtCheckpointListener
    ) throws InterruptedException {

        // in case the indexer is already shutting down
        if (indexerThreadShuttingDown) {
            context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
            saveStateRequestedDuringIndexerThreadShutdown = true;
            return false;
        }

        IndexerState state = getState();

        // in case the indexer isn't running, respond immediately
        if (state == IndexerState.STARTED && context.shouldStopAtCheckpoint() != shouldStopAtCheckpoint) {
            context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);

            // because save state is async we need to block the call until state is persisted, so that the job can not
            // be triggered (ensured by synchronized)
            CountDownLatch latch = new CountDownLatch(1);
            logger.debug("[{}] persisiting stop at checkpoint", getJobId());

            doSaveState(IndexerState.STARTED, getPosition(), () -> { latch.countDown(); });

            if (latch.await(PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                logger.error(
                    new ParameterizedMessage(
                        "[{}] Timed out ({}s) waiting for transform state to be stored.",
                        getJobId(),
                        PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC
                    )
                );
            }
            return false;
        }

        if (state != IndexerState.INDEXING) {
            return false;
        }

        if (saveStateListeners.updateAndGet(currentListeners -> {
            // check the state again (optimistic locking), while we checked the last time, the indexing thread could have
            // saved the state and is finishing. As it first set the state and _than_ gets saveStateListeners, it's safe
            // to just check the indexer state again
            if (getState() != IndexerState.INDEXING) {
                return null;
            }

            if (currentListeners == null) {
                // in case shouldStopAtCheckpoint has already the desired value _and_ we know its _persisted_, respond immediately
                if (context.shouldStopAtCheckpoint() == shouldStopAtCheckpoint) {
                    return null;
                }

                return Collections.singletonList(shouldStopAtCheckpointListener);
            }
            return CollectionUtils.appendToCopy(currentListeners, shouldStopAtCheckpointListener);
        }) == null) {
            return false;
        }

        context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
        // in case of throttling the indexer might wait for the next search, fast forward, so stop listeners do not wait to long
        runSearchImmediately();
        return true;
    }

    synchronized void stopAndMaybeSaveState() {
        onStop();
        IndexerState state = stop();

        if (indexerThreadShuttingDown) {
            saveStateRequestedDuringIndexerThreadShutdown = true;
            // if stop() returned STOPPED we need to persist state, otherwise the indexer does it for us
        } else if (state == IndexerState.STOPPED) {
            doSaveState(IndexerState.STOPPED, getPosition(), () -> {});
        }
    }

    synchronized void handleFailure(Exception e) {
        logger.warn(new ParameterizedMessage("[{}] transform encountered an exception: ", getJobId()), e);
        Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);

        if (unwrappedException instanceof CircuitBreakingException) {
            handleCircuitBreakingException((CircuitBreakingException) unwrappedException);
            return;
        }

        if (unwrappedException instanceof ScriptException) {
            handleScriptException((ScriptException) unwrappedException);
            return;
        }

        if (unwrappedException instanceof BulkIndexingException && ((BulkIndexingException) unwrappedException).isIrrecoverable()) {
            handleIrrecoverableBulkIndexingException((BulkIndexingException) unwrappedException);
            return;
        }

        // irrecoverable error without special handling
        if (unwrappedException instanceof ElasticsearchException) {
            ElasticsearchException elasticsearchException = (ElasticsearchException) unwrappedException;
            if (ExceptionRootCauseFinder.IRRECOVERABLE_REST_STATUSES.contains(elasticsearchException.status())) {
                failIndexer("task encountered irrecoverable failure: " + elasticsearchException.getDetailedMessage());
                return;
            }
        }

        if (unwrappedException instanceof IllegalArgumentException) {
            failIndexer("task encountered irrecoverable failure: " + e.getMessage());
            return;
        }

        if (context.getAndIncrementFailureCount() > context.getNumFailureRetries()) {
            failIndexer(
                "task encountered more than "
                    + context.getNumFailureRetries()
                    + " failures; latest failure: "
                    + ExceptionRootCauseFinder.getDetailedMessage(unwrappedException)
            );
            return;
        }

        // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
        // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
        if (e.getMessage().equals(lastAuditedExceptionMessage) == false) {
            String message = ExceptionRootCauseFinder.getDetailedMessage(unwrappedException);

            auditor.warning(
                getJobId(),
                "Transform encountered an exception: " + message + "; Will attempt again at next scheduled trigger."
            );
            lastAuditedExceptionMessage = message;
        }
    }

    /**
     * Cleanup old checkpoints
     *
     * @param listener listener to call after done
     */
    private void cleanupOldCheckpoints(ActionListener<Void> listener) {
        long now = getTimeNanos() * 1000;
        long checkpointLowerBound = context.getCheckpoint() - NUMBER_OF_CHECKPOINTS_TO_KEEP;
        long lowerBoundEpochMs = now - RETENTION_OF_CHECKPOINTS_MS;

        if (checkpointLowerBound > 0 && lowerBoundEpochMs > 0) {
            transformsConfigManager.deleteOldCheckpoints(
                transformConfig.getId(),
                checkpointLowerBound,
                lowerBoundEpochMs,
                ActionListener.wrap(deletes -> {
                    logger.debug("[{}] deleted [{}] outdated checkpoints", getJobId(), deletes);
                    listener.onResponse(null);
                    lastCheckpointCleanup = context.getCheckpoint();
                }, e -> {
                    logger.warn(
                        new ParameterizedMessage("[{}] failed to cleanup old checkpoints, retrying after next checkpoint", getJobId()),
                        e
                    );
                    auditor.warning(
                        getJobId(),
                        "Failed to cleanup old checkpoints, retrying after next checkpoint. Exception: " + e.getMessage()
                    );

                    listener.onResponse(null);
                })
            );
        } else {
            logger.debug("[{}] checked for outdated checkpoints", getJobId());
            listener.onResponse(null);
        }
    }

    private void sourceHasChanged(ActionListener<Boolean> hasChangedListener) {
        checkpointProvider.sourceHasChanged(getLastCheckpoint(), ActionListener.wrap(hasChanged -> {
            logger.trace("[{}] change detected [{}].", getJobId(), hasChanged);
            hasChangedListener.onResponse(hasChanged);
        }, e -> {
            logger.warn(
                new ParameterizedMessage("[{}] failed to detect changes for transform. Skipping update till next check.", getJobId()),
                e
            );
            auditor.warning(
                getJobId(),
                "Failed to detect changes for transform, skipping update till next check. Exception: " + e.getMessage()
            );
            hasChangedListener.onResponse(false);
        }));
    }

    private IterationResult<TransformIndexerPosition> processBuckets(final SearchResponse searchResponse) {
        Tuple<Stream<IndexRequest>, Map<String, Object>> indexRequestStreamAndCursor = function.processSearchResponse(
            searchResponse,
            getConfig().getDestination().getIndex(),
            getConfig().getDestination().getPipeline(),
            getFieldMappings(),
            getStats(),
            progress
        );

        if (indexRequestStreamAndCursor == null || indexRequestStreamAndCursor.v1() == null) {
            if (nextCheckpoint.getCheckpoint() == 1 || isContinuous() == false || changeCollector.queryForChanges() == false) {
                return new IterationResult<>(Stream.empty(), null, true);
            }

            // cleanup changed Buckets
            changeCollector.clear();

            // reset the runState to fetch changed buckets
            runState = RunState.IDENTIFY_CHANGES;

            // advance the cursor for changed bucket detection
            return new IterationResult<>(Stream.empty(), new TransformIndexerPosition(null, nextChangeCollectorBucketPosition), false);
        }

        Stream<IndexRequest> indexRequestStream = indexRequestStreamAndCursor.v1();
        TransformIndexerPosition oldPosition = getPosition();
        TransformIndexerPosition newPosition = new TransformIndexerPosition(
            indexRequestStreamAndCursor.v2(),
            oldPosition != null ? getPosition().getBucketsPosition() : null
        );

        return new IterationResult<>(indexRequestStream, newPosition, false);
    }

    private IterationResult<TransformIndexerPosition> processChangedBuckets(final SearchResponse searchResponse) {
        nextChangeCollectorBucketPosition = changeCollector.processSearchResponse(searchResponse);

        if (nextChangeCollectorBucketPosition == null) {
            changeCollector.clear();
            return new IterationResult<>(Stream.empty(), null, true);
        }

        // reset the runState to fetch the partial updates next
        runState = RunState.APPLY_RESULTS;

        return new IterationResult<>(Stream.empty(), getPosition(), false);
    }

    protected QueryBuilder buildFilterQuery() {
        assert nextCheckpoint != null;

        QueryBuilder queryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        TransformConfig config = getConfig();
        if (this.isContinuous()) {
            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder);

            if (lastCheckpoint != null) {
                filteredQuery.filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));
            } else {
                filteredQuery.filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));
            }
            return filteredQuery;
        }

        return queryBuilder;
    }

    protected SearchRequest buildSearchRequest() {
        assert nextCheckpoint != null;

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().runtimeMappings(getConfig().getSource().getRuntimeMappings());
        switch (runState) {
            case APPLY_RESULTS:
                buildUpdateQuery(sourceBuilder);
                break;
            case IDENTIFY_CHANGES:
                buildChangedBucketsQuery(sourceBuilder);
                break;
            default:
                // Any other state is a bug, should not happen
                logger.warn("Encountered unexpected run state [" + runState + "]");
                throw new IllegalStateException("Transform indexer job encountered an illegal state [" + runState + "]");
        }

        return new SearchRequest(getConfig().getSource().getIndex()).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .source(sourceBuilder);
    }

    private SearchSourceBuilder buildChangedBucketsQuery(SearchSourceBuilder sourceBuilder) {
        assert isContinuous();

        TransformIndexerPosition position = getPosition();

        changeCollector.buildChangesQuery(sourceBuilder, position != null ? position.getBucketsPosition() : null, pageSize);

        QueryBuilder queryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        TransformConfig config = getConfig();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder)
            .filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));

        // TODO: if buildChangesQuery changes the query it get overwritten
        sourceBuilder.query(filteredQuery);

        logger.debug("[{}] Querying for changes: {}", getJobId(), sourceBuilder);
        return sourceBuilder;
    }

    private SearchSourceBuilder buildUpdateQuery(SearchSourceBuilder sourceBuilder) {
        TransformIndexerPosition position = getPosition();

        TransformConfig config = getConfig();

        function.buildSearchQuery(sourceBuilder, position != null ? position.getIndexerPosition() : null, pageSize);

        QueryBuilder queryBuilder = config.getSource().getQueryConfig().getQuery();

        if (isContinuous()) {
            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder)
                .filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));

            // Only apply extra filter if it is the subsequent run of the continuous transform
            if (nextCheckpoint.getCheckpoint() > 1 && changeCollector != null) {
                QueryBuilder filter = changeCollector.buildFilterQuery(
                    lastCheckpoint.getTimeUpperBound(),
                    nextCheckpoint.getTimeUpperBound()
                );
                if (filter != null) {
                    filteredQuery.filter(filter);
                }
            }

            queryBuilder = filteredQuery;
        }

        sourceBuilder.query(queryBuilder);
        logger.debug(() -> new ParameterizedMessage("[{}] Querying for data: {}", getJobId(), sourceBuilder));

        return sourceBuilder;
    }

    /**
     * Handle the circuit breaking case: A search consumed to much memory and got aborted.
     *
     * Going out of memory we smoothly reduce the page size which reduces memory consumption.
     *
     * Implementation details: We take the values from the circuit breaker as a hint, but
     * note that it breaks early, that's why we also reduce using
     *
     * @param circuitBreakingException CircuitBreakingException thrown
     */
    private void handleCircuitBreakingException(CircuitBreakingException circuitBreakingException) {
        double reducingFactor = Math.min(
            (double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
            1 - (Math.log10(pageSize) * 0.1)
        );

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < MINIMUM_PAGE_SIZE) {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            failIndexer(message);
        } else {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize, newPageSize);
            auditor.info(getJobId(), message);
            logger.info("[{}] {}", getJobId(), message);
            pageSize = newPageSize;
        }
    }

    /**
     * Handle script exception case. This is error is irrecoverable.
     *
     * @param scriptException ScriptException thrown
     */
    private void handleScriptException(ScriptException scriptException) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_SCRIPT_ERROR,
            scriptException.getDetailedMessage(),
            scriptException.getScriptStack()
        );
        failIndexer(message);
    }

    /**
     * Handle permanent bulk indexing exception case. This is error is irrecoverable.
     *
     * @param bulkIndexingException BulkIndexingException thrown
     */
    private void handleIrrecoverableBulkIndexingException(BulkIndexingException bulkIndexingException) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_IRRECOVERABLE_BULK_INDEXING_ERROR,
            bulkIndexingException.getDetailedMessage()
        );
        failIndexer(message);
    }

    protected void failIndexer(String failureMessage) {
        // note: logging and audit is done as part of context.markAsFailed
        context.markAsFailed(failureMessage);
    }

    /**
     * Indicates if an audit message should be written when onFinish is called for the given checkpoint
     * We audit the first checkpoint, and then every 10 checkpoints until completedCheckpoint == 99
     * Then we audit every 100, until completedCheckpoint == 999
     *
     * Then we always audit every 1_000 checkpoints
     *
     * @param completedCheckpoint The checkpoint that was just completed
     * @return {@code true} if an audit message should be written
     */
    protected boolean shouldAuditOnFinish(long completedCheckpoint) {
        if (++logCount % logEvery != 0) {
            return false;
        }
        if (completedCheckpoint == 0) {
            return true;
        }
        int log10Checkpoint = (int) Math.floor(Math.log10(completedCheckpoint));
        logEvery = log10Checkpoint >= 3 ? 1_000 : (int) Math.pow(10.0, log10Checkpoint);
        logCount = 0;
        return true;
    }

    private RunState determineRunStateAtStart() {
        // either 1st run or not a continuous transform
        if (nextCheckpoint.getCheckpoint() == 1 || isContinuous() == false) {
            return RunState.APPLY_RESULTS;
        }

        // if we don't have a change collector or the collector does not require an extra run
        if (changeCollector == null || changeCollector.queryForChanges() == false) {
            return RunState.APPLY_RESULTS;
        }

        // continuous mode: we need to get the changed buckets first
        return RunState.IDENTIFY_CHANGES;
    }

    private void configurePageSize(Integer newPageSize) {
        initialConfiguredPageSize = newPageSize;

        // if the user explicitly set a page size, take it from the config, otherwise let the function decide
        if (initialConfiguredPageSize != null && initialConfiguredPageSize > 0) {
            pageSize = initialConfiguredPageSize;
        } else {
            pageSize = function.getInitialPageSize();
        }
    }

    private synchronized void startIndexerThreadShutdown() {
        indexerThreadShuttingDown = true;
        saveStateRequestedDuringIndexerThreadShutdown = false;
    }

    private synchronized void finishIndexerThreadShutdown() {
        indexerThreadShuttingDown = false;
        if (saveStateRequestedDuringIndexerThreadShutdown) {
            // if stop has been called and set shouldStopAtCheckpoint to true,
            // we should stop if we just finished a checkpoint
            if (context.shouldStopAtCheckpoint() && nextCheckpoint == null) {
                stop();
            }
            doSaveState(getState(), getPosition(), () -> {});
        }
    }

    /**
     * Thrown when the transform configuration disappeared permanently.
     * (not if reloading failed due to an intermittent problem)
     */
    static class TransformConfigLostOnReloadException extends ResourceNotFoundException {
        TransformConfigLostOnReloadException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }
}
