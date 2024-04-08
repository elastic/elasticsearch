/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.RetentionPolicyToDeleteByQueryRequestConverter.RetentionPolicyException;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformSchedulingUtils;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

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

    // Constant for triggering state persistence, used when there are no state persistence errors.
    // In face of errors, exponential backoff scheme is used.
    public static final TimeValue DEFAULT_TRIGGER_SAVE_STATE_INTERVAL = TimeValue.timeValueSeconds(60);

    protected final TransformConfigManager transformsConfigManager;
    private final CheckpointProvider checkpointProvider;
    protected final TransformFailureHandler failureHandler;
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
    private final AtomicInteger remainingCheckpointsUntilAudit = new AtomicInteger(0);
    private volatile TransformCheckpoint lastCheckpoint;
    private volatile TransformCheckpoint nextCheckpoint;

    private volatile RunState runState;

    private volatile long lastCheckpointCleanup = 0L;
    private volatile long lastSaveStateMilliseconds;

    protected volatile boolean indexerThreadShuttingDown = false;
    protected volatile boolean saveStateRequestedDuringIndexerThreadShutdown = false;

    @SuppressWarnings("this-escape")
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
        // important: note that we pass the context object as lock object
        super(threadPool, initialState, initialPosition, jobStats, context);
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

        this.failureHandler = new TransformFailureHandler(auditor, context, transformConfig.getId());
        if (transformConfig.getSettings() != null && transformConfig.getSettings().getDocsPerSecond() != null) {
            docsPerSecond = transformConfig.getSettings().getDocsPerSecond();
        }
        this.lastSaveStateMilliseconds = TimeUnit.NANOSECONDS.toMillis(getTimeNanos());
    }

    abstract void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener);

    abstract void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener);

    abstract void doMaybeCreateDestIndex(Map<String, String> deducedDestIndexMappings, ActionListener<Boolean> listener);

    abstract void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener);

    abstract void refreshDestinationIndex(ActionListener<Void> responseListener);

    abstract void persistState(TransformState state, ActionListener<Void> listener);

    abstract void validate(ActionListener<ValidateTransformAction.Response> listener);

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
        if (saveStateListeners.get() != null) {
            return true;
        }
        long currentTimeMilliseconds = TimeUnit.NANOSECONDS.toMillis(getTimeNanos());
        long nextSaveStateMilliseconds = TransformSchedulingUtils.calculateNextScheduledTime(
            lastSaveStateMilliseconds,
            DEFAULT_TRIGGER_SAVE_STATE_INTERVAL,
            context.getStatePersistenceFailureCount()
        );
        return currentTimeMilliseconds > nextSaveStateMilliseconds;
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
                        logger.warn(() -> "[" + getJobId() + "] failed to create checkpoint.", createCheckpointException);
                        listener.onFailure(
                            new RuntimeException(
                                "Failed to create checkpoint due to: " + createCheckpointException.getMessage(),
                                createCheckpointException
                            )
                        );
                    })
                ),
                getCheckPointException -> {
                    logger.warn(() -> "[" + getJobId() + "] failed to retrieve checkpoint.", getCheckPointException);
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

        if (context.getAuthState() != null && HealthStatus.RED.equals(context.getAuthState().getStatus())) {
            // AuthorizationState status is RED which means there was permission check error during PUT or _update.
            listener.onFailure(
                new ElasticsearchSecurityException(
                    TransformMessages.getMessage(TransformMessages.TRANSFORM_CANNOT_START_WITHOUT_PERMISSIONS, getConfig().getId())
                )
            );
            return;
        }

        ActionListener<Void> finalListener = listener.delegateFailureAndWrap((l, r) -> {
            // if we haven't set the page size yet, if it is set we might have reduced it after running into an out of memory
            if (context.getPageSize() == 0) {
                configurePageSize(getConfig().getSettings().getMaxPageSearchSize());
            }

            runState = determineRunStateAtStart();
            l.onResponse(true);
        });

        // On each run, we need to get the total number of docs and reset the count of processed docs
        // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
        // the progress here, and not in the executor.
        ActionListener<Boolean> configurationReadyListener = ActionListener.wrap(unused -> {
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
                            logger.warn(() -> "[" + getJobId() + "] unable to load progress information for task.", failure);
                            finalListener.onResponse(null);
                        }));
                    }, failure -> {
                        progress = new TransformProgress();
                        logger.warn(() -> "[" + getJobId() + "] unable to load progress information for task.", failure);
                        finalListener.onResponse(null);
                    }));
                }, listener::onFailure));
            } else {
                finalListener.onResponse(null);
            }
        }, listener::onFailure);

        var deducedDestIndexMappings = new SetOnce<Map<String, String>>();
        var shouldMaybeCreateDestIndexForUnattended = context.getCheckpoint() == 0
            && TransformEffectiveSettings.isUnattended(transformConfig.getSettings());

        ActionListener<Map<String, String>> fieldMappingsListener = ActionListener.wrap(destIndexMappings -> {
            if (destIndexMappings.isEmpty() == false) {
                // If we managed to fetch destination index mappings, we use them from now on ...
                this.fieldMappings = destIndexMappings;
            } else {
                // ... otherwise we fall back to index mappings deduced based on source indices
                this.fieldMappings = deducedDestIndexMappings.get();
            }
            // Since the unattended transform could not have created the destination index yet, we do it here.
            // This is important to create the destination index explicitly before indexing first documents. Otherwise, the destination
            // index aliases may be missing.
            if (destIndexMappings.isEmpty() && shouldMaybeCreateDestIndexForUnattended) {
                doMaybeCreateDestIndex(deducedDestIndexMappings.get(), configurationReadyListener);
            } else {
                configurationReadyListener.onResponse(null);
            }
        }, listener::onFailure);

        ActionListener<Void> reLoadFieldMappingsListener = ActionListener.wrap(updateConfigResponse -> {
            logger.debug(() -> format("[%s] Retrieve field mappings from the destination index", getJobId()));

            doGetFieldMappings(fieldMappingsListener);
        }, listener::onFailure);

        // If we are continuous, we will want to verify we have the latest stored configuration
        ActionListener<ValidateTransformAction.Response> changedSourceListener = ActionListener.wrap(validationResponse -> {
            deducedDestIndexMappings.set(validationResponse.getDestIndexMappings());
            if (isContinuous()) {
                transformsConfigManager.getTransformConfiguration(getJobId(), ActionListener.wrap(config -> {
                    if (transformConfig.equals(config) && fieldMappings != null && shouldMaybeCreateDestIndexForUnattended == false) {
                        logger.trace("[{}] transform config has not changed.", getJobId());
                        configurationReadyListener.onResponse(null);
                    } else {
                        transformConfig = config;
                        logger.debug("[{}] successfully refreshed transform config from index.", getJobId());
                        reLoadFieldMappingsListener.onResponse(null);
                    }
                }, failure -> {
                    String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION, getJobId());
                    // If the transform config index or the transform config is gone, something serious occurred
                    // We are in an unknown state and should fail out
                    if (failure instanceof ResourceNotFoundException) {
                        logger.error(msg, failure);
                        reLoadFieldMappingsListener.onFailure(new TransformConfigLostOnReloadException(msg, failure));
                    } else {
                        logger.warn(msg, failure);
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
            checkpointProvider.sourceHasChanged(getLastCheckpoint(), ActionListener.wrap(hasChanged -> {
                context.setLastSearchTime(instantOfTrigger);
                hasSourceChanged = hasChanged;
                if (hasChanged) {
                    context.setChangesLastDetectedAt(instantOfTrigger);
                    logger.debug("[{}] source has changed, triggering new indexer run.", getJobId());
                    changedSourceListener.onResponse(new ValidateTransformAction.Response(emptyMap()));
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
        } else if (context.getCheckpoint() == 0 && TransformEffectiveSettings.isUnattended(transformConfig.getSettings())) {
            // this transform runs in unattended mode and has never run, to go on
            validate(changedSourceListener);
        } else {
            hasSourceChanged = true;
            context.setLastSearchTime(instantOfTrigger);
            context.setChangesLastDetectedAt(instantOfTrigger);
            changedSourceListener.onResponse(new ValidateTransformAction.Response(emptyMap()));
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
            failureHandler.handleIndexerFailure(failure, getConfig().getSettings());
            listener.onFailure(failure);
        });

        try {
            refreshDestinationIndex(ActionListener.wrap(response -> {
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
            () -> format(
                "[%s] Run delete based on retention policy using dbq [%s] with query: [%s]",
                getJobId(),
                deleteByQuery,
                deleteByQuery.getSearchRequest()
            )
        );
        getStats().markStartDelete();

        ActionListener<Void> deleteByQueryAndRefreshDoneListener = ActionListener.wrap(
            unused -> finalizeCheckpoint(listener),
            listener::onFailure
        );

        doDeleteByQuery(deleteByQuery, ActionListener.wrap(bulkByScrollResponse -> {
            logger.trace(() -> format("[%s] dbq response: [%s]", getJobId(), bulkByScrollResponse));

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

            // Since we configure DBQ request *not* to perform a refresh, we need to perform the refresh manually.
            // This separation ensures that the DBQ runs with user permissions and the refresh runs with system permissions.
            refreshDestinationIndex(deleteByQueryAndRefreshDoneListener);
        }, listener::onFailure));
    }

    private void finalizeCheckpoint(ActionListener<Void> listener) {
        try {
            // reset the page size, so we do not memorize a low page size forever
            context.setPageSize(function.getInitialPageSize());
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
                long docsIndexed = progress.getDocumentsIndexed();
                long docsProcessed = progress.getDocumentsProcessed();

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
    public boolean maybeTriggerAsyncJob(long now) {
        // threadpool: trigger_engine_scheduler if triggered from the scheduler, generic if called from the task on start

        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getJobId());
            return false;
        }

        synchronized (context) {
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
            failureHandler.handleIndexerFailure(exc, getConfig().getSettings());
        } catch (Exception e) {
            logger.error(() -> "[" + getJobId() + "] transform encountered an unexpected internal exception: ", e);
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

    @Override
    protected void doSaveState(IndexerState indexerState, TransformIndexerPosition position, Runnable next) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to save state and stats while failed.", getJobId());
            // If we are failed, we should call next to allow failure handling to occur if necessary.
            next.run();
            return;
        }
        if (indexerState.equals(IndexerState.ABORTING)) {
            // If we're aborting, just invoke `next` (which is likely an onFailure handler)
            next.run();
            return;
        }

        // getting the listeners that registered till now, in theory a new listener could sneak in between this line
        // and the next, however this is benign: we would store `shouldStopAtCheckpoint = true` twice which is ok
        Collection<ActionListener<Void>> saveStateListenersAtTheMomentOfCalling = saveStateListeners.getAndSet(null);
        boolean shouldStopAtCheckpoint = context.shouldStopAtCheckpoint();

        // If we should stop at the next checkpoint, are STARTED, and with `initialRun()` we are in one of two states
        // 1. We have just called `onFinish` completing our request, but `shouldStopAtCheckpoint` was set to `true` before our check
        // there and now
        // 2. We are on the very first run of a NEW checkpoint and got here either through a failure, or the very first save state call.
        //
        // In either case, we should stop so that we guarantee a consistent state and that there are no partially completed checkpoints
        if (shouldStopAtCheckpoint && initialRun() && indexerState.equals(IndexerState.STARTED)) {
            indexerState = IndexerState.STOPPED;
            auditor.info(transformConfig.getId(), "Transform is no longer in the middle of a checkpoint, initiating stop.");
            logger.info("[{}] transform is no longer in the middle of a checkpoint, initiating stop.", transformConfig.getId());
        }

        // This means that the indexer was triggered to discover changes, found none, and exited early.
        // If the state is `STOPPED` this means that TransformTask#stop was called while we were checking for changes.
        // Allow the stop call path to continue
        if (hasSourceChanged == false && indexerState.equals(IndexerState.STOPPED) == false) {
            if (saveStateListenersAtTheMomentOfCalling != null) {
                ActionListener.onResponse(saveStateListenersAtTheMomentOfCalling, null);
            }
            next.run();
            return;
        }

        TransformTaskState taskState = context.getTaskState();

        if (indexerState.equals(IndexerState.STARTED) && context.getCheckpoint() == 1 && this.isContinuous() == false) {
            // set both to stopped so they are persisted as such
            indexerState = IndexerState.STOPPED;

            auditor.info(transformConfig.getId(), "Transform finished indexing all data, initiating stop");
            logger.info("[{}] transform finished indexing all data, initiating stop.", transformConfig.getId());
        }

        // If we are `STOPPED` on a `doSaveState` call, that indicates we transitioned to `STOPPED` from `STOPPING`
        // OR we called `doSaveState` manually as the indexer was not actively running.
        // Since we save the state to an index, we should make sure that our task state is in parity with the indexer state
        if (indexerState.equals(IndexerState.STOPPED)) {
            // If we are going to stop after the state is saved, we should NOT persist `shouldStopAtCheckpoint: true` as this may
            // cause problems if the task starts up again.
            // Additionally, we don't have to worry about inconsistency with the ClusterState (if it is persisted there) as the
            // when we stop, we mark the task as complete and that state goes away.
            shouldStopAtCheckpoint = false;

            // We don't want adjust the stored taskState because as soon as it is `STOPPED` a user could call
            // .start again.
            taskState = TransformTaskState.STOPPED;
        }

        final TransformState state = new TransformState(
            taskState,
            indexerState,
            position,
            context.getCheckpoint(),
            context.getStateReason(),
            getProgress(),
            null,
            shouldStopAtCheckpoint,
            context.getAuthState()
        );
        logger.debug("[{}] updating persistent state of transform to [{}].", transformConfig.getId(), state.toString());

        // we might need to call the save state listeners, but do not want to stop rolling
        persistStateWithAutoStop(state, ActionListener.wrap(r -> {
            try {
                if (saveStateListenersAtTheMomentOfCalling != null) {
                    ActionListener.onResponse(saveStateListenersAtTheMomentOfCalling, r);
                }
            } catch (Exception onResponseException) {
                String msg = LoggerMessageFormat.format("[{}] failed notifying saveState listeners, ignoring.", getJobId());
                logger.warn(msg, onResponseException);
            } finally {
                lastSaveStateMilliseconds = TimeUnit.NANOSECONDS.toMillis(getTimeNanos());
                next.run();
            }
        }, e -> {
            try {
                if (saveStateListenersAtTheMomentOfCalling != null) {
                    ActionListener.onFailure(saveStateListenersAtTheMomentOfCalling, e);
                }
            } catch (Exception onFailureException) {
                String msg = LoggerMessageFormat.format("[{}] failed notifying saveState listeners, ignoring.", getJobId());
                logger.warn(msg, onFailureException);
            } finally {
                next.run();
            }
        }));
    }

    private void persistStateWithAutoStop(TransformState state, ActionListener<Void> listener) {
        persistState(state, ActionListener.runBefore(listener, () -> {
            if (state.getTaskState().equals(TransformTaskState.STOPPED)) {
                context.shutdown();
            }
        }));
    }

    /**
     * Let the indexer stop at the next checkpoint and call the listener after the flag has been persisted in state.
     *
     * If the indexer isn't running, persist state if required and call the listener immediately.
     */
    final void setStopAtCheckpoint(boolean shouldStopAtCheckpoint, ActionListener<Void> shouldStopAtCheckpointListener) {
        // this should be called from the generic threadpool
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

        try {
            if (addSetStopAtCheckpointListener(shouldStopAtCheckpoint, shouldStopAtCheckpointListener) == false) {
                shouldStopAtCheckpointListener.onResponse(null);
            }
        } catch (InterruptedException e) {
            logger.error(
                () -> format(
                    "[%s] Interrupt waiting (%ss) for transform state to be stored.",
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
            logger.error(() -> "[" + getJobId() + "] failed to persist transform state.", e);
            shouldStopAtCheckpointListener.onFailure(e);
        }
    }

    private boolean addSetStopAtCheckpointListener(boolean shouldStopAtCheckpoint, ActionListener<Void> shouldStopAtCheckpointListener)
        throws InterruptedException {

        synchronized (context) {
            // in case the indexer is already shutting down
            if (indexerThreadShuttingDown) {
                context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
                saveStateRequestedDuringIndexerThreadShutdown = true;
                return false;
            }

            IndexerState state = getState();

            // in case the indexer isn't running, respond immediately
            if (state == IndexerState.STARTED && context.shouldStopAtCheckpoint() != shouldStopAtCheckpoint) {
                IndexerState newIndexerState = IndexerState.STARTED;
                TransformTaskState newtaskState = context.getTaskState();

                // check if the transform is at a checkpoint, if so, we will shortcut and stop it below
                // otherwise we set shouldStopAtCheckpoint, for this case the transform needs to get
                // triggered, complete the checkpoint and stop
                if (shouldStopAtCheckpoint && initialRun()) {
                    newIndexerState = IndexerState.STOPPED;
                    newtaskState = TransformTaskState.STOPPED;
                    logger.debug("[{}] transform is at a checkpoint, initiating stop.", transformConfig.getId());
                } else {
                    context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
                }

                final TransformState newTransformState = new TransformState(
                    newtaskState,
                    newIndexerState,
                    getPosition(),
                    context.getCheckpoint(),
                    context.getStateReason(),
                    getProgress(),
                    null,
                    newIndexerState == IndexerState.STARTED,
                    context.getAuthState()
                );

                // because save state is async we need to block the call until state is persisted, so that the job can not
                // be triggered (ensured by synchronized)
                CountDownLatch latch = new CountDownLatch(1);
                logger.debug("[{}] persisting stop at checkpoint", getJobId());

                persistState(newTransformState, ActionListener.running(() -> latch.countDown()));

                if (latch.await(PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                    logger.error(
                        () -> format(
                            "[%s] Timed out (%ss) waiting for transform state to be stored.",
                            getJobId(),
                            PERSIST_STOP_AT_CHECKPOINT_TIMEOUT_SEC
                        )
                    );
                }

                // stop the transform if the decision was to stop it above
                if (newtaskState.equals(TransformTaskState.STOPPED)) {
                    context.shutdown();
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
        }
        // in case of throttling the indexer might wait for the next search, fast forward, so stop listeners do not wait to long
        runSearchImmediately();
        return true;
    }

    void stopAndMaybeSaveState() {
        synchronized (context) {
            onStop();
            IndexerState state = stop();

            if (indexerThreadShuttingDown) {
                saveStateRequestedDuringIndexerThreadShutdown = true;
                // if stop() returned STOPPED we need to persist state, otherwise the indexer does it for us
            } else if (state == IndexerState.STOPPED) {
                doSaveState(IndexerState.STOPPED, getPosition(), () -> {});
            }
        }
    }

    /**
     * Checks the given exception and handles the error based on it.
     *
     * In case the error is permanent or the number for failures exceed the number of retries, sets the indexer
     * to `FAILED`.
     *
     * Important: Might call into TransformTask, this should _only_ be called with an acquired indexer lock if and only if
     * the lock for TransformTask has been acquired, too. See gh#75846
     *
     * (Note: originally this method was synchronized, which is not necessary)
     */
    void handleFailure(Exception e) {
        failureHandler.handleIndexerFailure(e, getConfig().getSettings());
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
                    logger.warn(() -> "[" + getJobId() + "] failed to cleanup old checkpoints, retrying after next checkpoint", e);
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

    protected Tuple<String, SearchRequest> buildSearchRequest() {
        assert nextCheckpoint != null;

        switch (runState) {
            case APPLY_RESULTS:
                return new Tuple<>("apply_results", buildQueryToUpdateDestinationIndex());
            case IDENTIFY_CHANGES:
                return new Tuple<>("identify_changes", buildQueryToFindChanges());
            default:
                // Any other state is a bug, should not happen
                logger.warn("Encountered unexpected run state [" + runState + "]");
                throw new IllegalStateException("Transform indexer job encountered an illegal state [" + runState + "]");
        }
    }

    private SearchRequest buildQueryToFindChanges() {
        assert isContinuous();

        TransformIndexerPosition position = getPosition();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().runtimeMappings(getConfig().getSource().getRuntimeMappings());

        // reduce the indexes to query to the ones that have changes
        SearchRequest request = new SearchRequest(
            /*
             * gh#77329 optimization turned off, gh#81252 transform can fail if an index gets deleted during searches
             *
             * Until proper checkpoint searches (seq_id per shard) are possible, we have to query
             *  - all indices
             *  - resolve indices at search
             *
             * TransformCheckpoint.getChangedIndices(TransformCheckpoint.EMPTY, getNextCheckpoint()).toArray(new String[0])
             */
            getConfig().getSource().getIndex()
        );

        request.allowPartialSearchResults(false) // shard failures should fail the request
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN); // TODO: make configurable

        changeCollector.buildChangesQuery(sourceBuilder, position != null ? position.getBucketsPosition() : null, context.getPageSize());

        QueryBuilder queryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        TransformConfig config = getConfig();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder)
            .filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));

        // TODO: if buildChangesQuery changes the query it get overwritten
        sourceBuilder.query(filteredQuery);

        logger.debug("[{}] Querying {} for changes: {}", getJobId(), request.indices(), sourceBuilder);
        return request.source(sourceBuilder);
    }

    private SearchRequest buildQueryToUpdateDestinationIndex() {
        TransformIndexerPosition position = getPosition();

        TransformConfig config = getConfig();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().runtimeMappings(getConfig().getSource().getRuntimeMappings());

        function.buildSearchQuery(sourceBuilder, position != null ? position.getIndexerPosition() : null, context.getPageSize());

        SearchRequest request = new SearchRequest();
        QueryBuilder queryBuilder = config.getSource().getQueryConfig().getQuery();

        if (isContinuous()) {
            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(queryBuilder)
                .filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));

            // Only apply extra filter if it is the subsequent run of the continuous transform
            if (changeCollector != null) {
                QueryBuilder filter = changeCollector.buildFilterQuery(lastCheckpoint, nextCheckpoint);
                if (filter != null) {
                    filteredQuery.filter(filter);
                }
                /*
                 * gh#81252 transform can fail if an index gets deleted during searches
                 *
                 * Until proper checkpoint searches (seq_id per shard) are possible, we have to query
                 *  - all indices
                 *  - resolve indices at search time
                 *
                 * request.indices(changeCollector.getIndicesToQuery(lastCheckpoint, nextCheckpoint).toArray(new String[0]));
                 */
                request.indices(getConfig().getSource().getIndex());
            } else {
                request.indices(getConfig().getSource().getIndex());
            }

            queryBuilder = filteredQuery;

        } else {
            request.indices(getConfig().getSource().getIndex());
        }

        sourceBuilder.query(queryBuilder);
        logger.debug("[{}] Querying {} for data: {}", getJobId(), request.indices(), sourceBuilder);

        return request.source(sourceBuilder)
            .allowPartialSearchResults(false) // shard failures should fail the request
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN); // TODO: make configurable
    }

    /**
     * Indicates if an audit message should be written when onFinish is called for the given checkpoint.
     * We audit every checkpoint for the first 10 checkpoints until completedCheckpoint == 9.
     * Then we audit every 10th checkpoint until completedCheckpoint == 99.
     * Then we audit every 100th checkpoint until completedCheckpoint == 999.
     * Then we always audit every 1_000th checkpoints.
     *
     * @param completedCheckpoint The checkpoint that was just completed
     * @return {@code true} if an audit message should be written
     */
    protected boolean shouldAuditOnFinish(long completedCheckpoint) {
        return remainingCheckpointsUntilAudit.getAndUpdate(count -> {
            if (count > 0) {
                return count - 1;
            }

            if (completedCheckpoint >= 1000) {
                return 999;
            } else if (completedCheckpoint >= 100) {
                return 99;
            } else if (completedCheckpoint >= 10) {
                return 9;
            } else {
                return 0;
            }
        }) == 0;
    }

    private RunState determineRunStateAtStart() {
        if (context.from() != null && changeCollector != null && changeCollector.queryForChanges()) {
            return RunState.IDENTIFY_CHANGES;
        }

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
            context.setPageSize(initialConfiguredPageSize);
        } else {
            context.setPageSize(function.getInitialPageSize());
        }
    }

    private void startIndexerThreadShutdown() {
        synchronized (context) {
            indexerThreadShuttingDown = true;
            saveStateRequestedDuringIndexerThreadShutdown = false;
        }
    }

    private void finishIndexerThreadShutdown() {
        synchronized (context) {
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
