/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDoc;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.dataframe.transforms.pivot.AggregationResultUtils;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ClientDataFrameIndexer extends DataFrameIndexer {

    private static final Logger logger = LogManager.getLogger(ClientDataFrameIndexer.class);

    private long logEvery = 1;
    private long logCount = 0;
    private final Client client;
    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final CheckpointProvider checkpointProvider;
    private final DataFrameTransformTask transformTask;
    private final AtomicInteger failureCount;
    private volatile boolean auditBulkFailures = true;
    // Indicates that the source has changed for the current run
    private volatile boolean hasSourceChanged = true;
    // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
    private volatile String lastAuditedExceptionMessage = null;
    private final AtomicBoolean oldStatsCleanedUp = new AtomicBoolean(false);
    private volatile Instant changesLastDetectedAt;

    ClientDataFrameIndexer(DataFrameTransformsConfigManager transformsConfigManager,
                           CheckpointProvider checkpointProvider,
                           AtomicReference<IndexerState> initialState,
                           DataFrameIndexerPosition initialPosition,
                           Client client,
                           DataFrameAuditor auditor,
                           DataFrameIndexerTransformStats initialStats,
                           DataFrameTransformConfig transformConfig,
                           Map<String, String> fieldMappings,
                           DataFrameTransformProgress transformProgress,
                           DataFrameTransformCheckpoint lastCheckpoint,
                           DataFrameTransformCheckpoint nextCheckpoint,
                           DataFrameTransformTask parentTask) {
        super(ExceptionsHelper.requireNonNull(parentTask, "parentTask")
                .getThreadPool()
                .executor(ThreadPool.Names.GENERIC),
            ExceptionsHelper.requireNonNull(auditor, "auditor"),
            transformConfig,
            fieldMappings,
            ExceptionsHelper.requireNonNull(initialState, "initialState"),
            initialPosition,
            initialStats == null ? new DataFrameIndexerTransformStats() : initialStats,
            transformProgress,
            lastCheckpoint,
            nextCheckpoint);
        this.transformsConfigManager = ExceptionsHelper.requireNonNull(transformsConfigManager, "transformsConfigManager");
        this.checkpointProvider = ExceptionsHelper.requireNonNull(checkpointProvider, "checkpointProvider");

        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.transformTask = parentTask;
        this.failureCount = new AtomicInteger(0);
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        if (transformTask.getTaskState() == DataFrameTransformTaskState.FAILED) {
            logger.debug("[{}] attempted to start while failed.", getJobId());
            listener.onFailure(new ElasticsearchException("Attempted to start a failed transform [{}].", getJobId()));
            return;
        }
        // On each run, we need to get the total number of docs and reset the count of processed docs
        // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
        // the progress here, and not in the executor.
        ActionListener<Void> updateConfigListener = ActionListener.wrap(
            updateConfigResponse -> {
                if (initialRun()) {
                    createCheckpoint(ActionListener.wrap(cp -> {
                        nextCheckpoint = cp;
                        // If nextCheckpoint > 1, this means that we are now on the checkpoint AFTER the batch checkpoint
                        // Consequently, the idea of percent complete no longer makes sense.
                        if (nextCheckpoint.getCheckpoint() > 1) {
                            progress = new DataFrameTransformProgress(null, 0L, 0L);
                            super.onStart(now, listener);
                            return;
                        }
                        TransformProgressGatherer.getInitialProgress(this.client, buildFilterQuery(), getConfig(), ActionListener.wrap(
                            newProgress -> {
                                logger.trace("[{}] reset the progress from [{}] to [{}].", getJobId(), progress, newProgress);
                                progress = newProgress;
                                super.onStart(now, listener);
                            },
                            failure -> {
                                progress = null;
                                logger.warn(new ParameterizedMessage("[{}] unable to load progress information for task.",
                                    getJobId()),
                                    failure);
                                super.onStart(now, listener);
                            }
                        ));
                    }, listener::onFailure));
                } else {
                    super.onStart(now, listener);
                }
            },
            listener::onFailure
        );

        // If we are continuous, we will want to verify we have the latest stored configuration
        ActionListener<Void> changedSourceListener = ActionListener.wrap(
            r -> {
                if (isContinuous()) {
                    transformsConfigManager.getTransformConfiguration(getJobId(), ActionListener.wrap(
                        config -> {
                            transformConfig = config;
                            logger.debug("[{}] successfully refreshed data frame transform config from index.", getJobId());
                            updateConfigListener.onResponse(null);
                        },
                        failure -> {
                            String msg = DataFrameMessages.getMessage(
                                DataFrameMessages.FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION,
                                getJobId());
                            logger.error(msg, failure);
                            // If the transform config index or the transform config is gone, something serious occurred
                            // We are in an unknown state and should fail out
                            if (failure instanceof ResourceNotFoundException) {
                                updateConfigListener.onFailure(new TransformConfigReloadingException(msg, failure));
                            } else {
                                auditor.warning(getJobId(), msg);
                                updateConfigListener.onResponse(null);
                            }
                        }
                    ));
                } else {
                    updateConfigListener.onResponse(null);
                }
            },
            listener::onFailure
        );

        // If we are not on the initial batch checkpoint and its the first pass of whatever continuous checkpoint we are on,
        // we should verify if there are local changes based on the sync config. If not, do not proceed further and exit.
        if (transformTask.getCheckpoint() > 0 && initialRun()) {
            sourceHasChanged(ActionListener.wrap(
                hasChanged -> {
                    hasSourceChanged = hasChanged;
                    if (hasChanged) {
                        changesLastDetectedAt = Instant.now();
                        logger.debug("[{}] source has changed, triggering new indexer run.", getJobId());
                        changedSourceListener.onResponse(null);
                    } else {
                        logger.trace("[{}] source has not changed, finish indexer early.", getJobId());
                        // No changes, stop executing
                        listener.onResponse(false);
                    }
                },
                failure -> {
                    // If we failed determining if the source changed, it's safer to assume there were changes.
                    // We should allow the failure path to complete as normal
                    hasSourceChanged = true;
                    listener.onFailure(failure);
                }
            ));
        } else {
            hasSourceChanged = true;
            changedSourceListener.onResponse(null);
        }
    }

    public CheckpointProvider getCheckpointProvider() {
        return checkpointProvider;
    }

    Instant getChangesLastDetectedAt() {
        return changesLastDetectedAt;
    }

    @Override
    public synchronized boolean maybeTriggerAsyncJob(long now) {
        if (transformTask.getTaskState() == DataFrameTransformTaskState.FAILED) {
            logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getJobId());
            return false;
        }

        // ignore trigger if indexer is running, prevents log spam in A2P indexer
        IndexerState indexerState = getState();
        if (IndexerState.INDEXING.equals(indexerState) || IndexerState.STOPPING.equals(indexerState)) {
            logger.debug("[{}] indexer for transform has state [{}]. Ignoring trigger.", getJobId(), indexerState);
            return false;
        }

        return super.maybeTriggerAsyncJob(now);
    }

    @Override
    protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
        if (transformTask.getTaskState() == DataFrameTransformTaskState.FAILED) {
            logger.debug("[{}] attempted to search while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a search request for failed transform [{}].",
                getJobId()));
            return;
        }
        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client,
                SearchAction.INSTANCE, request, nextPhase);
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        if (transformTask.getTaskState() == DataFrameTransformTaskState.FAILED) {
            logger.debug("[{}] attempted to bulk index while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a bulk index request for failed transform [{}].",
                getJobId()));
            return;
        }
        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(),
            ClientHelper.DATA_FRAME_ORIGIN,
            client,
            BulkAction.INSTANCE,
            request,
            ActionListener.wrap(bulkResponse -> {
                if (bulkResponse.hasFailures()) {
                    int failureCount = 0;
                    for(BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            failureCount++;
                        }
                        // TODO gather information on irrecoverable failures and update isIrrecoverableFailure
                    }
                    if (auditBulkFailures) {
                        auditor.warning(getJobId(),
                            "Experienced at least [" +
                                failureCount +
                                "] bulk index failures. See the logs of the node running the transform for details. " +
                                bulkResponse.buildFailureMessage());
                        auditBulkFailures = false;
                    }
                    // This calls AsyncTwoPhaseIndexer#finishWithIndexingFailure
                    // It increments the indexing failure, and then calls the `onFailure` logic
                    nextPhase.onFailure(
                        new BulkIndexingException("Bulk index experienced failures. " +
                            "See the logs of the node running the transform for details."));
                } else {
                    auditBulkFailures = true;
                    nextPhase.onResponse(bulkResponse);
                }
            }, nextPhase::onFailure));
    }

    @Override
    protected void doSaveState(IndexerState indexerState, DataFrameIndexerPosition position, Runnable next) {
        if (transformTask.getTaskState() == DataFrameTransformTaskState.FAILED) {
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

        // This means that the indexer was triggered to discover changes, found none, and exited early.
        // If the state is `STOPPED` this means that DataFrameTransformTask#stop was called while we were checking for changes.
        // Allow the stop call path to continue
        if (hasSourceChanged == false && indexerState.equals(IndexerState.STOPPED) == false) {
            next.run();
            return;
        }

        DataFrameTransformTaskState taskState = transformTask.getTaskState();

        if (indexerState.equals(IndexerState.STARTED)
            && transformTask.getCheckpoint() == 1
            && this.isContinuous() == false) {
            // set both to stopped so they are persisted as such
            indexerState = IndexerState.STOPPED;

            auditor.info(transformConfig.getId(), "Data frame finished indexing all data, initiating stop");
            logger.info("[{}] data frame transform finished indexing all data, initiating stop.", transformConfig.getId());
        }

        // If we are `STOPPED` on a `doSaveState` call, that indicates we transitioned to `STOPPED` from `STOPPING`
        // OR we called `doSaveState` manually as the indexer was not actively running.
        // Since we save the state to an index, we should make sure that our task state is in parity with the indexer state
        if (indexerState.equals(IndexerState.STOPPED)) {
            // We don't want adjust the stored taskState because as soon as it is `STOPPED` a user could call
            // .start again.
            taskState = DataFrameTransformTaskState.STOPPED;
        }

        final DataFrameTransformState state = new DataFrameTransformState(
            taskState,
            indexerState,
            position,
            transformTask.getCheckpoint(),
            transformTask.getStateReason(),
            getProgress());
        logger.debug("[{}] updating persistent state of transform to [{}].", transformConfig.getId(), state.toString());

        // This could be `null` but the putOrUpdateTransformStoredDoc handles that case just fine
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = transformTask.getSeqNoPrimaryTermAndIndex();

        // Persist the current state and stats in the internal index. The interval of this method being
        // called is controlled by AsyncTwoPhaseIndexer#onBulkResponse which calls doSaveState every so
        // often when doing bulk indexing calls or at the end of one indexing run.
        transformsConfigManager.putOrUpdateTransformStoredDoc(
                new DataFrameTransformStoredDoc(getJobId(), state, getStats()),
                seqNoPrimaryTermAndIndex,
                ActionListener.wrap(
                        r -> {
                            transformTask.updateSeqNoPrimaryTermAndIndex(seqNoPrimaryTermAndIndex, r);
                            // for auto stop shutdown the task
                            if (state.getTaskState().equals(DataFrameTransformTaskState.STOPPED)) {
                                transformTask.shutdown();
                            }
                            // Only do this clean up once, if it succeeded, no reason to do the query again.
                            if (oldStatsCleanedUp.compareAndSet(false, true)) {
                                transformsConfigManager.deleteOldTransformStoredDocuments(getJobId(), ActionListener.wrap(
                                    nil -> {
                                        logger.trace("[{}] deleted old transform stats and state document", getJobId());
                                        next.run();
                                    },
                                    e -> {
                                        String msg = LoggerMessageFormat.format("[{}] failed deleting old transform configurations.",
                                            getJobId());
                                        logger.warn(msg, e);
                                        // If we have failed, we should attempt the clean up again later
                                        oldStatsCleanedUp.set(false);
                                        next.run();
                                    }
                                ));
                            } else {
                                next.run();
                            }
                        },
                        statsExc -> {
                            logger.error(new ParameterizedMessage("[{}] updating stats of transform failed.",
                                transformConfig.getId()),
                                statsExc);
                            auditor.warning(getJobId(),
                                "Failure updating stats of transform: " + statsExc.getMessage());
                            // for auto stop shutdown the task
                            if (state.getTaskState().equals(DataFrameTransformTaskState.STOPPED)) {
                                transformTask.shutdown();
                            }
                            next.run();
                        }
                ));
    }

    @Override
    protected void onFailure(Exception exc) {
        // the failure handler must not throw an exception due to internal problems
        try {
            handleFailure(exc);
        } catch (Exception e) {
            logger.error(
                new ParameterizedMessage("[{}] data frame transform encountered an unexpected internal exception: ", getJobId()),
                e);
        }
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        try {
            // This indicates an early exit since no changes were found.
            // So, don't treat this like a checkpoint being completed, as no work was done.
            if (hasSourceChanged == false) {
                listener.onResponse(null);
                return;
            }
            // TODO: needs cleanup super is called with a listener, but listener.onResponse is called below
            // super.onFinish() fortunately ignores the listener
            super.onFinish(listener);
            long checkpoint = transformTask.incrementCheckpoint();
            lastCheckpoint = getNextCheckpoint();
            nextCheckpoint = null;
            // Reset our failure count as we have finished and may start again with a new checkpoint
            failureCount.set(0);
            transformTask.setStateReason(null);

            // With bucket_selector we could have read all the buckets and completed the transform
            // but not "see" all the buckets since they were filtered out. Consequently, progress would
            // show less than 100% even though we are done.
            // NOTE: this method is called in the same thread as the processing thread.
            // Theoretically, there should not be a race condition with updating progress here.
            // NOTE 2: getPercentComplete should only NOT be null on the first (batch) checkpoint
            if (progress != null && progress.getPercentComplete() != null && progress.getPercentComplete() < 100.0) {
                progress.incrementDocsProcessed(progress.getTotalDocs() - progress.getDocumentsProcessed());
            }
            // If the last checkpoint is now greater than 1, that means that we have just processed the first
            // continuous checkpoint and should start recording the exponential averages
            if (lastCheckpoint != null && lastCheckpoint.getCheckpoint() > 1) {
                long docsIndexed = 0;
                long docsProcessed = 0;
                // This should not happen as we simply create a new one when we reach continuous checkpoints
                // but this is a paranoid `null` check
                if (progress != null) {
                    docsIndexed = progress.getDocumentsIndexed();
                    docsProcessed = progress.getDocumentsProcessed();
                }
                long durationMs = System.currentTimeMillis() - lastCheckpoint.getTimestamp();
                getStats().incrementCheckpointExponentialAverages(durationMs < 0 ? 0 : durationMs, docsIndexed, docsProcessed);
            }
            if (shouldAuditOnFinish(checkpoint)) {
                auditor.info(getJobId(),
                    "Finished indexing for data frame transform checkpoint [" + checkpoint + "].");
            }
            logger.debug(
                "[{}] finished indexing for data frame transform checkpoint [{}].", getJobId(), checkpoint);
            auditBulkFailures = true;
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
        logEvery = log10Checkpoint >= 3  ? 1_000 : (int)Math.pow(10.0, log10Checkpoint);
        logCount = 0;
        return true;
    }

    @Override
    protected void onStop() {
        auditor.info(transformConfig.getId(), "Data frame transform has stopped.");
        logger.info("[{}] data frame transform has stopped.", transformConfig.getId());
    }

    @Override
    protected void onAbort() {
        auditor.info(transformConfig.getId(), "Received abort request, stopping data frame transform.");
        logger.info("[{}] data frame transform received abort request. Stopping indexer.", transformConfig.getId());
        transformTask.shutdown();
    }

    @Override
    protected void createCheckpoint(ActionListener<DataFrameTransformCheckpoint> listener) {
        checkpointProvider.createNextCheckpoint(getLastCheckpoint(), ActionListener.wrap(
                checkpoint -> transformsConfigManager.putTransformCheckpoint(checkpoint,
                    ActionListener.wrap(
                        putCheckPointResponse -> listener.onResponse(checkpoint),
                        createCheckpointException -> {
                            logger.warn(new ParameterizedMessage("[{}] failed to create checkpoint.", getJobId()),
                                createCheckpointException);
                            listener.onFailure(
                                new RuntimeException("Failed to create checkpoint due to " + createCheckpointException.getMessage(),
                                    createCheckpointException));
                        }
                )),
                getCheckPointException -> {
                    logger.warn(new ParameterizedMessage("[{}] failed to retrieve checkpoint.", getJobId()),
                        getCheckPointException);
                    listener.onFailure(
                        new RuntimeException("Failed to retrieve checkpoint due to " + getCheckPointException.getMessage(),
                            getCheckPointException));
                }
        ));
    }

    @Override
    protected void sourceHasChanged(ActionListener<Boolean> hasChangedListener) {
        checkpointProvider.sourceHasChanged(getLastCheckpoint(),
            ActionListener.wrap(
                hasChanged -> {
                    logger.trace("[{}] change detected [{}].", getJobId(), hasChanged);
                    hasChangedListener.onResponse(hasChanged);
                },
                e -> {
                    logger.warn(
                        new ParameterizedMessage(
                            "[{}] failed to detect changes for data frame transform. Skipping update till next check.",
                            getJobId()),
                        e);
                    auditor.warning(getJobId(),
                        "Failed to detect changes for data frame transform, skipping update till next check. Exception: "
                            + e.getMessage());
                    hasChangedListener.onResponse(false);
                }));
    }

    private boolean isIrrecoverableFailure(Exception e) {
        return e instanceof IndexNotFoundException
            || e instanceof AggregationResultUtils.AggregationExtractionException
            || e instanceof TransformConfigReloadingException;
    }

    synchronized void handleFailure(Exception e) {
        logger.warn(new ParameterizedMessage("[{}] data frame transform encountered an exception: ",
            getJobId()),
            e);
        if (handleCircuitBreakingException(e)) {
            return;
        }

        if (isIrrecoverableFailure(e) || failureCount.incrementAndGet() > transformTask.getNumFailureRetries()) {
            String failureMessage = isIrrecoverableFailure(e) ?
                "task encountered irrecoverable failure: " + e.getMessage() :
                "task encountered more than " + transformTask.getNumFailureRetries() + " failures; latest failure: " + e.getMessage();
            failIndexer(failureMessage);
        } else {
            // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
            // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
            if (e.getMessage().equals(lastAuditedExceptionMessage) == false) {
                auditor.warning(getJobId(),
                    "Data frame transform encountered an exception: " + e.getMessage() +
                        " Will attempt again at next scheduled trigger.");
                lastAuditedExceptionMessage = e.getMessage();
            }
        }
    }

    @Override
    protected void failIndexer(String failureMessage) {
        logger.error("[{}] transform has failed; experienced: [{}].", getJobId(), failureMessage);
        auditor.error(getJobId(), failureMessage);
        transformTask.markAsFailed(failureMessage, ActionListener.wrap(
            r -> {
                // Successfully marked as failed, reset counter so that task can be restarted
                failureCount.set(0);
            }, e -> {}));
    }

    // Considered a recoverable indexing failure
    private static class BulkIndexingException extends ElasticsearchException {
        BulkIndexingException(String msg, Object... args) {
            super(msg, args);
        }
    }

    private static class TransformConfigReloadingException extends ElasticsearchException {
        TransformConfigReloadingException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }
}
