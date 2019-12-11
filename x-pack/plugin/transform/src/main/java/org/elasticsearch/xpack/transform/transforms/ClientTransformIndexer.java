/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ClientTransformIndexer extends TransformIndexer {

    private static final Logger logger = LogManager.getLogger(ClientTransformIndexer.class);

    private final Client client;
    private final AtomicBoolean oldStatsCleanedUp = new AtomicBoolean(false);

    private final AtomicReference<SeqNoPrimaryTermAndIndex> seqNoPrimaryTermAndIndex;

    ClientTransformIndexer(
        Executor executor,
        TransformConfigManager transformsConfigManager,
        CheckpointProvider checkpointProvider,
        TransformProgressGatherer progressGatherer,
        AtomicReference<IndexerState> initialState,
        TransformIndexerPosition initialPosition,
        Client client,
        TransformAuditor auditor,
        TransformIndexerStats initialStats,
        TransformConfig transformConfig,
        Map<String, String> fieldMappings,
        TransformProgress transformProgress,
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        TransformContext context,
        boolean shouldStopAtCheckpoint
    ) {
        super(
            ExceptionsHelper.requireNonNull(executor, "executor"),
            transformsConfigManager,
            checkpointProvider,
            progressGatherer,
            auditor,
            transformConfig,
            fieldMappings,
            ExceptionsHelper.requireNonNull(initialState, "initialState"),
            initialPosition,
            initialStats == null ? new TransformIndexerStats() : initialStats,
            transformProgress,
            lastCheckpoint,
            nextCheckpoint,
            context
        );
        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.seqNoPrimaryTermAndIndex = new AtomicReference<>(seqNoPrimaryTermAndIndex);

        // TODO: move into context constructor
        context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
    }

    void persistShouldStopAtCheckpoint(boolean shouldStopAtCheckpoint, ActionListener<Void> shouldStopAtCheckpointListener) {
        if (context.shouldStopAtCheckpoint() == shouldStopAtCheckpoint
            || getState() == IndexerState.STOPPED
            || getState() == IndexerState.STOPPING) {
            shouldStopAtCheckpointListener.onResponse(null);
            return;
        }
        TransformState state = new TransformState(
            context.getTaskState(),
            getState(),
            getPosition(),
            context.getCheckpoint(),
            context.getStateReason(),
            getProgress(),
            null, // Node attributes
            shouldStopAtCheckpoint
        );
        doSaveState(state, ActionListener.wrap(r -> {
            // We only want to update this internal value if it is persisted as such
            context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
            logger.debug("[{}] successfully persisted should_stop_at_checkpoint update [{}]", getJobId(), shouldStopAtCheckpoint);
            shouldStopAtCheckpointListener.onResponse(null);
        }, statsExc -> {
            logger.warn("[{}] failed to persist should_stop_at_checkpoint update [{}]", getJobId(), shouldStopAtCheckpoint);
            shouldStopAtCheckpointListener.onFailure(statsExc);
        }));
    }

    @Override
    protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to search while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a search request for failed transform [{}].", getJobId()));
            return;
        }
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            request,
            nextPhase
        );
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to bulk index while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a bulk index request for failed transform [{}].", getJobId()));
            return;
        }
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            BulkAction.INSTANCE,
            request,
            ActionListener.wrap(bulkResponse -> {
                if (bulkResponse.hasFailures()) {
                    int failureCount = 0;
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            failureCount++;
                        }
                        // TODO gather information on irrecoverable failures and update isIrrecoverableFailure
                    }
                    if (auditBulkFailures) {
                        String failureMessage = bulkResponse.buildFailureMessage();
                        logger.debug("[{}] Bulk index failure encountered: {}", getJobId(), failureMessage);
                        auditor.warning(
                            getJobId(),
                            "Experienced at least ["
                                + failureCount
                                + "] bulk index failures. See the logs of the node running the transform for details. "
                                + failureMessage
                        );
                        auditBulkFailures = false;
                    }
                    // This calls AsyncTwoPhaseIndexer#finishWithIndexingFailure
                    // It increments the indexing failure, and then calls the `onFailure` logic
                    nextPhase.onFailure(
                        new BulkIndexingException(
                            "Bulk index experienced failures. " + "See the logs of the node running the transform for details."
                        )
                    );
                } else {
                    auditBulkFailures = true;
                    nextPhase.onResponse(bulkResponse);
                }
            }, nextPhase::onFailure)
        );
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
            shouldStopAtCheckpoint
        );
        logger.debug("[{}] updating persistent state of transform to [{}].", transformConfig.getId(), state.toString());

        doSaveState(state, ActionListener.wrap(r -> next.run(), e -> next.run()));
    }

    private void doSaveState(TransformState state, ActionListener<Void> listener) {

        // This could be `null` but the putOrUpdateTransformStoredDoc handles that case just fine
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = getSeqNoPrimaryTermAndIndex();

        // Persist the current state and stats in the internal index. The interval of this method being
        // called is controlled by AsyncTwoPhaseIndexer#onBulkResponse which calls doSaveState every so
        // often when doing bulk indexing calls or at the end of one indexing run.
        transformsConfigManager.putOrUpdateTransformStoredDoc(
            new TransformStoredDoc(getJobId(), state, getStats()),
            seqNoPrimaryTermAndIndex,
            ActionListener.wrap(r -> {
                updateSeqNoPrimaryTermAndIndex(seqNoPrimaryTermAndIndex, r);
                // for auto stop shutdown the task
                if (state.getTaskState().equals(TransformTaskState.STOPPED)) {
                    context.shutdown();
                }
                // Only do this clean up once, if it succeeded, no reason to do the query again.
                if (oldStatsCleanedUp.compareAndSet(false, true)) {
                    transformsConfigManager.deleteOldTransformStoredDocuments(getJobId(), ActionListener.wrap(nil -> {
                        logger.trace("[{}] deleted old transform stats and state document", getJobId());
                        listener.onResponse(null);
                    }, e -> {
                        String msg = LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", getJobId());
                        logger.warn(msg, e);
                        // If we have failed, we should attempt the clean up again later
                        oldStatsCleanedUp.set(false);
                        listener.onResponse(null);
                    }));
                } else {
                    listener.onResponse(null);
                }
            }, statsExc -> {
                logger.error(new ParameterizedMessage("[{}] updating stats of transform failed.", transformConfig.getId()), statsExc);
                auditor.warning(getJobId(), "Failure updating stats of transform: " + statsExc.getMessage());
                // for auto stop shutdown the task
                if (state.getTaskState().equals(TransformTaskState.STOPPED)) {
                    context.shutdown();
                }
                listener.onFailure(statsExc);
            })
        );

    }

    void updateSeqNoPrimaryTermAndIndex(SeqNoPrimaryTermAndIndex expectedValue, SeqNoPrimaryTermAndIndex newValue) {
        boolean updated = seqNoPrimaryTermAndIndex.compareAndSet(expectedValue, newValue);
        // This should never happen. We ONLY ever update this value if at initialization or we just finished updating the document
        // famous last words...
        assert updated : "[" + getJobId() + "] unexpected change to seqNoPrimaryTermAndIndex.";
    }

    @Nullable
    SeqNoPrimaryTermAndIndex getSeqNoPrimaryTermAndIndex() {
        return seqNoPrimaryTermAndIndex.get();
    }

    // Considered a recoverable indexing failure
    private static class BulkIndexingException extends ElasticsearchException {
        BulkIndexingException(String msg, Object... args) {
            super(msg, args);
        }
    }

}
