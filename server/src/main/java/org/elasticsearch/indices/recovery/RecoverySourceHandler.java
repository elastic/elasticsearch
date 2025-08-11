/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardRecoveryPlan;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 *
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer. The transaction log
 * phase bypasses the rate limiter entirely.
 */
public class RecoverySourceHandler {

    protected final Logger logger;
    // Shard that is going to be recovered (the "source")
    private final IndexShard shard;
    private final int shardId;
    // Request containing source and target node information
    private final StartRecoveryRequest request;
    private final int chunkSizeInBytes;
    private final RecoveryTargetHandler recoveryTarget;
    private final int maxConcurrentFileChunks;
    private final int maxConcurrentOperations;
    private final int maxConcurrentSnapshotFileDownloads;
    private final boolean useSnapshots;
    private final ThreadPool threadPool;
    private final RecoveryPlannerService recoveryPlannerService;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();
    private final ListenableFuture<RecoveryResponse> future = new ListenableFuture<>();

    public RecoverySourceHandler(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        ThreadPool threadPool,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations,
        int maxConcurrentSnapshotFileDownloads,
        boolean useSnapshots,
        RecoveryPlannerService recoveryPlannerService
    ) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.threadPool = threadPool;
        this.recoveryPlannerService = recoveryPlannerService;
        this.request = request;
        this.shardId = this.request.shardId().id();
        this.logger = Loggers.getLogger(getClass(), request.shardId(), "recover to " + request.targetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        // if the target is on an old version, it won't be able to handle out-of-order file chunks.
        this.maxConcurrentFileChunks = request.targetNode().getVersion().onOrAfter(Version.V_6_7_0) ? maxConcurrentFileChunks : 1;
        this.maxConcurrentOperations = maxConcurrentOperations;
        this.maxConcurrentSnapshotFileDownloads = maxConcurrentSnapshotFileDownloads;
        this.useSnapshots = useSnapshots;
    }

    public StartRecoveryRequest getRequest() {
        return request;
    }

    public void addListener(ActionListener<RecoveryResponse> listener) {
        future.addListener(listener);
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public void recoverToTarget(ActionListener<RecoveryResponse> listener) {
        addListener(listener);
        final Closeable releaseResources = () -> IOUtils.close(resources);
        try {
            cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
                final RuntimeException e;
                if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                    e = new IndexShardClosedException(shard.shardId(), "shard is closed and recovery was canceled reason [" + reason + "]");
                } else {
                    e = new CancellableThreads.ExecutionCancelledException("recovery was canceled reason [" + reason + "]");
                }
                if (beforeCancelEx != null) {
                    e.addSuppressed(beforeCancelEx);
                }
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
                throw e;
            });
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
            };

            final boolean softDeletesEnabled = shard.indexSettings().isSoftDeleteEnabled();
            final SetOnce<RetentionLease> retentionLeaseRef = new SetOnce<>();

            runUnderPrimaryPermit(() -> {
                final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
                if (targetShardRouting == null) {
                    logger.debug(
                        "delaying recovery of {} as it is not listed as assigned to target node {}",
                        request.shardId(),
                        request.targetNode()
                    );
                    throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
                }
                assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
                retentionLeaseRef.set(
                    shard.getRetentionLeases().get(ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetShardRouting))
                );
            },
                shardId + " validating recovery target [" + request.targetAllocationId() + "] registered ",
                shard,
                cancellableThreads,
                logger
            );
            final Engine.HistorySource historySource;
            if (softDeletesEnabled && (shard.useRetentionLeasesInPeerRecovery() || retentionLeaseRef.get() != null)) {
                historySource = Engine.HistorySource.INDEX;
            } else {
                historySource = Engine.HistorySource.TRANSLOG;
            }
            final Closeable retentionLock = shard.acquireHistoryRetentionLock(historySource);
            resources.add(retentionLock);
            final long startingSeqNo;
            final boolean isSequenceNumberBasedRecovery = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                && isTargetSameHistory()
                && shard.hasCompleteHistoryOperations("peer-recovery", historySource, request.startingSeqNo())
                && (historySource == Engine.HistorySource.TRANSLOG
                    || (retentionLeaseRef.get() != null && retentionLeaseRef.get().retainingSequenceNumber() <= request.startingSeqNo()));
            // NB check hasCompleteHistoryOperations when computing isSequenceNumberBasedRecovery, even if there is a retention lease,
            // because when doing a rolling upgrade from earlier than 7.4 we may create some leases that are initially unsatisfied. It's
            // possible there are other cases where we cannot satisfy all leases, because that's not a property we currently expect to hold.
            // Also it's pretty cheap when soft deletes are enabled, and it'd be a disaster if we tried a sequence-number-based recovery
            // without having a complete history.

            if (isSequenceNumberBasedRecovery && softDeletesEnabled && retentionLeaseRef.get() != null) {
                // all the history we need is retained by an existing retention lease, so we do not need a separate retention lock
                retentionLock.close();
                logger.trace("history is retained by {}", retentionLeaseRef.get());
            } else {
                // all the history we need is retained by the retention lock, obtained before calling shard.hasCompleteHistoryOperations()
                // and before acquiring the safe commit we'll be using, so we can be certain that all operations after the safe commit's
                // local checkpoint will be retained for the duration of this recovery.
                logger.trace("history is retained by retention lock");
            }

            final StepListener<SendFileResult> sendFileStep = new StepListener<>();
            final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
            final StepListener<SendSnapshotResult> sendSnapshotStep = new StepListener<>();
            final StepListener<Void> finalizeStep = new StepListener<>();

            if (isSequenceNumberBasedRecovery) {
                logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
                startingSeqNo = request.startingSeqNo();
                if (retentionLeaseRef.get() == null) {
                    createRetentionLease(startingSeqNo, sendFileStep.map(ignored -> SendFileResult.EMPTY));
                } else {
                    sendFileStep.onResponse(SendFileResult.EMPTY);
                }
            } else {
                final Engine.IndexCommitRef safeCommitRef;
                try {
                    safeCommitRef = acquireSafeCommit(shard);
                    resources.add(safeCommitRef);
                } catch (final Exception e) {
                    throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
                }

                // Try and copy enough operations to the recovering peer so that if it is promoted to primary then it has a chance of being
                // able to recover other replicas using operations-based recoveries. If we are not using retention leases then we
                // conservatively copy all available operations. If we are using retention leases then "enough operations" is just the
                // operations from the local checkpoint of the safe commit onwards, because when using soft deletes the safe commit retains
                // at least as much history as anything else. The safe commit will often contain all the history retained by the current set
                // of retention leases, but this is not guaranteed: an earlier peer recovery from a different primary might have created a
                // retention lease for some history that this primary already discarded, since we discard history when the global checkpoint
                // advances and not when creating a new safe commit. In any case this is a best-effort thing since future recoveries can
                // always fall back to file-based ones, and only really presents a problem if this primary fails before things have settled
                // down.
                startingSeqNo = softDeletesEnabled
                    ? Long.parseLong(safeCommitRef.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L
                    : 0;
                logger.trace("performing file-based recovery followed by history replay starting at [{}]", startingSeqNo);

                try {
                    final int estimateNumOps = shard.estimateNumberOfHistoryOperations("peer-recovery", historySource, startingSeqNo);
                    final Releasable releaseStore = acquireStore(shard.store());
                    resources.add(releaseStore);
                    sendFileStep.whenComplete(r -> IOUtils.close(safeCommitRef, releaseStore), e -> {
                        try {
                            IOUtils.close(safeCommitRef, releaseStore);
                        } catch (Exception ex) {
                            logger.warn("releasing snapshot caused exception", ex);
                        }
                    });

                    final StepListener<ReplicationResponse> deleteRetentionLeaseStep = new StepListener<>();
                    runUnderPrimaryPermit(() -> {
                        try {
                            // If the target previously had a copy of this shard then a file-based recovery might move its global
                            // checkpoint backwards. We must therefore remove any existing retention lease so that we can create a
                            // new one later on in the recovery.
                            shard.removePeerRecoveryRetentionLease(
                                request.targetNode().getId(),
                                new ThreadedActionListener<>(
                                    logger,
                                    shard.getThreadPool(),
                                    ThreadPool.Names.GENERIC,
                                    deleteRetentionLeaseStep,
                                    false
                                )
                            );
                        } catch (RetentionLeaseNotFoundException e) {
                            logger.debug("no peer-recovery retention lease for " + request.targetAllocationId());
                            deleteRetentionLeaseStep.onResponse(null);
                        }
                    }, shardId + " removing retention lease for [" + request.targetAllocationId() + "]", shard, cancellableThreads, logger);

                    deleteRetentionLeaseStep.whenComplete(ignored -> {
                        assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[phase1]");
                        phase1(safeCommitRef.getIndexCommit(), startingSeqNo, () -> estimateNumOps, sendFileStep);
                    }, onFailure);

                } catch (final Exception e) {
                    throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
                }
            }
            assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;

            sendFileStep.whenComplete(r -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[prepareTargetForTranslog]");
                // For a sequence based recovery, the target can keep its local translog
                prepareTargetForTranslog(
                    shard.estimateNumberOfHistoryOperations("peer-recovery", historySource, startingSeqNo),
                    prepareEngineStep
                );
            }, onFailure);

            prepareEngineStep.whenComplete(prepareEngineTime -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[phase2]");
                /*
                 * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
                 * This means that any document indexed into the primary after this will be replicated to this replica as well
                 * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
                 * all documents up to maxSeqNo in phase2.
                 */
                runUnderPrimaryPermit(
                    () -> shard.initiateTracking(request.targetAllocationId()),
                    shardId + " initiating tracking of " + request.targetAllocationId(),
                    shard,
                    cancellableThreads,
                    logger
                );

                final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
                logger.trace(
                    "snapshot translog for recovery; current size is [{}]",
                    shard.estimateNumberOfHistoryOperations("peer-recovery", historySource, startingSeqNo)
                );
                final Translog.Snapshot phase2Snapshot = shard.getHistoryOperations("peer-recovery", historySource, startingSeqNo);
                resources.add(phase2Snapshot);
                retentionLock.close();

                // we have to capture the max_seen_auto_id_timestamp and the max_seq_no_of_updates to make sure that these values
                // are at least as high as the corresponding values on the primary when any of these operations were executed on it.
                final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();
                final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();
                final RetentionLeases retentionLeases = shard.getRetentionLeases();
                final long mappingVersionOnPrimary = shard.indexSettings().getIndexMetadata().getMappingVersion();
                phase2(
                    startingSeqNo,
                    endingSeqNo,
                    phase2Snapshot,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersionOnPrimary,
                    sendSnapshotStep
                );

            }, onFailure);

            // Recovery target can trim all operations >= startingSeqNo as we have sent all these operations in the phase 2
            final long trimAboveSeqNo = startingSeqNo - 1;
            sendSnapshotStep.whenComplete(r -> finalizeRecovery(r.targetLocalCheckpoint, trimAboveSeqNo, finalizeStep), onFailure);

            finalizeStep.whenComplete(r -> {
                final long phase1ThrottlingWaitTime = 0L; // TODO: return the actual throttle time
                final SendSnapshotResult sendSnapshotResult = sendSnapshotStep.result();
                final SendFileResult sendFileResult = sendFileStep.result();
                final RecoveryResponse response = new RecoveryResponse(
                    sendFileResult.phase1FileNames,
                    sendFileResult.phase1FileSizes,
                    sendFileResult.phase1ExistingFileNames,
                    sendFileResult.phase1ExistingFileSizes,
                    sendFileResult.totalSize,
                    sendFileResult.existingTotalSize,
                    sendFileResult.took.millis(),
                    phase1ThrottlingWaitTime,
                    prepareEngineStep.result().millis(),
                    sendSnapshotResult.sentOperations,
                    sendSnapshotResult.tookTime.millis()
                );
                try {
                    future.onResponse(response);
                } finally {
                    IOUtils.close(resources);
                }
            }, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        }
    }

    private boolean isTargetSameHistory() {
        final String targetHistoryUUID = request.metadataSnapshot().getHistoryUUID();
        assert targetHistoryUUID != null || shard.indexSettings().getIndexVersionCreated().before(Version.V_6_0_0_rc1)
            : "incoming target history N/A but index was created after or on 6.0.0-rc1";
        return targetHistoryUUID != null && targetHistoryUUID.equals(shard.getHistoryUUID());
    }

    static void runUnderPrimaryPermit(
        CancellableThreads.Interruptible runnable,
        String reason,
        IndexShard primary,
        CancellableThreads cancellableThreads,
        Logger logger
    ) {
        cancellableThreads.execute(() -> {
            CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }
            };
            primary.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, reason);
            try (Releasable ignored = FutureUtils.get(permit)) {
                // check that the IndexShard still has the primary authority. This needs to be checked under operation permit to prevent
                // races, as IndexShard will switch its authority only when it holds all operation permits, see IndexShard.relocated()
                if (primary.isRelocatedPrimary()) {
                    throw new IndexShardRelocatedException(primary.shardId());
                }
                runnable.run();
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }
        });
    }

    /**
     * Increases the store reference and returns a {@link Releasable} that will decrease the store reference using the generic thread pool.
     * We must never release the store using an interruptible thread as we can risk invalidating the node lock.
     */
    private Releasable acquireStore(Store store) {
        store.incRef();
        return Releasables.releaseOnce(() -> runWithGenericThreadPool(store::decRef));
    }

    /**
     * Releasing a safe commit can access some commit files. It's better not to use {@link CancellableThreads} to interact
     * with the file systems due to interrupt (see {@link org.apache.lucene.store.NIOFSDirectory} javadocs for more detail).
     * This method acquires a safe commit and wraps it to make sure that it will be released using the generic thread pool.
     */
    private Engine.IndexCommitRef acquireSafeCommit(IndexShard shard) {
        final Engine.IndexCommitRef commitRef = shard.acquireSafeIndexCommit();
        return new Engine.IndexCommitRef(commitRef.getIndexCommit(), () -> runWithGenericThreadPool(commitRef::close));
    }

    private void runWithGenericThreadPool(CheckedRunnable<Exception> task) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        assert threadPool.generic().isShutdown() == false;
        // TODO: We shouldn't use the generic thread pool here as we already execute this from the generic pool.
        // While practically unlikely at a min pool size of 128 we could technically block the whole pool by waiting on futures
        // below and thus make it impossible for the store release to execute which in turn would block the futures forever
        threadPool.generic().execute(ActionRunnable.run(future, task));
        FutureUtils.get(future);
    }

    static final class SendFileResult {
        final List<String> phase1FileNames;
        final List<Long> phase1FileSizes;
        final long totalSize;

        final List<String> phase1ExistingFileNames;
        final List<Long> phase1ExistingFileSizes;
        final long existingTotalSize;

        final TimeValue took;

        SendFileResult(
            List<String> phase1FileNames,
            List<Long> phase1FileSizes,
            long totalSize,
            List<String> phase1ExistingFileNames,
            List<Long> phase1ExistingFileSizes,
            long existingTotalSize,
            TimeValue took
        ) {
            this.phase1FileNames = phase1FileNames;
            this.phase1FileSizes = phase1FileSizes;
            this.totalSize = totalSize;
            this.phase1ExistingFileNames = phase1ExistingFileNames;
            this.phase1ExistingFileSizes = phase1ExistingFileSizes;
            this.existingTotalSize = existingTotalSize;
            this.took = took;
        }

        static final SendFileResult EMPTY = new SendFileResult(
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            TimeValue.ZERO
        );
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link IndexCommit}
     * snapshot has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     * <p>
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     */
    void phase1(IndexCommit snapshot, long startingSeqNo, IntSupplier translogOps, ActionListener<SendFileResult> listener) {
        cancellableThreads.checkForCancel();
        final Store store = shard.store();
        try {
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata;
            final String shardStateIdentifier;
            try {
                recoverySourceMetadata = store.getMetadata(snapshot);
                shardStateIdentifier = SnapshotShardsService.getShardStateId(shard, snapshot);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                shard.failShard("recovery", ex);
                throw ex;
            }
            for (String name : snapshot.getFileNames()) {
                final StoreFileMetadata md = recoverySourceMetadata.get(name);
                if (md == null) {
                    logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                    throw new CorruptIndexException(
                        "Snapshot differs from actual index - maybe index was removed metadata has "
                            + recoverySourceMetadata.asMap().size()
                            + " files",
                        name
                    );
                }
            }
            // When sync ids were used we could use them to check if two shard copies were equivalent,
            // if that's the case we can skip sending files from the source shard to the target shard.
            // If the shard uses the current replication mechanism, we have to compute the recovery plan,
            // and it is still possible to skip the sending files from the source shard to the target shard
            // using a different mechanism to determine it.
            // TODO: is this still relevant today?
            if (hasSameLegacySyncId(recoverySourceMetadata, request.metadataSnapshot()) == false) {
                cancellableThreads.checkForCancel();
                final boolean canUseSnapshots = canUseSnapshots();
                recoveryPlannerService.computeRecoveryPlan(
                    shard.shardId(),
                    shardStateIdentifier,
                    recoverySourceMetadata,
                    request.metadataSnapshot(),
                    startingSeqNo,
                    translogOps.getAsInt(),
                    getRequest().targetNode().getVersion(),
                    canUseSnapshots,
                    ActionListener.wrap(plan -> recoverFilesFromSourceAndSnapshot(plan, store, stopWatch, listener), listener::onFailure)
                );
            } else {
                logger.trace("skipping [phase1] since source and target have identical sync id [{}]", recoverySourceMetadata.getSyncId());

                // but we must still create a retention lease
                final StepListener<RetentionLease> createRetentionLeaseStep = new StepListener<>();
                createRetentionLease(startingSeqNo, createRetentionLeaseStep);
                createRetentionLeaseStep.whenComplete(retentionLease -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.trace("recovery [phase1]: took [{}]", took);
                    listener.onResponse(
                        new SendFileResult(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            0L,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            0L,
                            took
                        )
                    );
                }, listener::onFailure);

            }
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), 0, new ByteSizeValue(0L), e);
        }
    }

    private boolean canUseSnapshots() {
        return useSnapshots && request.canDownloadSnapshotFiles()
        // Avoid using snapshots for searchable snapshots as these are implicitly recovered from a snapshot
            && SearchableSnapshotsSettings.isSearchableSnapshotStore(shard.indexSettings().getSettings()) == false;
    }

    void recoverFilesFromSourceAndSnapshot(
        ShardRecoveryPlan shardRecoveryPlan,
        Store store,
        StopWatch stopWatch,
        ActionListener<SendFileResult> listener
    ) {
        cancellableThreads.checkForCancel();

        final List<String> filesToRecoverNames = shardRecoveryPlan.getFilesToRecoverNames();
        final List<Long> filesToRecoverSizes = shardRecoveryPlan.getFilesToRecoverSizes();
        final List<String> phase1ExistingFileNames = shardRecoveryPlan.getFilesPresentInTargetNames();
        final List<Long> phase1ExistingFileSizes = shardRecoveryPlan.getFilesPresentInTargetSizes();
        final long totalSize = shardRecoveryPlan.getTotalSize();
        final long existingTotalSize = shardRecoveryPlan.getExistingSize();

        if (logger.isTraceEnabled()) {
            for (StoreFileMetadata md : shardRecoveryPlan.getFilesPresentInTarget()) {
                logger.trace(
                    "recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," + " size [{}]",
                    md.name(),
                    md.checksum(),
                    md.length()
                );
            }

            for (StoreFileMetadata md : shardRecoveryPlan.getSourceFilesToRecover()) {
                if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                    logger.trace(
                        "recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                        md.name(),
                        request.metadataSnapshot().asMap().get(md.name()),
                        md
                    );
                } else {
                    logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                }
            }

            for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : shardRecoveryPlan.getSnapshotFilesToRecover()) {
                final StoreFileMetadata md = fileInfo.metadata();
                if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                    logger.trace(
                        "recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                        md.name(),
                        request.metadataSnapshot().asMap().get(md.name()),
                        md
                    );
                } else {
                    logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                }
            }

            logger.trace(
                "recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                filesToRecoverNames.size(),
                new ByteSizeValue(totalSize),
                phase1ExistingFileNames.size(),
                new ByteSizeValue(existingTotalSize)
            );
        }

        // We need to pass the ShardRecovery plan between steps instead of capturing it in the closures
        // since the plan can change after a failure recovering files from the snapshots that cannot be
        // recovered from the source node, in that case we have to start from scratch using the fallback
        // recovery plan that would be used in subsequent steps.
        final StepListener<Void> sendFileInfoStep = new StepListener<>();
        final StepListener<Tuple<ShardRecoveryPlan, List<StoreFileMetadata>>> recoverSnapshotFilesStep = new StepListener<>();
        final StepListener<ShardRecoveryPlan> sendFilesStep = new StepListener<>();
        final StepListener<Tuple<ShardRecoveryPlan, RetentionLease>> createRetentionLeaseStep = new StepListener<>();
        final StepListener<ShardRecoveryPlan> cleanFilesStep = new StepListener<>();

        final int translogOps = shardRecoveryPlan.getTranslogOps();
        recoveryTarget.receiveFileInfo(
            filesToRecoverNames,
            filesToRecoverSizes,
            phase1ExistingFileNames,
            phase1ExistingFileSizes,
            translogOps,
            sendFileInfoStep
        );

        sendFileInfoStep.whenComplete(unused -> {
            recoverSnapshotFiles(shardRecoveryPlan, new ActionListener<List<StoreFileMetadata>>() {
                @Override
                public void onResponse(List<StoreFileMetadata> filesFailedToRecoverFromSnapshot) {
                    recoverSnapshotFilesStep.onResponse(Tuple.tuple(shardRecoveryPlan, filesFailedToRecoverFromSnapshot));
                }

                @Override
                public void onFailure(Exception e) {
                    if (shardRecoveryPlan.canRecoverSnapshotFilesFromSourceNode() == false
                        && e instanceof CancellableThreads.ExecutionCancelledException == false) {
                        ShardRecoveryPlan fallbackPlan = shardRecoveryPlan.getFallbackPlan();
                        recoveryTarget.receiveFileInfo(
                            fallbackPlan.getFilesToRecoverNames(),
                            fallbackPlan.getFilesToRecoverSizes(),
                            fallbackPlan.getFilesPresentInTargetNames(),
                            fallbackPlan.getFilesPresentInTargetSizes(),
                            fallbackPlan.getTranslogOps(),
                            recoverSnapshotFilesStep.map(r -> Tuple.tuple(fallbackPlan, Collections.emptyList()))
                        );
                    } else {
                        recoverSnapshotFilesStep.onFailure(e);
                    }
                }
            });
        }, listener::onFailure);

        recoverSnapshotFilesStep.whenComplete(planAndFilesFailedToRecoverFromSnapshot -> {
            ShardRecoveryPlan recoveryPlan = planAndFilesFailedToRecoverFromSnapshot.v1();
            List<StoreFileMetadata> filesFailedToRecoverFromSnapshot = planAndFilesFailedToRecoverFromSnapshot.v2();
            final List<StoreFileMetadata> filesToRecoverFromSource;
            if (filesFailedToRecoverFromSnapshot.isEmpty()) {
                filesToRecoverFromSource = recoveryPlan.getSourceFilesToRecover();
            } else {
                filesToRecoverFromSource = concatLists(recoveryPlan.getSourceFilesToRecover(), filesFailedToRecoverFromSnapshot);
            }

            sendFiles(
                store,
                filesToRecoverFromSource.toArray(new StoreFileMetadata[0]),
                recoveryPlan::getTranslogOps,
                sendFilesStep.map(unused -> recoveryPlan)
            );
        }, listener::onFailure);

        sendFilesStep.whenComplete(recoveryPlan -> {
            createRetentionLease(
                recoveryPlan.getStartingSeqNo(),
                createRetentionLeaseStep.map(retentionLease -> Tuple.tuple(recoveryPlan, retentionLease))
            );
        }, listener::onFailure);

        createRetentionLeaseStep.whenComplete(recoveryPlanAndRetentionLease -> {
            final ShardRecoveryPlan recoveryPlan = recoveryPlanAndRetentionLease.v1();
            final RetentionLease retentionLease = recoveryPlanAndRetentionLease.v2();
            final Store.MetadataSnapshot recoverySourceMetadata = recoveryPlan.getSourceMetadataSnapshot();
            final long lastKnownGlobalCheckpoint = shard.getLastKnownGlobalCheckpoint();
            assert retentionLease == null || retentionLease.retainingSequenceNumber() - 1 <= lastKnownGlobalCheckpoint
                : retentionLease + " vs " + lastKnownGlobalCheckpoint;
            // Establishes new empty translog on the replica with global checkpoint set to lastKnownGlobalCheckpoint. We want
            // the commit we just copied to be a safe commit on the replica, so why not set the global checkpoint on the replica
            // to the max seqno of this commit? Because (in rare corner cases) this commit might not be a safe commit here on
            // the primary, and in these cases the max seqno would be too high to be valid as a global checkpoint.
            cleanFiles(
                store,
                recoverySourceMetadata,
                () -> translogOps,
                lastKnownGlobalCheckpoint,
                cleanFilesStep.map(unused -> recoveryPlan)
            );
        }, listener::onFailure);

        cleanFilesStep.whenComplete(recoveryPlan -> {
            final TimeValue took = stopWatch.totalTime();
            logger.trace("recovery [phase1]: took [{}]", took);
            listener.onResponse(
                new SendFileResult(
                    recoveryPlan.getFilesToRecoverNames(),
                    recoveryPlan.getFilesToRecoverSizes(),
                    recoveryPlan.getTotalSize(),
                    recoveryPlan.getFilesPresentInTargetNames(),
                    recoveryPlan.getFilesPresentInTargetSizes(),
                    recoveryPlan.getExistingSize(),
                    took
                )
            );
        }, listener::onFailure);
    }

    /**
     * Send requests to the target node to recover files from a given snapshot. In case of failure, the listener
     * value contains the list of files that failed to be recovered from a snapshot.
     */
    void recoverSnapshotFiles(ShardRecoveryPlan shardRecoveryPlan, ActionListener<List<StoreFileMetadata>> listener) {
        ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover = shardRecoveryPlan.getSnapshotFilesToRecover();

        if (snapshotFilesToRecover.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }

        new SnapshotRecoverFileRequestsSender(shardRecoveryPlan, listener).start();
    }

    private class SnapshotRecoverFileRequestsSender {
        private final ShardRecoveryPlan shardRecoveryPlan;
        private final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover;
        private final ActionListener<List<StoreFileMetadata>> listener;
        private final CountDown countDown;
        private final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> pendingSnapshotFilesToRecover;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final Set<ListenableFuture<Void>> outstandingRequests = new HashSet<>(maxConcurrentSnapshotFileDownloads);
        private List<StoreFileMetadata> filesFailedToDownloadFromSnapshot;

        SnapshotRecoverFileRequestsSender(ShardRecoveryPlan shardRecoveryPlan, ActionListener<List<StoreFileMetadata>> listener) {
            this.shardRecoveryPlan = shardRecoveryPlan;
            this.snapshotFilesToRecover = shardRecoveryPlan.getSnapshotFilesToRecover();
            this.listener = listener;
            this.countDown = new CountDown(shardRecoveryPlan.getSnapshotFilesToRecover().size());
            this.pendingSnapshotFilesToRecover = new LinkedBlockingQueue<>(
                shardRecoveryPlan.getSnapshotFilesToRecover().getSnapshotFiles()
            );
        }

        void start() {
            for (int i = 0; i < maxConcurrentSnapshotFileDownloads; i++) {
                sendRequest();
            }
        }

        void sendRequest() {
            BlobStoreIndexShardSnapshot.FileInfo snapshotFileToRecover = pendingSnapshotFilesToRecover.poll();
            if (snapshotFileToRecover == null) {
                return;
            }

            final ListenableFuture<Void> requestFuture = new ListenableFuture<>();
            try {
                cancellableThreads.checkForCancel();

                ActionListener<Void> sendRequestListener = new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        onRequestCompletion(snapshotFileToRecover.metadata(), null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            new ParameterizedMessage(
                                "failed to recover file [{}] from snapshot, " + "will recover from primary instead",
                                snapshotFileToRecover.metadata()
                            ),
                            e
                        );
                        if (shardRecoveryPlan.canRecoverSnapshotFilesFromSourceNode()) {
                            onRequestCompletion(snapshotFileToRecover.metadata(), e);
                        } else {
                            cancel(e);
                        }
                    }
                };
                requestFuture.addListener(sendRequestListener);

                trackOutstandingRequest(requestFuture);
                recoveryTarget.restoreFileFromSnapshot(
                    snapshotFilesToRecover.getRepository(),
                    snapshotFilesToRecover.getIndexId(),
                    snapshotFileToRecover,
                    ActionListener.runBefore(requestFuture, () -> unTrackOutstandingRequest(requestFuture))
                );
            } catch (CancellableThreads.ExecutionCancelledException e) {
                cancel(e);
            } catch (Exception e) {
                unTrackOutstandingRequest(requestFuture);
                onRequestCompletion(snapshotFileToRecover.metadata(), e);
            }
        }

        void cancel(Exception e) {
            if (cancelled.compareAndSet(false, true)) {
                pendingSnapshotFilesToRecover.clear();
                notifyFailureOnceAllOutstandingRequestAreDone(e);
            }
        }

        void onRequestCompletion(StoreFileMetadata storeFileMetadata, @Nullable Exception exception) {
            if (cancelled.get()) {
                return;
            }

            if (exception != null) {
                addFileFailedToRecoverFromSnapshot(storeFileMetadata);
            }

            if (countDown.countDown()) {
                final List<StoreFileMetadata> failedToRecoverFromSnapshotFiles = getFilesFailedToRecoverFromSnapshot();
                listener.onResponse(failedToRecoverFromSnapshotFiles);
            } else {
                sendRequest();
            }
        }

        synchronized void addFileFailedToRecoverFromSnapshot(StoreFileMetadata storeFileMetadata) {
            if (filesFailedToDownloadFromSnapshot == null) {
                filesFailedToDownloadFromSnapshot = new ArrayList<>();
            }
            filesFailedToDownloadFromSnapshot.add(storeFileMetadata);
        }

        synchronized List<StoreFileMetadata> getFilesFailedToRecoverFromSnapshot() {
            if (filesFailedToDownloadFromSnapshot == null) {
                return Collections.emptyList();
            }
            return filesFailedToDownloadFromSnapshot;
        }

        private void trackOutstandingRequest(ListenableFuture<Void> future) {
            boolean cancelled;
            synchronized (outstandingRequests) {
                cancelled = cancellableThreads.isCancelled() || this.cancelled.get();
                if (cancelled == false) {
                    outstandingRequests.add(future);
                }
            }
            if (cancelled) {
                cancellableThreads.checkForCancel();
                // If the recover snapshot files operation is cancelled but the recovery is still
                // valid, it means that some of the snapshot files download failed and the snapshot files
                // differ from the source index files. In that case we have to cancel all pending operations
                // and wait until all the in-flight operations are done to reset the recovery and start from
                // scratch using the source node index files.
                assert this.cancelled.get();
                throw new CancellableThreads.ExecutionCancelledException("Recover snapshot files cancelled");
            }
        }

        private void unTrackOutstandingRequest(ListenableFuture<Void> future) {
            synchronized (outstandingRequests) {
                outstandingRequests.remove(future);
            }
        }

        private void notifyFailureOnceAllOutstandingRequestAreDone(Exception e) {
            assert cancelled.get();

            final Set<ListenableFuture<Void>> pendingRequests;
            synchronized (outstandingRequests) {
                pendingRequests = new HashSet<>(outstandingRequests);
            }

            if (pendingRequests.isEmpty()) {
                listener.onFailure(e);
                return;
            }
            // The recovery was cancelled so from this point onwards outstandingRequests won't track
            // new requests and therefore we can safely use to wait until all the pending requests complete
            // to notify the listener about the cancellation
            final CountDown pendingRequestsCountDown = new CountDown(pendingRequests.size());
            for (ListenableFuture<Void> outstandingFuture : pendingRequests) {
                outstandingFuture.addListener(ActionListener.wrap(() -> {
                    if (pendingRequestsCountDown.countDown()) {
                        listener.onFailure(e);
                    }
                }));
            }
        }
    }

    void createRetentionLease(final long startingSeqNo, ActionListener<RetentionLease> listener) {
        runUnderPrimaryPermit(() -> {
            // Clone the peer recovery retention lease belonging to the source shard. We are retaining history between the the local
            // checkpoint of the safe commit we're creating and this lease's retained seqno with the retention lock, and by cloning an
            // existing lease we (approximately) know that all our peers are also retaining history as requested by the cloned lease. If
            // the recovery now fails before copying enough history over then a subsequent attempt will find this lease, determine it is
            // not enough, and fall back to a file-based recovery.
            //
            // (approximately) because we do not guarantee to be able to satisfy every lease on every peer.
            logger.trace("cloning primary's retention lease");
            try {
                final StepListener<ReplicationResponse> cloneRetentionLeaseStep = new StepListener<>();
                final RetentionLease clonedLease = shard.cloneLocalPeerRecoveryRetentionLease(
                    request.targetNode().getId(),
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, cloneRetentionLeaseStep, false)
                );
                logger.trace("cloned primary's retention lease as [{}]", clonedLease);
                cloneRetentionLeaseStep.addListener(listener.map(rr -> clonedLease));
            } catch (RetentionLeaseNotFoundException e) {
                // it's possible that the primary has no retention lease yet if we are doing a rolling upgrade from a version before
                // 7.4, and in that case we just create a lease using the local checkpoint of the safe commit which we're using for
                // recovery as a conservative estimate for the global checkpoint.
                assert shard.indexSettings().getIndexVersionCreated().before(Version.V_7_4_0)
                    || shard.indexSettings().isSoftDeleteEnabled() == false;
                final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
                final long estimatedGlobalCheckpoint = startingSeqNo - 1;
                final RetentionLease newLease = shard.addPeerRecoveryRetentionLease(
                    request.targetNode().getId(),
                    estimatedGlobalCheckpoint,
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, addRetentionLeaseStep, false)
                );
                addRetentionLeaseStep.addListener(listener.map(rr -> newLease));
                logger.trace("created retention lease with estimated checkpoint of [{}]", estimatedGlobalCheckpoint);
            }
        }, shardId + " establishing retention lease for [" + request.targetAllocationId() + "]", shard, cancellableThreads, logger);
    }

    boolean hasSameLegacySyncId(Store.MetadataSnapshot source, Store.MetadataSnapshot target) {
        if (source.getSyncId() == null || source.getSyncId().equals(target.getSyncId()) == false) {
            return false;
        }
        if (source.getNumDocs() != target.getNumDocs()) {
            throw new IllegalStateException(
                "try to recover "
                    + request.shardId()
                    + " from primary shard with sync id but number "
                    + "of docs differ: "
                    + source.getNumDocs()
                    + " ("
                    + request.sourceNode().getName()
                    + ", primary) vs "
                    + target.getNumDocs()
                    + "("
                    + request.targetNode().getName()
                    + ")"
            );
        }
        SequenceNumbers.CommitInfo sourceSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(source.getCommitUserData().entrySet());
        SequenceNumbers.CommitInfo targetSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(target.getCommitUserData().entrySet());
        if (sourceSeqNos.localCheckpoint != targetSeqNos.localCheckpoint || targetSeqNos.maxSeqNo != sourceSeqNos.maxSeqNo) {
            final String message = "try to recover "
                + request.shardId()
                + " with sync id but "
                + "seq_no stats are mismatched: ["
                + source.getCommitUserData()
                + "] vs ["
                + target.getCommitUserData()
                + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
        return true;
    }

    void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
        StopWatch stopWatch = new StopWatch().start();
        final ActionListener<Void> wrappedListener = ActionListener.wrap(nullVal -> {
            stopWatch.stop();
            final TimeValue tookTime = stopWatch.totalTime();
            logger.trace("recovery [phase1]: remote engine start took [{}]", tookTime);
            listener.onResponse(tookTime);
        }, e -> listener.onFailure(new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e)));
        // Send a request preparing the new shard's translog to receive operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        cancellableThreads.checkForCancel();
        recoveryTarget.prepareForTranslogOperations(totalTranslogOps, wrappedListener);
    }

    /**
     * Perform phase two of the recovery process.
     * <p>
     * Phase two uses a snapshot of the current translog *without* acquiring the write lock (however, the translog snapshot is
     * point-in-time view of the translog). It then sends each translog operation to the target node so it can be replayed into the new
     * shard.
     *
     * @param startingSeqNo              the sequence number to start recovery from, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if all
     *                                   ops should be sent
     * @param endingSeqNo                the highest sequence number that should be sent
     * @param snapshot                   a snapshot of the translog
     * @param maxSeenAutoIdTimestamp     the max auto_id_timestamp of append-only requests on the primary
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates or deletes on the primary after these operations were executed on it.
     * @param listener                   a listener which will be notified with the local checkpoint on the target.
     */
    void phase2(
        final long startingSeqNo,
        final long endingSeqNo,
        final Translog.Snapshot snapshot,
        final long maxSeenAutoIdTimestamp,
        final long maxSeqNoOfUpdatesOrDeletes,
        final RetentionLeases retentionLeases,
        final long mappingVersion,
        final ActionListener<SendSnapshotResult> listener
    ) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        logger.trace("recovery [phase2]: sending transaction log operations (from [" + startingSeqNo + "] to [" + endingSeqNo + "]");
        final StopWatch stopWatch = new StopWatch().start();
        final StepListener<Void> sendListener = new StepListener<>();
        final OperationBatchSender sender = new OperationBatchSender(
            startingSeqNo,
            endingSeqNo,
            snapshot,
            maxSeenAutoIdTimestamp,
            maxSeqNoOfUpdatesOrDeletes,
            retentionLeases,
            mappingVersion,
            sendListener
        );
        sendListener.whenComplete(ignored -> {
            final long skippedOps = sender.skippedOps.get();
            final int totalSentOps = sender.sentOps.get();
            final long targetLocalCheckpoint = sender.targetLocalCheckpoint.get();
            assert snapshot.totalOperations() == snapshot.skippedOperations() + skippedOps + totalSentOps
                : String.format(
                    Locale.ROOT,
                    "expected total [%d], overridden [%d], skipped [%d], total sent [%d]",
                    snapshot.totalOperations(),
                    snapshot.skippedOperations(),
                    skippedOps,
                    totalSentOps
                );
            stopWatch.stop();
            final TimeValue tookTime = stopWatch.totalTime();
            logger.trace("recovery [phase2]: took [{}]", tookTime);
            listener.onResponse(new SendSnapshotResult(targetLocalCheckpoint, totalSentOps, tookTime));
        }, listener::onFailure);
        sender.start();
    }

    private static class OperationChunkRequest implements MultiChunkTransfer.ChunkRequest {
        final List<Translog.Operation> operations;
        final boolean lastChunk;

        OperationChunkRequest(List<Translog.Operation> operations, boolean lastChunk) {
            this.operations = operations;
            this.lastChunk = lastChunk;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }
    }

    private class OperationBatchSender extends MultiChunkTransfer<Translog.Snapshot, OperationChunkRequest> {
        private final long startingSeqNo;
        private final long endingSeqNo;
        private final Translog.Snapshot snapshot;
        private final long maxSeenAutoIdTimestamp;
        private final long maxSeqNoOfUpdatesOrDeletes;
        private final RetentionLeases retentionLeases;
        private final long mappingVersion;
        private int lastBatchCount = 0; // used to estimate the count of the subsequent batch.
        private final AtomicInteger skippedOps = new AtomicInteger();
        private final AtomicInteger sentOps = new AtomicInteger();
        private final AtomicLong targetLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        OperationBatchSender(
            long startingSeqNo,
            long endingSeqNo,
            Translog.Snapshot snapshot,
            long maxSeenAutoIdTimestamp,
            long maxSeqNoOfUpdatesOrDeletes,
            RetentionLeases retentionLeases,
            long mappingVersion,
            ActionListener<Void> listener
        ) {
            super(logger, threadPool.getThreadContext(), listener, maxConcurrentOperations, Collections.singletonList(snapshot));
            this.startingSeqNo = startingSeqNo;
            this.endingSeqNo = endingSeqNo;
            this.snapshot = snapshot;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.retentionLeases = retentionLeases;
            this.mappingVersion = mappingVersion;
        }

        @Override
        protected synchronized OperationChunkRequest nextChunkRequest(Translog.Snapshot snapshot) throws IOException {
            // We need to synchronized Snapshot#next() because it's called by different threads through sendBatch.
            // Even though those calls are not concurrent, Snapshot#next() uses non-synchronized state and is not multi-thread-compatible.
            assert Transports.assertNotTransportThread("[phase2]");
            cancellableThreads.checkForCancel();
            final List<Translog.Operation> ops = lastBatchCount > 0 ? new ArrayList<>(lastBatchCount) : new ArrayList<>();
            long batchSizeInBytes = 0L;
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                final long seqNo = operation.seqNo();
                if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                    skippedOps.incrementAndGet();
                    continue;
                }
                ops.add(operation);
                batchSizeInBytes += operation.estimateSize();
                sentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (batchSizeInBytes >= chunkSizeInBytes) {
                    break;
                }
            }
            lastBatchCount = ops.size();
            return new OperationChunkRequest(ops, operation == null);
        }

        @Override
        protected void executeChunkRequest(OperationChunkRequest request, ActionListener<Void> listener) {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperations(
                request.operations,
                snapshot.totalOperations(),
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersion,
                listener.delegateFailure((l, newCheckpoint) -> {
                    targetLocalCheckpoint.updateAndGet(curr -> SequenceNumbers.max(curr, newCheckpoint));
                    l.onResponse(null);
                })
            );
        }

        @Override
        protected void handleError(Translog.Snapshot snapshot, Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 2, "failed to send/replay operations", e);
        }

        @Override
        public void close() throws IOException {
            snapshot.close();
        }
    }

    void finalizeRecovery(long targetLocalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("finalizing recovery");
        /*
         * Before marking the shard as in-sync we acquire an operation permit. We do this so that there is a barrier between marking a
         * shard as in-sync and relocating a shard. If we acquire the permit then no relocation handoff can complete before we are done
         * marking the shard as in-sync. If the relocation handoff holds all the permits then after the handoff completes and we acquire
         * the permit then the state of the shard will be relocated and this recovery will fail.
         */
        runUnderPrimaryPermit(
            () -> shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint),
            shardId + " marking " + request.targetAllocationId() + " as in sync",
            shard,
            cancellableThreads,
            logger
        );
        final long globalCheckpoint = shard.getLastKnownGlobalCheckpoint(); // this global checkpoint is persisted in finalizeRecovery
        final StepListener<Void> finalizeListener = new StepListener<>();
        cancellableThreads.checkForCancel();
        recoveryTarget.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, finalizeListener);
        finalizeListener.whenComplete(r -> {
            runUnderPrimaryPermit(
                () -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),
                shardId + " updating " + request.targetAllocationId() + "'s global checkpoint",
                shard,
                cancellableThreads,
                logger
            );

            if (request.isPrimaryRelocation()) {
                logger.trace("performing relocation hand-off");
                // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
                cancellableThreads.execute(
                    () -> shard.relocated(request.targetAllocationId(), recoveryTarget::handoffPrimaryContext, ActionListener.wrap(v -> {
                        cancellableThreads.checkForCancel();
                        completeFinalizationListener(listener, stopWatch);
                    }, listener::onFailure))
                );
                /*
                 * if the recovery process fails after disabling primary mode on the source shard, both relocation source and
                 * target are failed (see {@link IndexShard#updateRoutingEntry}).
                 */
            } else {
                completeFinalizationListener(listener, stopWatch);
            }
        }, listener::onFailure);
    }

    private void completeFinalizationListener(ActionListener<Void> listener, StopWatch stopWatch) {
        stopWatch.stop();
        logger.trace("finalizing recovery took [{}]", stopWatch.totalTime());
        listener.onResponse(null);
    }

    static final class SendSnapshotResult {
        final long targetLocalCheckpoint;
        final int sentOperations;
        final TimeValue tookTime;

        SendSnapshotResult(final long targetLocalCheckpoint, final int sentOperations, final TimeValue tookTime) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.sentOperations = sentOperations;
            this.tookTime = tookTime;
        }
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
        recoveryTarget.cancel();
    }

    @Override
    public String toString() {
        return "ShardRecoveryHandler{"
            + "shardId="
            + request.shardId()
            + ", sourceNode="
            + request.sourceNode()
            + ", targetNode="
            + request.targetNode()
            + '}';
    }

    private static class FileChunk implements MultiChunkTransfer.ChunkRequest, Releasable {
        final StoreFileMetadata md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        final Releasable onClose;

        FileChunk(StoreFileMetadata md, BytesReference content, long position, boolean lastChunk, Releasable onClose) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
            this.onClose = onClose;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    void sendFiles(Store store, StoreFileMetadata[] files, IntSupplier translogOps, ActionListener<Void> listener) {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length)); // send smallest first

        // use a smaller buffer than the configured chunk size if we only have files smaller than the chunk size
        final int bufferSize = files.length == 0 ? 0 : (int) Math.min(chunkSizeInBytes, files[files.length - 1].length());
        Releasable temporaryStoreRef = acquireStore(store);
        try {
            final Releasable storeRef = temporaryStoreRef;
            final MultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileSender = new MultiChunkTransfer<StoreFileMetadata, FileChunk>(
                logger,
                threadPool.getThreadContext(),
                listener,
                maxConcurrentFileChunks,
                Arrays.asList(files)
            ) {

                final Deque<byte[]> buffers = new ConcurrentLinkedDeque<>();
                final AtomicInteger liveBufferCount = new AtomicInteger(); // only used in assertions to verify proper recycling
                IndexInput currentInput = null;
                long offset = 0;

                @Override
                protected void onNewResource(StoreFileMetadata md) throws IOException {
                    offset = 0;
                    IOUtils.close(currentInput);
                    if (md.hashEqualsContents()) {
                        // we already have the file contents on heap no need to open the file again
                        currentInput = null;
                    } else {
                        currentInput = store.directory().openInput(md.name(), IOContext.READONCE);
                    }
                }

                private byte[] acquireBuffer() {
                    assert liveBufferCount.incrementAndGet() > 0;
                    final byte[] buffer = buffers.pollFirst();
                    if (buffer != null) {
                        return buffer;
                    }
                    return new byte[bufferSize];
                }

                @Override
                protected FileChunk nextChunkRequest(StoreFileMetadata md) throws IOException {
                    assert Transports.assertNotTransportThread("read file chunk");
                    cancellableThreads.checkForCancel();
                    if (currentInput == null) {
                        // no input => reading directly from the metadata
                        assert md.hashEqualsContents();
                        return new FileChunk(md, new BytesArray(md.hash()), 0, true, () -> {});
                    }
                    final byte[] buffer = acquireBuffer();
                    final int toRead = Math.toIntExact(Math.min(md.length() - offset, buffer.length));
                    currentInput.readBytes(buffer, 0, toRead, false);
                    final boolean lastChunk = offset + toRead == md.length();
                    final FileChunk chunk = new FileChunk(md, new BytesArray(buffer, 0, toRead), offset, lastChunk, () -> {
                        assert liveBufferCount.decrementAndGet() >= 0;
                        buffers.addFirst(buffer);
                    });
                    offset += toRead;
                    return chunk;
                }

                @Override
                protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                    cancellableThreads.checkForCancel();
                    final ReleasableBytesReference content = new ReleasableBytesReference(request.content, request);
                    recoveryTarget.writeFileChunk(
                        request.md,
                        request.position,
                        content,
                        request.lastChunk,
                        translogOps.getAsInt(),
                        ActionListener.runBefore(listener, content::close)
                    );
                }

                @Override
                protected void handleError(StoreFileMetadata md, Exception e) throws Exception {
                    handleErrorOnSendFiles(store, e, new StoreFileMetadata[] { md });
                }

                @Override
                public void close() throws IOException {
                    IOUtils.close(currentInput, storeRef);
                }

                @Override
                protected boolean assertOnSuccess() {
                    assert liveBufferCount.get() == 0 : "leaked [" + liveBufferCount + "] buffers";
                    return true;
                }
            };
            resources.add(multiFileSender);
            temporaryStoreRef = null; // now owned by multiFileSender, tracked in resources, so won't be leaked
            multiFileSender.start();
        } finally {
            Releasables.close(temporaryStoreRef);
        }
    }

    private void cleanFiles(
        Store store,
        Store.MetadataSnapshot sourceMetadata,
        IntSupplier translogOps,
        long globalCheckpoint,
        ActionListener<Void> listener
    ) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        cancellableThreads.checkForCancel();
        recoveryTarget.cleanFiles(
            translogOps.getAsInt(),
            globalCheckpoint,
            sourceMetadata,
            listener.delegateResponse((l, e) -> ActionListener.completeWith(l, () -> {
                StoreFileMetadata[] mds = StreamSupport.stream(sourceMetadata.spliterator(), false).toArray(StoreFileMetadata[]::new);
                ArrayUtil.timSort(mds, Comparator.comparingLong(StoreFileMetadata::length)); // check small files first
                handleErrorOnSendFiles(store, e, mds);
                throw e;
            }))
        );
    }

    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetadata md : mds) {
                cancellableThreads.checkForCancel();
                logger.debug("checking integrity for file {} after remove corruption exception", md);
                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                    if (localException == null) {
                        localException = corruptIndexException;
                    }
                    failEngine(corruptIndexException);
                }
            }
            if (localException != null) {
                throw localException;
            } else { // corruption has happened on the way to replica
                RemoteTransportException remoteException = new RemoteTransportException(
                    "File corruption occurred on recovery but checksums are ok",
                    null
                );
                remoteException.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "{} Remote file corruption on node {}, recovering {}. local checksum OK",
                        shardId,
                        request.targetNode(),
                        mds
                    ),
                    corruptIndexException
                );
                throw remoteException;
            }
        }
        throw e;
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
