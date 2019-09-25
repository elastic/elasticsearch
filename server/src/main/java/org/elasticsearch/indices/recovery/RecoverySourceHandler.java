/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.FutureUtils;
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
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.StreamSupport;

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
    private final ThreadPool threadPool;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();

    public RecoverySourceHandler(IndexShard shard, RecoveryTargetHandler recoveryTarget, ThreadPool threadPool,
                                 StartRecoveryRequest request, int fileChunkSizeInBytes, int maxConcurrentFileChunks) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.threadPool = threadPool;
        this.request = request;
        this.shardId = this.request.shardId().id();
        this.logger = Loggers.getLogger(getClass(), request.shardId(), "recover to " + request.targetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    public StartRecoveryRequest getRequest() {
        return request;
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public void recoverToTarget(ActionListener<RecoveryResponse> listener) {
        final Closeable releaseResources = () -> IOUtils.close(resources);
        final ActionListener<RecoveryResponse> wrappedListener = ActionListener.notifyOnce(listener);
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
                IOUtils.closeWhileHandlingException(releaseResources, () -> wrappedListener.onFailure(e));
                throw e;
            });
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> wrappedListener.onFailure(e));
            };

            final boolean useRetentionLeases = shard.indexSettings().isSoftDeleteEnabled()
                && shard.indexSettings().getIndexMetaData().getState() != IndexMetaData.State.CLOSE;
            final SetOnce<RetentionLease> retentionLeaseRef = new SetOnce<>();

            runUnderPrimaryPermit(() -> {
                final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
                if (targetShardRouting == null) {
                    logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(),
                        request.targetNode());
                    throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
                }
                assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
                retentionLeaseRef.set(useRetentionLeases ? shard.getRetentionLeases().get(
                    ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetShardRouting)) : null);
            }, shardId + " validating recovery target ["+ request.targetAllocationId() + "] registered ",
                shard, cancellableThreads, logger);
            final Closeable retentionLock = shard.acquireRetentionLock();
            resources.add(retentionLock);
            final long startingSeqNo;
            final boolean isSequenceNumberBasedRecovery
                = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                && isTargetSameHistory()
                && shard.hasCompleteHistoryOperations("peer-recovery", request.startingSeqNo())
                && (useRetentionLeases == false
                || (retentionLeaseRef.get() != null && retentionLeaseRef.get().retainingSequenceNumber() <= request.startingSeqNo()));
            // NB check hasCompleteHistoryOperations when computing isSequenceNumberBasedRecovery, even if there is a retention lease,
            // because when doing a rolling upgrade from earlier than 7.4 we may create some leases that are initially unsatisfied. It's
            // possible there are other cases where we cannot satisfy all leases, because that's not a property we currently expect to hold.
            // Also it's pretty cheap when soft deletes are enabled, and it'd be a disaster if we tried a sequence-number-based recovery
            // without having a complete history.

            if (isSequenceNumberBasedRecovery && useRetentionLeases) {
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
                sendFileStep.onResponse(SendFileResult.EMPTY);
            } else {
                final Engine.IndexCommitRef safeCommitRef;
                try {
                    safeCommitRef = shard.acquireSafeIndexCommit();
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
                startingSeqNo = useRetentionLeases
                    ? Long.parseLong(safeCommitRef.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L
                    : 0;
                logger.trace("performing file-based recovery followed by history replay starting at [{}]", startingSeqNo);

                try {
                    final int estimateNumOps = shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo);
                    final Releasable releaseStore = acquireStore(shard.store());
                    resources.add(releaseStore);
                    sendFileStep.whenComplete(r -> IOUtils.close(safeCommitRef, releaseStore), e -> {
                        try {
                            IOUtils.close(safeCommitRef, releaseStore);
                        } catch (final IOException ex) {
                            logger.warn("releasing snapshot caused exception", ex);
                        }
                    });

                    final StepListener<ReplicationResponse> deleteRetentionLeaseStep = new StepListener<>();
                    if (useRetentionLeases) {
                        runUnderPrimaryPermit(() -> {
                                try {
                                    // If the target previously had a copy of this shard then a file-based recovery might move its global
                                    // checkpoint backwards. We must therefore remove any existing retention lease so that we can create a
                                    // new one later on in the recovery.
                                    shard.removePeerRecoveryRetentionLease(request.targetNode().getId(),
                                        new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC,
                                            deleteRetentionLeaseStep, false));
                                } catch (RetentionLeaseNotFoundException e) {
                                    logger.debug("no peer-recovery retention lease for " + request.targetAllocationId());
                                    deleteRetentionLeaseStep.onResponse(null);
                                }
                            }, shardId + " removing retention leaes for [" + request.targetAllocationId() + "]",
                            shard, cancellableThreads, logger);
                    } else {
                        deleteRetentionLeaseStep.onResponse(null);
                    }

                    deleteRetentionLeaseStep.whenComplete(ignored -> {
                        assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[phase1]");

                        final Consumer<ActionListener<RetentionLease>> createRetentionLeaseAsync;
                        if (useRetentionLeases) {
                            createRetentionLeaseAsync = l -> createRetentionLease(startingSeqNo, l);
                        } else {
                            createRetentionLeaseAsync = l -> l.onResponse(null);
                        }

                        phase1(safeCommitRef.getIndexCommit(), createRetentionLeaseAsync, () -> estimateNumOps, sendFileStep);
                    }, onFailure);

                } catch (final Exception e) {
                    throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
                }
            }
            assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;

            sendFileStep.whenComplete(r -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[prepareTargetForTranslog]");
                // For a sequence based recovery, the target can keep its local translog
                prepareTargetForTranslog(shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo), prepareEngineStep);
            }, onFailure);

            prepareEngineStep.whenComplete(prepareEngineTime -> {
                assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[phase2]");
                /*
                 * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
                 * This means that any document indexed into the primary after this will be replicated to this replica as well
                 * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
                 * all documents up to maxSeqNo in phase2.
                 */
                runUnderPrimaryPermit(() -> shard.initiateTracking(request.targetAllocationId()),
                    shardId + " initiating tracking of " + request.targetAllocationId(), shard, cancellableThreads, logger);

                final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
                logger.trace("snapshot translog for recovery; current size is [{}]",
                    shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo));
                final Translog.Snapshot phase2Snapshot = shard.getHistoryOperations("peer-recovery", startingSeqNo);
                resources.add(phase2Snapshot);

                if (useRetentionLeases == false || isSequenceNumberBasedRecovery == false) {
                    // we can release the retention lock here because the snapshot itself will retain the required operations.
                    retentionLock.close();
                }

                // we have to capture the max_seen_auto_id_timestamp and the max_seq_no_of_updates to make sure that these values
                // are at least as high as the corresponding values on the primary when any of these operations were executed on it.
                final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();
                final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();
                final RetentionLeases retentionLeases = shard.getRetentionLeases();
                final long mappingVersionOnPrimary = shard.indexSettings().getIndexMetaData().getMappingVersion();
                phase2(startingSeqNo, endingSeqNo, phase2Snapshot, maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases, mappingVersionOnPrimary, sendSnapshotStep);
                sendSnapshotStep.whenComplete(
                    r -> IOUtils.close(phase2Snapshot),
                    e -> {
                        IOUtils.closeWhileHandlingException(phase2Snapshot);
                        onFailure.accept(new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e));
                    });

            }, onFailure);

            // Recovery target can trim all operations >= startingSeqNo as we have sent all these operations in the phase 2
            final long trimAboveSeqNo = startingSeqNo - 1;
            sendSnapshotStep.whenComplete(r -> finalizeRecovery(r.targetLocalCheckpoint, trimAboveSeqNo, finalizeStep), onFailure);

            finalizeStep.whenComplete(r -> {
                final long phase1ThrottlingWaitTime = 0L; // TODO: return the actual throttle time
                final SendSnapshotResult sendSnapshotResult = sendSnapshotStep.result();
                final SendFileResult sendFileResult = sendFileStep.result();
                final RecoveryResponse response = new RecoveryResponse(sendFileResult.phase1FileNames, sendFileResult.phase1FileSizes,
                    sendFileResult.phase1ExistingFileNames, sendFileResult.phase1ExistingFileSizes, sendFileResult.totalSize,
                    sendFileResult.existingTotalSize, sendFileResult.took.millis(), phase1ThrottlingWaitTime,
                    prepareEngineStep.result().millis(), sendSnapshotResult.totalOperations, sendSnapshotResult.tookTime.millis());
                try {
                    wrappedListener.onResponse(response);
                } finally {
                    IOUtils.close(resources);
                }
            }, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> wrappedListener.onFailure(e));
        }
    }

    private boolean isTargetSameHistory() {
        final String targetHistoryUUID = request.metadataSnapshot().getHistoryUUID();
        assert targetHistoryUUID != null : "incoming target history missing";
        return targetHistoryUUID.equals(shard.getHistoryUUID());
    }

    static void runUnderPrimaryPermit(CancellableThreads.Interruptible runnable, String reason,
                                      IndexShard primary, CancellableThreads cancellableThreads, Logger logger) {
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
        return Releasables.releaseOnce(() -> {
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            assert threadPool.generic().isShutdown() == false;
            // TODO: We shouldn't use the generic thread pool here as we already execute this from the generic pool.
            //       While practically unlikely at a min pool size of 128 we could technically block the whole pool by waiting on futures
            //       below and thus make it impossible for the store release to execute which in turn would block the futures forever
            threadPool.generic().execute(ActionRunnable.wrap(future, l -> {
                store.decRef();
                l.onResponse(null);
            }));
            FutureUtils.get(future);
        });
    }

    static final class SendFileResult {
        final List<String> phase1FileNames;
        final List<Long> phase1FileSizes;
        final long totalSize;

        final List<String> phase1ExistingFileNames;
        final List<Long> phase1ExistingFileSizes;
        final long existingTotalSize;

        final TimeValue took;

        SendFileResult(List<String> phase1FileNames, List<Long> phase1FileSizes, long totalSize,
                       List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, long existingTotalSize, TimeValue took) {
            this.phase1FileNames = phase1FileNames;
            this.phase1FileSizes = phase1FileSizes;
            this.totalSize = totalSize;
            this.phase1ExistingFileNames = phase1ExistingFileNames;
            this.phase1ExistingFileSizes = phase1ExistingFileSizes;
            this.existingTotalSize = existingTotalSize;
            this.took = took;
        }

        static final SendFileResult EMPTY = new SendFileResult(Collections.emptyList(), Collections.emptyList(), 0L,
            Collections.emptyList(), Collections.emptyList(), 0L, TimeValue.ZERO);
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
    void phase1(IndexCommit snapshot, Consumer<ActionListener<RetentionLease>> createRetentionLease,
                IntSupplier translogOps, ActionListener<SendFileResult> listener) {
        cancellableThreads.checkForCancel();
        final Store store = shard.store();
        try {
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata;
            try {
                recoverySourceMetadata = store.getMetadata(snapshot);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                shard.failShard("recovery", ex);
                throw ex;
            }
            for (String name : snapshot.getFileNames()) {
                final StoreFileMetaData md = recoverySourceMetadata.get(name);
                if (md == null) {
                    logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                    throw new CorruptIndexException("Snapshot differs from actual index - maybe index was removed metadata has " +
                            recoverySourceMetadata.asMap().size() + " files", name);
                }
            }
            if (canSkipPhase1(recoverySourceMetadata, request.metadataSnapshot()) == false) {
                final List<String> phase1FileNames = new ArrayList<>();
                final List<Long> phase1FileSizes = new ArrayList<>();
                final List<String> phase1ExistingFileNames = new ArrayList<>();
                final List<Long> phase1ExistingFileSizes = new ArrayList<>();

                // Total size of segment files that are recovered
                long totalSizeInBytes = 0;
                // Total size of segment files that were able to be re-used
                long existingTotalSizeInBytes = 0;

                // Generate a "diff" of all the identical, different, and missing
                // segment files on the target node, using the existing files on
                // the source node
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetaData md : diff.identical) {
                    phase1ExistingFileNames.add(md.name());
                    phase1ExistingFileSizes.add(md.length());
                    existingTotalSizeInBytes += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace("recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," +
                                        " size [{}]", md.name(), md.checksum(), md.length());
                    }
                    totalSizeInBytes += md.length();
                }
                List<StoreFileMetaData> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
                phase1Files.addAll(diff.different);
                phase1Files.addAll(diff.missing);
                for (StoreFileMetaData md : phase1Files) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace("recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            md.name(), request.metadataSnapshot().asMap().get(md.name()), md);
                    } else {
                        logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                    }
                    phase1FileNames.add(md.name());
                    phase1FileSizes.add(md.length());
                    totalSizeInBytes += md.length();
                }

                logger.trace("recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                    phase1FileNames.size(), new ByteSizeValue(totalSizeInBytes),
                    phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSizeInBytes));
                final StepListener<Void> sendFileInfoStep = new StepListener<>();
                final StepListener<Void> sendFilesStep = new StepListener<>();
                final StepListener<RetentionLease> createRetentionLeaseStep = new StepListener<>();
                final StepListener<Void> cleanFilesStep = new StepListener<>();
                cancellableThreads.checkForCancel();
                recoveryTarget.receiveFileInfo(phase1FileNames, phase1FileSizes, phase1ExistingFileNames,
                        phase1ExistingFileSizes, translogOps.getAsInt(), sendFileInfoStep);

                sendFileInfoStep.whenComplete(r ->
                    sendFiles(store, phase1Files.toArray(new StoreFileMetaData[0]), translogOps, sendFilesStep), listener::onFailure);

                sendFilesStep.whenComplete(r -> createRetentionLease.accept(createRetentionLeaseStep), listener::onFailure);

                createRetentionLeaseStep.whenComplete(retentionLease ->
                    {
                        final long lastKnownGlobalCheckpoint = shard.getLastKnownGlobalCheckpoint();
                        assert retentionLease == null || retentionLease.retainingSequenceNumber() - 1 <= lastKnownGlobalCheckpoint
                            : retentionLease + " vs " + lastKnownGlobalCheckpoint;
                        // Establishes new empty translog on the replica with global checkpoint set to lastKnownGlobalCheckpoint. We want
                        // the commit we just copied to be a safe commit on the replica, so why not set the global checkpoint on the replica
                        // to the max seqno of this commit? Because (in rare corner cases) this commit might not be a safe commit here on
                        // the primary, and in these cases the max seqno would be too high to be valid as a global checkpoint.
                        cleanFiles(store, recoverySourceMetadata, translogOps, lastKnownGlobalCheckpoint, cleanFilesStep);
                    },
                    listener::onFailure);

                final long totalSize = totalSizeInBytes;
                final long existingTotalSize = existingTotalSizeInBytes;
                cleanFilesStep.whenComplete(r -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.trace("recovery [phase1]: took [{}]", took);
                    listener.onResponse(new SendFileResult(phase1FileNames, phase1FileSizes, totalSize, phase1ExistingFileNames,
                        phase1ExistingFileSizes, existingTotalSize, took));
                }, listener::onFailure);
            } else {
                logger.trace("skipping [phase1] since source and target have identical sync id [{}]", recoverySourceMetadata.getSyncId());

                // but we must still create a retention lease
                final StepListener<RetentionLease> createRetentionLeaseStep = new StepListener<>();
                createRetentionLease.accept(createRetentionLeaseStep);
                createRetentionLeaseStep.whenComplete(retentionLease -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.trace("recovery [phase1]: took [{}]", took);
                    listener.onResponse(new SendFileResult(Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyList(),
                        Collections.emptyList(), 0L, took));
                }, listener::onFailure);

            }
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), 0, new ByteSizeValue(0L), e);
        }
    }

    private void createRetentionLease(final long startingSeqNo, ActionListener<RetentionLease> listener) {
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
                    final RetentionLease clonedLease
                        = shard.cloneLocalPeerRecoveryRetentionLease(request.targetNode().getId(),
                        new ThreadedActionListener<>(logger, shard.getThreadPool(),
                            ThreadPool.Names.GENERIC, cloneRetentionLeaseStep, false));
                    logger.trace("cloned primary's retention lease as [{}]", clonedLease);
                    cloneRetentionLeaseStep.whenComplete(rr -> listener.onResponse(clonedLease), listener::onFailure);
                } catch (RetentionLeaseNotFoundException e) {
                    // it's possible that the primary has no retention lease yet if we are doing a rolling upgrade from a version before
                    // 7.4, and in that case we just create a lease using the local checkpoint of the safe commit which we're using for
                    // recovery as a conservative estimate for the global checkpoint.
                    assert shard.indexSettings().getIndexVersionCreated().before(Version.V_7_4_0);
                    final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
                    final long estimatedGlobalCheckpoint = startingSeqNo - 1;
                    final RetentionLease newLease = shard.addPeerRecoveryRetentionLease(request.targetNode().getId(),
                        estimatedGlobalCheckpoint, new ThreadedActionListener<>(logger, shard.getThreadPool(),
                            ThreadPool.Names.GENERIC, addRetentionLeaseStep, false));
                    addRetentionLeaseStep.whenComplete(rr -> listener.onResponse(newLease), listener::onFailure);
                    logger.trace("created retention lease with estimated checkpoint of [{}]", estimatedGlobalCheckpoint);
                }
            }, shardId + " establishing retention lease for [" + request.targetAllocationId() + "]",
            shard, cancellableThreads, logger);
    }

    boolean canSkipPhase1(Store.MetadataSnapshot source, Store.MetadataSnapshot target) {
        if (source.getSyncId() == null || source.getSyncId().equals(target.getSyncId()) == false) {
            return false;
        }
        if (source.getNumDocs() != target.getNumDocs()) {
            throw new IllegalStateException("try to recover " + request.shardId() + " from primary shard with sync id but number " +
                "of docs differ: " + source.getNumDocs() + " (" + request.sourceNode().getName() + ", primary) vs " + target.getNumDocs()
                + "(" + request.targetNode().getName() + ")");
        }
        SequenceNumbers.CommitInfo sourceSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(source.getCommitUserData().entrySet());
        SequenceNumbers.CommitInfo targetSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(target.getCommitUserData().entrySet());
        if (sourceSeqNos.localCheckpoint != targetSeqNos.localCheckpoint || targetSeqNos.maxSeqNo != sourceSeqNos.maxSeqNo) {
            final String message = "try to recover " + request.shardId() + " with sync id but " +
                "seq_no stats are mismatched: [" + source.getCommitUserData() + "] vs [" + target.getCommitUserData() + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
        return true;
    }

    void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
        StopWatch stopWatch = new StopWatch().start();
        final ActionListener<Void> wrappedListener = ActionListener.wrap(
            nullVal -> {
                stopWatch.stop();
                final TimeValue tookTime = stopWatch.totalTime();
                logger.trace("recovery [phase1]: remote engine start took [{}]", tookTime);
                listener.onResponse(tookTime);
            },
            e -> listener.onFailure(new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e)));
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
            final ActionListener<SendSnapshotResult> listener) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        logger.trace("recovery [phase2]: sending transaction log operations (from [" + startingSeqNo + "] to [" + endingSeqNo + "]");

        final AtomicInteger skippedOps = new AtomicInteger();
        final AtomicInteger totalSentOps = new AtomicInteger();
        final AtomicInteger lastBatchCount = new AtomicInteger(); // used to estimate the count of the subsequent batch.
        final CheckedSupplier<List<Translog.Operation>, IOException> readNextBatch = () -> {
            // We need to synchronized Snapshot#next() because it's called by different threads through sendBatch.
            // Even though those calls are not concurrent, Snapshot#next() uses non-synchronized state and is not multi-thread-compatible.
            synchronized (snapshot) {
                final List<Translog.Operation> ops = lastBatchCount.get() > 0 ? new ArrayList<>(lastBatchCount.get()) : new ArrayList<>();
                long batchSizeInBytes = 0L;
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (shard.state() == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(request.shardId());
                    }
                    cancellableThreads.checkForCancel();
                    final long seqNo = operation.seqNo();
                    if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                        skippedOps.incrementAndGet();
                        continue;
                    }
                    ops.add(operation);
                    batchSizeInBytes += operation.estimateSize();
                    totalSentOps.incrementAndGet();

                    // check if this request is past bytes threshold, and if so, send it off
                    if (batchSizeInBytes >= chunkSizeInBytes) {
                        break;
                    }
                }
                lastBatchCount.set(ops.size());
                return ops;
            }
        };

        final StopWatch stopWatch = new StopWatch().start();
        final ActionListener<Long> batchedListener = ActionListener.map(listener,
            targetLocalCheckpoint -> {
                assert snapshot.totalOperations() == snapshot.skippedOperations() + skippedOps.get() + totalSentOps.get()
                    : String.format(Locale.ROOT, "expected total [%d], overridden [%d], skipped [%d], total sent [%d]",
                    snapshot.totalOperations(), snapshot.skippedOperations(), skippedOps.get(), totalSentOps.get());
                stopWatch.stop();
                final TimeValue tookTime = stopWatch.totalTime();
                logger.trace("recovery [phase2]: took [{}]", tookTime);
                return new SendSnapshotResult(targetLocalCheckpoint, totalSentOps.get(), tookTime);
            }
        );

        sendBatch(
                readNextBatch,
                true,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                snapshot.totalOperations(),
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersion,
                batchedListener);
    }

    private void sendBatch(
            final CheckedSupplier<List<Translog.Operation>, IOException> nextBatch,
            final boolean firstBatch,
            final long targetLocalCheckpoint,
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestamp,
            final long maxSeqNoOfUpdatesOrDeletes,
            final RetentionLeases retentionLeases,
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) throws IOException {
        assert ThreadPool.assertCurrentMethodIsNotCalledRecursively();
        assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[send translog]");
        final List<Translog.Operation> operations = nextBatch.get();
        // send the leftover operations or if no operations were sent, request the target to respond with its local checkpoint
        if (operations.isEmpty() == false || firstBatch) {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperations(
                operations,
                totalTranslogOps,
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersionOnPrimary,
                ActionListener.wrap(
                    newCheckpoint -> {
                        sendBatch(
                            nextBatch,
                            false,
                            SequenceNumbers.max(targetLocalCheckpoint, newCheckpoint),
                            totalTranslogOps,
                            maxSeenAutoIdTimestamp,
                            maxSeqNoOfUpdatesOrDeletes,
                            retentionLeases,
                            mappingVersionOnPrimary,
                            listener);
                    },
                    listener::onFailure
                ));
        } else {
            listener.onResponse(targetLocalCheckpoint);
        }
    }

    void finalizeRecovery(long targetLocalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) throws IOException {
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
        runUnderPrimaryPermit(() -> shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint),
            shardId + " marking " + request.targetAllocationId() + " as in sync", shard, cancellableThreads, logger);
        final long globalCheckpoint = shard.getLastKnownGlobalCheckpoint(); // this global checkpoint is persisted in finalizeRecovery
        final StepListener<Void> finalizeListener = new StepListener<>();
        cancellableThreads.checkForCancel();
        recoveryTarget.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, finalizeListener);
        finalizeListener.whenComplete(r -> {
            runUnderPrimaryPermit(() -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),
                shardId + " updating " + request.targetAllocationId() + "'s global checkpoint", shard, cancellableThreads, logger);

            if (request.isPrimaryRelocation()) {
                logger.trace("performing relocation hand-off");
                // TODO: make relocated async
                // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
                cancellableThreads.execute(() -> shard.relocated(request.targetAllocationId(), recoveryTarget::handoffPrimaryContext));
                /*
                 * if the recovery process fails after disabling primary mode on the source shard, both relocation source and
                 * target are failed (see {@link IndexShard#updateRoutingEntry}).
                 */
            }
            stopWatch.stop();
            logger.trace("finalizing recovery took [{}]", stopWatch.totalTime());
            listener.onResponse(null);
        }, listener::onFailure);
    }

    static final class SendSnapshotResult {
        final long targetLocalCheckpoint;
        final int totalOperations;
        final TimeValue tookTime;

        SendSnapshotResult(final long targetLocalCheckpoint, final int totalOperations, final TimeValue tookTime) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.totalOperations = totalOperations;
            this.tookTime = tookTime;
        }
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
    }

    @Override
    public String toString() {
        return "ShardRecoveryHandler{" +
                "shardId=" + request.shardId() +
                ", sourceNode=" + request.sourceNode() +
                ", targetNode=" + request.targetNode() +
                '}';
    }

    private static class FileChunk implements MultiFileTransfer.ChunkRequest {
        final StoreFileMetaData md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;

        FileChunk(StoreFileMetaData md, BytesReference content, long position, boolean lastChunk) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }
    }

    void sendFiles(Store store, StoreFileMetaData[] files, IntSupplier translogOps, ActionListener<Void> listener) {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetaData::length)); // send smallest first

        final MultiFileTransfer<FileChunk> multiFileSender =
            new MultiFileTransfer<>(logger, threadPool.getThreadContext(), listener, maxConcurrentFileChunks, Arrays.asList(files)) {

                final byte[] buffer = new byte[chunkSizeInBytes];
                InputStreamIndexInput currentInput = null;
                long offset = 0;

                @Override
                protected void onNewFile(StoreFileMetaData md) throws IOException {
                    offset = 0;
                    IOUtils.close(currentInput, () -> currentInput = null);
                    final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
                    currentInput = new InputStreamIndexInput(indexInput, md.length()) {
                        @Override
                        public void close() throws IOException {
                            IOUtils.close(indexInput, super::close); // InputStreamIndexInput's close is a noop
                        }
                    };
                }

                @Override
                protected FileChunk nextChunkRequest(StoreFileMetaData md) throws IOException {
                    assert Transports.assertNotTransportThread("read file chunk");
                    cancellableThreads.checkForCancel();
                    final int bytesRead = currentInput.read(buffer);
                    if (bytesRead == -1) {
                        throw new CorruptIndexException("file truncated; length=" + md.length() + " offset=" + offset, md.name());
                    }
                    final boolean lastChunk = offset + bytesRead == md.length();
                    final FileChunk chunk = new FileChunk(md, new BytesArray(buffer, 0, bytesRead), offset, lastChunk);
                    offset += bytesRead;
                    return chunk;
                }

                @Override
                protected void sendChunkRequest(FileChunk request, ActionListener<Void> listener) {
                    cancellableThreads.checkForCancel();
                    recoveryTarget.writeFileChunk(
                        request.md, request.position, request.content, request.lastChunk, translogOps.getAsInt(), listener);
                }

                @Override
                protected void handleError(StoreFileMetaData md, Exception e) throws Exception {
                    handleErrorOnSendFiles(store, e, new StoreFileMetaData[]{md});
                }

                @Override
                public void close() throws IOException {
                    IOUtils.close(currentInput, () -> currentInput = null);
                }
            };
        resources.add(multiFileSender);
        multiFileSender.start();
    }

    private void cleanFiles(Store store, Store.MetadataSnapshot sourceMetadata, IntSupplier translogOps,
                            long globalCheckpoint, ActionListener<Void> listener) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        cancellableThreads.checkForCancel();
        recoveryTarget.cleanFiles(translogOps.getAsInt(), globalCheckpoint, sourceMetadata,
            ActionListener.delegateResponse(listener, (l, e) -> ActionListener.completeWith(l, () -> {
                StoreFileMetaData[] mds = StreamSupport.stream(sourceMetadata.spliterator(), false).toArray(StoreFileMetaData[]::new);
                ArrayUtil.timSort(mds, Comparator.comparingLong(StoreFileMetaData::length)); // check small files first
                handleErrorOnSendFiles(store, e, mds);
                throw e;
            })));
    }

    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetaData[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(RecoverySourceHandler.this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetaData md : mds) {
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
                    "File corruption occurred on recovery but checksums are ok", null);
                remoteException.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node {}, recovering {}. local checksum OK",
                    shardId, request.targetNode(), mds), corruptIndexException);
                throw remoteException;
            }
        }
        throw e;
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
