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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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
    private final LongSupplier relativeTimeInMillis;
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();
    private final AtomicReference<BaseFuture<RecoveryResponse>> recoveryFutureRef = new AtomicReference<>(); // for cancel purpose

    private final CancellableThreads cancellableThreads = new CancellableThreads() {
        @Override
        protected void onCancel(String reason, @Nullable Exception suppressedException) {
            RuntimeException e;
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                e = new IndexShardClosedException(shard.shardId(), "shard is closed and recovery was canceled reason [" + reason + "]");
            } else {
                e = new ExecutionCancelledException("recovery was canceled reason [" + reason + "]");
            }
            if (suppressedException != null) {
                e.addSuppressed(suppressedException);
            }
            final BaseFuture<RecoveryResponse> future = recoveryFutureRef.getAndSet(null);
            if (future != null) {
                future.completeExceptionally(e);
            }
            throw e;
        }
    };

    public RecoverySourceHandler(IndexShard shard, RecoveryTargetHandler recoveryTarget, LongSupplier relativeTimeInMillis,
                                 StartRecoveryRequest request, int fileChunkSizeInBytes) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.request = request;
        this.shardId = this.request.shardId().id();
        this.logger = Loggers.getLogger(getClass(), request.shardId(), "recover to " + request.targetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
    }

    public StartRecoveryRequest getRequest() {
        return request;
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public BaseFuture<RecoveryResponse> recoverToTarget() {
        resources.add(shard.acquireRetentionLockForPeerRecovery());
        try {
            runUnderPrimaryPermit(() -> {
                final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
                if (targetShardRouting == null) {
                    logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(),
                        request.targetNode());
                    throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
                }
                assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
            }, shardId + " validating recovery target ["+ request.targetAllocationId() + "]", shard, cancellableThreads, logger);

            final BaseFuture<Phase1Result> phase1Future;
            final long startingSeqNo;
            final long requiredSeqNoRangeStart;
            final boolean isSequenceNumberBasedRecovery = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO &&
                isTargetSameHistory() && shard.hasCompleteHistoryOperations("peer-recovery", request.startingSeqNo());
            if (isSequenceNumberBasedRecovery) {
                logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
                startingSeqNo = request.startingSeqNo();
                requiredSeqNoRangeStart = startingSeqNo;
                phase1Future = BaseFuture.completedFuture(Phase1Result.EMPTY); // skip phase1 entirely
            } else {
                final Engine.IndexCommitRef phase1Snapshot;
                try {
                    phase1Snapshot = shard.acquireSafeIndexCommit();
                    resources.add(phase1Snapshot);
                } catch (final Exception e) {
                    throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
                }
                // We must have everything above the local checkpoint in the commit
                requiredSeqNoRangeStart =
                    Long.parseLong(phase1Snapshot.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1;
                // If soft-deletes enabled, we need to transfer only operations after the local_checkpoint of the commit to have
                // the same history on the target. However, with translog, we need to set this to 0 to create a translog roughly
                // according to the retention policy on the target. Note that it will still filter out legacy operations without seqNo.
                startingSeqNo = shard.indexSettings().isSoftDeleteEnabled() ? requiredSeqNoRangeStart : 0;
                try {
                    final int translogOps = shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo);
                    phase1Future = phase1(phase1Snapshot.getIndexCommit(), () -> translogOps)
                        .whenComplete((r, e) -> IOUtils.closeWhileHandlingException(phase1Snapshot));
                } catch (final Exception e) {
                    IOUtils.closeWhileHandlingException(phase1Snapshot);
                    throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e);
                }
            }
            assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;
            assert requiredSeqNoRangeStart >= startingSeqNo : "requiredSeqNoRangeStart [" + requiredSeqNoRangeStart + "] is lower than ["
                + startingSeqNo + "]";

            final BaseFuture<RecoveryResponse> recoveryFuture = phase1Future
                .thenCompose(phase1Result -> {
                    final int translogOps;
                    try {
                        translogOps = shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo);
                    } catch (IOException e) {
                        throw new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e);
                    }
                    // For a sequence based recovery, the target can keep its local translog
                    return prepareTargetForTranslog(isSequenceNumberBasedRecovery == false, translogOps)
                        .thenApply(prepareTime -> Tuple.tuple(phase1Result, prepareTime));

                }).whenComplete((r, e) -> {
                    /*
                     * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
                     * This means that any document indexed into the primary after this will be replicated to this replica as well
                     * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
                     * all documents up to maxSeqNo in phase2.
                     */
                    if (e == null) {
                        runUnderPrimaryPermit(() -> shard.initiateTracking(request.targetAllocationId()),
                            shardId + " initiating tracking of " + request.targetAllocationId(), shard, cancellableThreads, logger);
                    }

                }).thenCompose(rs -> {
                    final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
                    /*
                     * We need to wait for all operations up to the current max to complete, otherwise we can not guarantee that all
                     * operations in the required range will be available for replaying from the translog of the source.
                     */
                    cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));
                    Translog.Snapshot phase2Snapshot = null;
                    try {
                        phase2Snapshot = shard.getHistoryOperations("peer-recovery", startingSeqNo);
                        resources.add(phase2Snapshot);
                        if (logger.isTraceEnabled()) {
                            logger.trace("all operations up to [{}] completed, which will be used as an ending seq_no", endingSeqNo);
                            logger.trace("snapshot translog for recovery; current size is [{}]", phase2Snapshot.totalOperations());
                        }
                        // we have to capture the max_seen_auto_id_timestamp and the max_seq_no_of_updates to make sure that these values
                        // are at least as high as the corresponding values on the primary when any of these operations were executed on it.
                        final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();
                        final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();
                        final Translog.Snapshot snapshot = phase2Snapshot;
                        return phase2(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, phase2Snapshot,
                            maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes)
                            .whenComplete((r, e) -> IOUtils.closeWhileHandlingException(snapshot))
                            .thenApply(phase2Result -> Tuple.tuple(rs, phase2Result));
                    } catch (Exception e) {
                        IOUtils.closeWhileHandlingException(phase2Snapshot);
                        throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
                    }

                }).thenCompose(rs -> {
                    try {
                        return finalizeRecovery(rs.v2().targetLocalCheckpoint).thenApply(ignored -> rs);
                    } catch (IOException e) {
                        throw new RecoveryEngineException(shard.shardId(), 2, "finalized failed", e);
                    }
                })
                .whenComplete((r, e) -> IOUtils.closeWhileHandlingException(resources))
                .thenApply(rs -> toResponse(rs.v1().v1(), rs.v1().v2(), rs.v2()));
            assert recoveryFutureRef.get() == null : "future recovery is set already";
            recoveryFutureRef.set(recoveryFuture);
            return recoveryFuture;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(resources);
            return BaseFuture.failedFuture(e);
        }
    }

    private boolean isTargetSameHistory() {
        final String targetHistoryUUID = request.metadataSnapshot().getHistoryUUID();
        assert targetHistoryUUID != null || shard.indexSettings().getIndexVersionCreated().before(Version.V_6_0_0_rc1) :
            "incoming target history N/A but index was created after or on 6.0.0-rc1";
        return targetHistoryUUID != null && targetHistoryUUID.equals(shard.getHistoryUUID());
    }

    static void runUnderPrimaryPermit(CancellableThreads.Interruptible runnable, String reason,
                                      IndexShard primary, CancellableThreads cancellableThreads, Logger logger) {
        cancellableThreads.execute(() -> {
            BaseFuture<Releasable> permit = new BaseFuture<>();
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

    static final class Phase1Result {
        final List<String> phase1FileNames;
        final List<Long> phase1FileSizes;
        final long totalSizeInBytes;
        final List<String> existingFileNames;
        final List<Long> existingFileSizes;
        final long reuseSizeInBytes;
        final TimeValue took;

        Phase1Result(List<String> phase1FileNames, List<Long> phase1FileSizes, long totalSizeInBytes,
                     List<String> existingFileNames, List<Long> existingFileSizes, long reuseSizeInBytes, TimeValue took) {
            this.phase1FileNames = phase1FileNames;
            this.phase1FileSizes = phase1FileSizes;
            this.totalSizeInBytes = totalSizeInBytes;
            this.existingFileNames = existingFileNames;
            this.existingFileSizes = existingFileSizes;
            this.reuseSizeInBytes = reuseSizeInBytes;
            this.took = took;
        }

        static final Phase1Result EMPTY = new Phase1Result(
            Collections.emptyList(), Collections.emptyList(), 0L,
            Collections.emptyList(), Collections.emptyList(), 0, new TimeValue(0));
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
    BaseFuture<Phase1Result> phase1(final IndexCommit snapshot, final Supplier<Integer> translogOps) {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
        final List<String> phase1FileNames = new ArrayList<>();
        final List<Long> phase1FileSizes = new ArrayList<>();
        final List<String> existingFileNames = new ArrayList<>();
        final List<Long> existingFileSizes = new ArrayList<>();
        final Store store = shard.store();
        try (Releasable ignored = retainStore(store)) {
            final long startTimeInMs = relativeTimeInMillis.getAsLong();
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
            // Generate a "diff" of all the identical, different, and missing
            // segment files on the target node, using the existing files on
            // the source node
            String recoverySourceSyncId = recoverySourceMetadata.getSyncId();
            String recoveryTargetSyncId = request.metadataSnapshot().getSyncId();
            final boolean recoverWithSyncId = recoverySourceSyncId != null &&
                recoverySourceSyncId.equals(recoveryTargetSyncId);
            if (recoverWithSyncId) {
                final long numDocsTarget = request.metadataSnapshot().getNumDocs();
                final long numDocsSource = recoverySourceMetadata.getNumDocs();
                if (numDocsTarget != numDocsSource) {
                    throw new IllegalStateException("try to recover " + request.shardId() + " from primary shard with sync id but number " +
                        "of docs differ: " + numDocsSource + " (" + request.sourceNode().getName() + ", primary) vs " + numDocsTarget
                        + "(" + request.targetNode().getName() + ")");
                }
                // we shortcut recovery here because we have nothing to copy. but we must still start the engine on the target.
                // so we don't return here
                logger.trace("skipping [phase1]- identical sync id [{}] found on both source and target", recoverySourceSyncId);
                return BaseFuture.completedFuture(Phase1Result.EMPTY); // skip phase 1
            } else {
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());

                for (StoreFileMetaData md : diff.identical) {
                    existingFileNames.add(md.name());
                    existingFileSizes.add(md.length());
                    existingTotalSize += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace("recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," +
                            " size [{}]", md.name(), md.checksum(), md.length());
                    }
                    totalSize += md.length();
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
                    totalSize += md.length();
                }

                logger.trace("recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                    phase1FileNames.size(), new ByteSizeValue(totalSize), existingFileNames.size(), new ByteSizeValue(existingTotalSize));
                // extra store reference for these futures
                final Releasable extraStoreRef = retainStore(store);
                resources.add(extraStoreRef);
                final long totalSizeInBytes = totalSize;
                final long reuseSizeInBytes = existingTotalSize;
                return interruptibleFuture(() -> recoveryTarget.receiveFileInfo(phase1FileNames, phase1FileSizes,
                    existingFileNames, existingFileSizes, translogOps.get()))
                    .thenCompose(r -> sendFiles(store, phase1Files.toArray(new StoreFileMetaData[0]), translogOps))
                    .thenCompose(r -> cleanFiles(store, snapshot, translogOps, recoverySourceMetadata))
                    .whenComplete((r, e) -> IOUtils.closeWhileHandlingException(extraStoreRef))
                    .thenApply(r -> {
                        final long tookInMs = relativeTimeInMillis.getAsLong() - startTimeInMs;
                        logger.trace("recovery [phase1]: took [{}]", tookInMs);
                        return new Phase1Result(phase1FileNames, phase1FileSizes, totalSizeInBytes,
                            existingFileNames, existingFileSizes, reuseSizeInBytes, new TimeValue(tookInMs));
                    });
            }
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        }
    }

    BaseFuture<TimeValue> prepareTargetForTranslog(final boolean fileBasedRecovery, final int totalTranslogOps) {
        final long startTimeInMs = relativeTimeInMillis.getAsLong();
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        // Send a request preparing the new shard's translog to receive operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        return interruptibleFuture(() -> recoveryTarget.prepareForTranslogOperations(fileBasedRecovery, totalTranslogOps))
            .exceptionally(e -> {
                throw new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e);
            })
            .thenApply(r -> {
                final TimeValue tookTime = new TimeValue(relativeTimeInMillis.getAsLong() - startTimeInMs);
                logger.trace("recovery [phase1]: remote engine start took [{}]", tookTime);
                return tookTime;
            });
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
     * @param requiredSeqNoRangeStart    the lower sequence number of the required range (ending with endingSeqNo)
     * @param endingSeqNo                the highest sequence number that should be sent
     * @param snapshot                   a snapshot of the translog
     * @param maxSeenAutoIdTimestamp     the max auto_id_timestamp of append-only requests on the primary
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates or deletes on the primary after these operations were executed on it.
     * @return the local checkpoint on the target
     */
    BaseFuture<SendSnapshotResult> phase2(long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo, Translog.Snapshot snapshot,
                                          long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();

        logger.trace("recovery [phase2]: sending transaction log operations (seq# from [" +  startingSeqNo  + "], " +
            "required [" + requiredSeqNoRangeStart + ":" + endingSeqNo + "]");

        final long startTimeInMs = relativeTimeInMillis.getAsLong();
        final SnapshotSender snapshotSender = new SnapshotSender(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, snapshot,
            maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes);

        final BaseFuture<Tuple<Long, Integer>> future = new BaseFuture<>();
        snapshotSender.sendNextBatch(true, future);
        return future.thenApply(rs -> {
            final TimeValue took = new TimeValue(relativeTimeInMillis.getAsLong() - startTimeInMs);
            logger.trace("recovery [phase2]: took [{}]", took);
            return new SendSnapshotResult(rs.v1(), rs.v2(), took);
        });
    }

    /*
     * finalizes the recovery process
     */
    public BaseFuture<Void> finalizeRecovery(final long targetLocalCheckpoint) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        final long startTimeInMs = relativeTimeInMillis.getAsLong();
        logger.trace("finalizing recovery");
        /*
         * Before marking the shard as in-sync we acquire an operation permit. We do this so that there is a barrier between marking a
         * shard as in-sync and relocating a shard. If we acquire the permit then no relocation handoff can complete before we are done
         * marking the shard as in-sync. If the relocation handoff holds all the permits then after the handoff completes and we acquire
         * the permit then the state of the shard will be relocated and this recovery will fail.
         */
        runUnderPrimaryPermit(() -> shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint),
            shardId + " marking " + request.targetAllocationId() + " as in sync", shard, cancellableThreads, logger);
        final long globalCheckpoint = shard.getGlobalCheckpoint();
        return interruptibleFuture(() -> recoveryTarget.finalizeRecovery(globalCheckpoint))
            .thenRun(() -> {
                runUnderPrimaryPermit(() -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),
                    shardId + " updating " + request.targetAllocationId() + "'s global checkpoint", shard, cancellableThreads, logger);
            })
            .thenRun(() -> {
                // TODO: make `relocated` support async.
                if (request.isPrimaryRelocation()) {
                    logger.trace("performing relocation hand-off");
                    // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
                    cancellableThreads.execute(() -> shard.relocated(recoveryTarget::handoffPrimaryContext));
                    /*
                     * if the recovery process fails after disabling primary mode on the source shard, both relocation source and
                     * target are failed (see {@link IndexShard#updateRoutingEntry}).
                     */
                }
                logger.trace("finalizing recovery took [{}]",
                    new TimeValue(relativeTimeInMillis.getAsLong() - startTimeInMs, TimeUnit.MILLISECONDS));
            });
    }

    static class SendSnapshotResult {
        final long targetLocalCheckpoint;
        final int totalOperations;
        final TimeValue tookTime;

        SendSnapshotResult(long targetLocalCheckpoint, int totalOperations, TimeValue tookTime) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.totalOperations = totalOperations;
            this.tookTime = tookTime;
        }
    }

    final class SnapshotSender {
        private AtomicInteger skippedOps = new AtomicInteger();
        private AtomicInteger totalSentOps = new AtomicInteger();
        private final int expectedTotalOps;

        private final long startingSeqNo;
        private final long requiredSeqNoRangeStart;
        private final long endingSeqNo;
        private final long maxSeenAutoIdTimestamp;
        private final long maxSeqNoOfUpdatesOrDeletes;
        private AtomicLong localCheckpointOnTarget = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        private final LocalCheckpointTracker requiredOpsTracker;
        private final Translog.Snapshot snapshot;

        SnapshotSender(long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo,
                       Translog.Snapshot snapshot, long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes) {
            this.startingSeqNo = startingSeqNo;
            this.requiredSeqNoRangeStart = requiredSeqNoRangeStart;
            this.endingSeqNo = endingSeqNo;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.requiredOpsTracker = new LocalCheckpointTracker(endingSeqNo, requiredSeqNoRangeStart - 1);
            this.snapshot = snapshot;
            this.expectedTotalOps = snapshot.totalOperations();
        }

        void sendNextBatch(boolean firstBatch, BaseFuture<Tuple<Long, Integer>> future) {
            try {
                cancellableThreads.executeIO(() -> {
                    final List<Translog.Operation> batch = readNextBatch();
                    // send the leftover operations or if no operations were sent, request the target to respond with its local checkpoint
                    if (batch.isEmpty() == false || firstBatch) {
                        recoveryTarget.indexTranslogOperations(batch, snapshot.totalOperations(),
                            maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes).whenComplete((checkpoint, e) -> {
                            if (e == null) {
                                this.localCheckpointOnTarget.set(checkpoint);
                                sendNextBatch(false, future);
                            } else {
                                future.completeExceptionally(e);
                            }
                        });
                    } else {
                        assert expectedTotalOps == snapshot.skippedOperations() + skippedOps.get() + totalSentOps.get()
                            : String.format(Locale.ROOT, "expected total [%d], overridden [%d], skipped [%d], total sent [%d]",
                            expectedTotalOps, snapshot.skippedOperations(), skippedOps.get(), totalSentOps.get());
                        assert localCheckpointOnTarget.get() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                        if (requiredOpsTracker.getCheckpoint() < endingSeqNo) {
                            throw new IllegalStateException("translog replay failed to cover required sequence numbers" +
                                " (required range [" + requiredSeqNoRangeStart + ":" + endingSeqNo + "). first missing op is ["
                                + (requiredOpsTracker.getCheckpoint() + 1) + "]");
                        }
                        future.complete(Tuple.tuple(localCheckpointOnTarget.get(), totalSentOps.get()));
                    }
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        synchronized List<Translog.Operation> readNextBatch() throws IOException{
            final List<Translog.Operation> batch = new ArrayList<>();
            long batchSizeInBytes = 0;
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
                batch.add(operation);
                batchSizeInBytes += operation.estimateSize();
                totalSentOps.incrementAndGet();
                requiredOpsTracker.markSeqNoAsCompleted(seqNo);
                // check if this request is past bytes threshold, and if so, send it off
                if (batchSizeInBytes >= chunkSizeInBytes) {
                    break;
                }
            }
            logger.trace("sent batch of [{}][{}] (total: [{}]) translog operations",
                batch, new ByteSizeValue(batchSizeInBytes), expectedTotalOps);
            return batch;
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

    final class FileChunkSender implements Closeable {
        private final Store store;
        private final List<StoreFileMetaData> files;
        private final Supplier<Integer> translogOps;
        private ChunkReader current = null;
        private final AtomicBoolean closed = new AtomicBoolean();

        FileChunkSender(Store store, StoreFileMetaData[] files, Supplier<Integer> translogOps) {
            this.store = store;
            this.files = new ArrayList<>(Arrays.asList(files));
            this.translogOps = translogOps;
        }

        void sendNextChunk(BaseFuture<Void> future) {
            try {
                cancellableThreads.executeIO(() -> {
                    final FileChunk chunk = readNextChunk();
                    if (chunk == null) {
                        future.complete(null);
                    } else {
                        recoveryTarget.writeFileChunk(chunk.md, chunk.position, chunk.content, chunk.lastChunk, translogOps.get())
                            .whenComplete((r, e) -> {
                                if (e != null) {
                                    handleError(future, chunk.md, e);
                                } else {
                                    sendNextChunk(future);
                                }
                            });
                    }
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        private synchronized FileChunk readNextChunk() throws IOException {
            cancellableThreads.checkForCancel();
            StoreFileMetaData md = null;
            try {
                if (current == null) {
                    if (files.isEmpty()) {
                        return null;
                    }
                    md = files.remove(0);
                    current = new ChunkReader(store, md, chunkSizeInBytes);
                }
                md = current.md;
                final FileChunk nextChunk = current.nextChunk();
                if (nextChunk.lastChunk) {
                    IOUtils.close(current);
                    current = null;
                }
                return nextChunk;
            } catch (IOException e) {
                final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
                if (corruptIndexException != null && store.checkIntegrityNoException(md) == false) {
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                    failEngine(corruptIndexException);
                    throw corruptIndexException;
                } else {
                    throw e;
                }
            }
        }

        synchronized void handleError(BaseFuture<?> future, StoreFileMetaData md, Throwable e) {
            try {
                final Throwable actual;
                final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
                if (corruptIndexException != null && md != null) {
                    if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                        logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                        failEngine(corruptIndexException);
                        actual = corruptIndexException;
                    } else { // corruption has happened on the way to replica
                        logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node {}, recovering {}. local checksum OK",
                            shardId, request.targetNode(), md), corruptIndexException);
                        actual = new RemoteTransportException("File corruption occurred on recovery but checksums are ok", null);
                        actual.addSuppressed(e);
                    }
                } else {
                    actual = e;
                }
                future.completeExceptionally(actual);
            } catch (Exception inner) {
                e.addSuppressed(inner);
                future.completeExceptionally(e);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(current);
            }
        }
    }

    static final class FileChunk {
        final StoreFileMetaData md;
        final int position;
        final BytesArray content;
        final boolean lastChunk;

        FileChunk(StoreFileMetaData md, int position, BytesArray content, boolean lastChunk) {
            this.md = md;
            this.position = position;
            this.content = content;
            this.lastChunk = lastChunk;
        }
    }

    static final class ChunkReader implements Closeable {
        private final StoreFileMetaData md;
        private final InputStream reader;
        private final byte[] buffer;
        private int position;

        ChunkReader(Store store, StoreFileMetaData md, int chunkSizeInBytes) throws IOException {
            this.md = md;
            this.position = 0;
            this.buffer = new byte[chunkSizeInBytes];
            final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
            this.reader = new BufferedInputStream(new InputStreamIndexInput(indexInput, md.length()) {
                @Override
                public void close() throws IOException {
                    indexInput.close();
                }
            }, chunkSizeInBytes);
        }

        synchronized FileChunk nextChunk() throws IOException {
            final int bytesRead = reader.read(buffer, 0, buffer.length);
            final boolean lastChunk = position + bytesRead == md.length();
            final FileChunk chunk = new FileChunk(md, position, new BytesArray(buffer, 0, bytesRead), lastChunk);
            position += bytesRead;
            return chunk;
        }

        @Override
        public synchronized void close() throws IOException {
            IOUtils.close(reader);
        }
    }


    BaseFuture<Void> sendFiles(Store store, StoreFileMetaData[] files, Supplier<Integer> translogOps) {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetaData::length)); // send smallest first
        final FileChunkSender sender = new FileChunkSender(store, files, translogOps);
        resources.add(sender);
        final BaseFuture<Void> future = new BaseFuture<>();
        sender.sendNextChunk(future);
        return future.whenComplete((r, e) -> IOUtils.closeWhileHandlingException(sender));
    }

    private BaseFuture<Void> cleanFiles(Store store, IndexCommit snapshot, Supplier<Integer> translogOps,
                                        Store.MetadataSnapshot recoverySourceMetadata) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        return interruptibleFuture(() -> recoveryTarget.cleanFiles(translogOps.get(), recoverySourceMetadata))
            .exceptionally(cause -> {
                final Exception targetException = (Exception) cause;
                if (targetException instanceof RemoteTransportException || targetException instanceof IOException) {
                    // we realized that after the index was copied and we wanted to finalize the recovery
                    // the index was corrupted:
                    //   - maybe due to a broken segments file on an empty index (transferred with no checksum)
                    //   - maybe due to old segments without checksums or length only checks
                    final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(targetException);
                    if (corruptIndexException != null) {
                        try {
                            final Store.MetadataSnapshot recoverySourceMetadata1 = store.getMetadata(snapshot);
                            StoreFileMetaData[] metadata =
                                StreamSupport.stream(recoverySourceMetadata1.spliterator(), false).toArray(StoreFileMetaData[]::new);
                            ArrayUtil.timSort(metadata, Comparator.comparingLong(StoreFileMetaData::length)); // check small files first
                            for (StoreFileMetaData md : metadata) {
                                cancellableThreads.checkForCancel();
                                logger.debug("checking integrity for file {} after remove corruption exception", md);
                                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                                    failEngine(corruptIndexException);
                                    logger.warn("Corrupted file detected {} checksum mismatch", md);
                                    throw corruptIndexException;
                                }
                            }
                        } catch (IOException ex) {
                            targetException.addSuppressed(ex);
                        }
                        // corruption has happened on the way to replica
                        RemoteTransportException exception = new RemoteTransportException(
                            "File corruption occurred on recovery but checksums are ok", null);
                        exception.addSuppressed(targetException);
                        logger.warn(() -> new ParameterizedMessage(
                            "{} Remote file corruption during finalization of recovery on node {}. local checksum OK",
                            shard.shardId(), request.targetNode()), corruptIndexException);
                        throw exception;
                    }
                }
                throw ExceptionsHelper.convertToRuntime(targetException);
            });
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }

    private <V> BaseFuture<V> interruptibleFuture(CheckedSupplier<BaseFuture<V>, ? extends IOException> supplier) {
        try {
            final AtomicReference<BaseFuture<V>> result = new AtomicReference<>();
            cancellableThreads.executeIO(() -> result.set(supplier.get()));
            return result.get();
        } catch (IOException e) {
            return BaseFuture.failedFuture(e);
        }
    }

    private Releasable retainStore(Store store) {
        final AtomicBoolean released = new AtomicBoolean();
        store.incRef();
        return () -> {
            if (released.compareAndSet(false, true)) {
                store.decRef();
            }
        };
    }

    private RecoveryResponse toResponse(Phase1Result phase1Result, TimeValue startTime, SendSnapshotResult sendSnapshotResult) {
        RecoveryResponse response = new RecoveryResponse();
        response.phase1FileNames.addAll(phase1Result.phase1FileNames);
        response.phase1FileSizes.addAll(phase1Result.existingFileSizes);
        response.phase1TotalSize = phase1Result.totalSizeInBytes;
        response.phase1ExistingFileNames.addAll(phase1Result.existingFileNames);
        response.phase1ExistingFileSizes.addAll(phase1Result.existingFileSizes);
        response.phase1ExistingTotalSize = phase1Result.reuseSizeInBytes;

        response.startTime = startTime.millis();
        response.phase2Operations = sendSnapshotResult.totalOperations;
        response.phase2Time = sendSnapshotResult.tookTime.millis();
        return response;
    }
}
