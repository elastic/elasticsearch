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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.core.internal.io.Streams;
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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
            throw e;
        }
    };

    public RecoverySourceHandler(final IndexShard shard, RecoveryTargetHandler recoveryTarget,
                                 final StartRecoveryRequest request,
                                 final int fileChunkSizeInBytes) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
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
    public RecoveryResponse recoverToTarget() throws IOException {
        runUnderPrimaryPermit(() -> {
            final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
            ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
            if (targetShardRouting == null) {
                logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(),
                    request.targetNode());
                throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
            }
            assert targetShardRouting.initializing() : "expected recovery target to be initializing but was " + targetShardRouting;
        }, shardId + " validating recovery target ["+ request.targetAllocationId() + "] registered ", shard, cancellableThreads, logger);

        try (Closeable ignored = shard.acquireRetentionLockForPeerRecovery()) {
            final long startingSeqNo;
            final long requiredSeqNoRangeStart;
            final boolean isSequenceNumberBasedRecovery = request.startingSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO &&
                isTargetSameHistory() && shard.hasCompleteHistoryOperations("peer-recovery", request.startingSeqNo());
            final SendFileResult sendFileResult;
            if (isSequenceNumberBasedRecovery) {
                logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
                startingSeqNo = request.startingSeqNo();
                requiredSeqNoRangeStart = startingSeqNo;
                sendFileResult = SendFileResult.EMPTY;
            } else {
                final Engine.IndexCommitRef phase1Snapshot;
                try {
                    phase1Snapshot = shard.acquireSafeIndexCommit();
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
                    final int estimateNumOps = shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo);
                    sendFileResult = phase1(phase1Snapshot.getIndexCommit(), () -> estimateNumOps);
                } catch (final Exception e) {
                    throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e);
                } finally {
                    try {
                        IOUtils.close(phase1Snapshot);
                    } catch (final IOException ex) {
                        logger.warn("releasing snapshot caused exception", ex);
                    }
                }
            }
            assert startingSeqNo >= 0 : "startingSeqNo must be non negative. got: " + startingSeqNo;
            assert requiredSeqNoRangeStart >= startingSeqNo : "requiredSeqNoRangeStart [" + requiredSeqNoRangeStart + "] is lower than ["
                + startingSeqNo + "]";

            final TimeValue prepareEngineTime;
            try {
                // For a sequence based recovery, the target can keep its local translog
                prepareEngineTime = prepareTargetForTranslog(isSequenceNumberBasedRecovery == false,
                    shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo));
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e);
            }

            /*
             * add shard to replication group (shard will receive replication requests from this point on) now that engine is open.
             * This means that any document indexed into the primary after this will be replicated to this replica as well
             * make sure to do this before sampling the max sequence number in the next step, to ensure that we send
             * all documents up to maxSeqNo in phase2.
             */
            runUnderPrimaryPermit(() -> shard.initiateTracking(request.targetAllocationId()),
                shardId + " initiating tracking of " + request.targetAllocationId(), shard, cancellableThreads, logger);

            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
            /*
             * We need to wait for all operations up to the current max to complete, otherwise we can not guarantee that all
             * operations in the required range will be available for replaying from the translog of the source.
             */
            cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));

            if (logger.isTraceEnabled()) {
                logger.trace("all operations up to [{}] completed, which will be used as an ending sequence number", endingSeqNo);
                logger.trace("snapshot translog for recovery; current size is [{}]",
                    shard.estimateNumberOfHistoryOperations("peer-recovery", startingSeqNo));
            }
            final SendSnapshotResult sendSnapshotResult;
            try (Translog.Snapshot snapshot = shard.getHistoryOperations("peer-recovery", startingSeqNo)) {
                // we have to capture the max_seen_auto_id_timestamp and the max_seq_no_of_updates to make sure that these values
                // are at least as high as the corresponding values on the primary when any of these operations were executed on it.
                final long maxSeenAutoIdTimestamp = shard.getMaxSeenAutoIdTimestamp();
                final long maxSeqNoOfUpdatesOrDeletes = shard.getMaxSeqNoOfUpdatesOrDeletes();
                sendSnapshotResult = phase2(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, snapshot,
                    maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes);
            } catch (Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery(sendSnapshotResult.targetLocalCheckpoint);
            final long phase1ThrottlingWaitTime = 0L; // TODO: return the actual throttle time
            return new RecoveryResponse(sendFileResult.phase1FileNames, sendFileResult.phase1FileSizes,
                sendFileResult.phase1ExistingFileNames, sendFileResult.phase1ExistingFileSizes, sendFileResult.totalSize,
                sendFileResult.existingTotalSize, sendFileResult.took.millis(), phase1ThrottlingWaitTime, prepareEngineTime.millis(),
                sendSnapshotResult.totalOperations, sendSnapshotResult.tookTime.millis());
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
    public SendFileResult phase1(final IndexCommit snapshot, final Supplier<Integer> translogOps) {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
        final List<String> phase1FileNames = new ArrayList<>();
        final List<Long> phase1FileSizes = new ArrayList<>();
        final List<String> phase1ExistingFileNames = new ArrayList<>();
        final List<Long> phase1ExistingFileSizes = new ArrayList<>();
        final Store store = shard.store();
        store.incRef();
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
            } else {
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetaData md : diff.identical) {
                    phase1ExistingFileNames.add(md.name());
                    phase1ExistingFileSizes.add(md.length());
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
                    phase1FileNames.size(), new ByteSizeValue(totalSize),
                    phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));
                cancellableThreads.execute(() -> recoveryTarget.receiveFileInfo(
                    phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, translogOps.get()));
                // How many bytes we've copied since we last called RateLimiter.pause
                final Function<StoreFileMetaData, OutputStream> outputStreamFactories =
                        md -> new BufferedOutputStream(new RecoveryOutputStream(md, translogOps), chunkSizeInBytes);
                sendFiles(store, phase1Files.toArray(new StoreFileMetaData[phase1Files.size()]), outputStreamFactories);
                // Send the CLEAN_FILES request, which takes all of the files that
                // were transferred and renames them from their temporary file
                // names to the actual file names. It also writes checksums for
                // the files after they have been renamed.
                //
                // Once the files have been renamed, any other files that are not
                // related to this recovery (out of date segments, for example)
                // are deleted
                try {
                    cancellableThreads.executeIO(() ->
                        recoveryTarget.cleanFiles(translogOps.get(), recoverySourceMetadata));
                } catch (RemoteTransportException | IOException targetException) {
                    final IOException corruptIndexException;
                    // we realized that after the index was copied and we wanted to finalize the recovery
                    // the index was corrupted:
                    //   - maybe due to a broken segments file on an empty index (transferred with no checksum)
                    //   - maybe due to old segments without checksums or length only checks
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(targetException)) != null) {
                        try {
                            final Store.MetadataSnapshot recoverySourceMetadata1 = store.getMetadata(snapshot);
                            StoreFileMetaData[] metadata =
                                    StreamSupport.stream(recoverySourceMetadata1.spliterator(), false).toArray(StoreFileMetaData[]::new);
                            ArrayUtil.timSort(metadata, Comparator.comparingLong(StoreFileMetaData::length)); // check small files first
                            for (StoreFileMetaData md : metadata) {
                                cancellableThreads.checkForCancel();
                                logger.debug("checking integrity for file {} after remove corruption exception", md);
                                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                                    shard.failShard("recovery", corruptIndexException);
                                    logger.warn("Corrupted file detected {} checksum mismatch", md);
                                    throw corruptIndexException;
                                }
                            }
                        } catch (IOException ex) {
                            targetException.addSuppressed(ex);
                            throw targetException;
                        }
                        // corruption has happened on the way to replica
                        RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but " +
                                "checksums are ok", null);
                        exception.addSuppressed(targetException);
                        logger.warn(() -> new ParameterizedMessage(
                                "{} Remote file corruption during finalization of recovery on node {}. local checksum OK",
                                shard.shardId(), request.targetNode()), corruptIndexException);
                        throw exception;
                    } else {
                        throw targetException;
                    }
                }
            }
            final TimeValue took = stopWatch.totalTime();
            logger.trace("recovery [phase1]: took [{}]", took);
            return new SendFileResult(phase1FileNames, phase1FileSizes, totalSize, phase1ExistingFileNames,
                phase1ExistingFileSizes, existingTotalSize, took);
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }

    TimeValue prepareTargetForTranslog(final boolean fileBasedRecovery, final int totalTranslogOps) throws IOException {
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        // Send a request preparing the new shard's translog to receive operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        cancellableThreads.executeIO(() -> recoveryTarget.prepareForTranslogOperations(fileBasedRecovery, totalTranslogOps));
        stopWatch.stop();
        final TimeValue tookTime = stopWatch.totalTime();
        logger.trace("recovery [phase1]: remote engine start took [{}]", tookTime);
        return tookTime;
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
     * @return the send snapshot result
     */
    SendSnapshotResult phase2(long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo, Translog.Snapshot snapshot,
                              long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes) throws IOException {
        assert requiredSeqNoRangeStart <= endingSeqNo + 1:
            "requiredSeqNoRangeStart " + requiredSeqNoRangeStart + " is larger than endingSeqNo " + endingSeqNo;
        assert startingSeqNo <= requiredSeqNoRangeStart :
            "startingSeqNo " + startingSeqNo + " is larger than requiredSeqNoRangeStart " + requiredSeqNoRangeStart;
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }

        final StopWatch stopWatch = new StopWatch().start();

        logger.trace("recovery [phase2]: sending transaction log operations (seq# from [" +  startingSeqNo  + "], " +
            "required [" + requiredSeqNoRangeStart + ":" + endingSeqNo + "]");

        int ops = 0;
        long size = 0;
        int skippedOps = 0;
        int totalSentOps = 0;
        final AtomicLong targetLocalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final List<Translog.Operation> operations = new ArrayList<>();
        final LocalCheckpointTracker requiredOpsTracker = new LocalCheckpointTracker(endingSeqNo, requiredSeqNoRangeStart - 1);

        final int expectedTotalOps = snapshot.totalOperations();
        if (expectedTotalOps == 0) {
            logger.trace("no translog operations to send");
        }

        final CancellableThreads.IOInterruptible sendBatch = () -> {
            final long targetCheckpoint = recoveryTarget.indexTranslogOperations(
                operations, expectedTotalOps, maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes);
            targetLocalCheckpoint.set(targetCheckpoint);
        };

        // send operations in batches
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(request.shardId());
            }
            cancellableThreads.checkForCancel();

            final long seqNo = operation.seqNo();
            if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                skippedOps++;
                continue;
            }
            operations.add(operation);
            ops++;
            size += operation.estimateSize();
            totalSentOps++;
            requiredOpsTracker.markSeqNoAsCompleted(seqNo);

            // check if this request is past bytes threshold, and if so, send it off
            if (size >= chunkSizeInBytes) {
                cancellableThreads.executeIO(sendBatch);
                logger.trace("sent batch of [{}][{}] (total: [{}]) translog operations", ops, new ByteSizeValue(size), expectedTotalOps);
                ops = 0;
                size = 0;
                operations.clear();
            }
        }

        if (!operations.isEmpty() || totalSentOps == 0) {
            // send the leftover operations or if no operations were sent, request the target to respond with its local checkpoint
            cancellableThreads.executeIO(sendBatch);
        }

        assert expectedTotalOps == snapshot.skippedOperations() + skippedOps + totalSentOps
            : String.format(Locale.ROOT, "expected total [%d], overridden [%d], skipped [%d], total sent [%d]",
            expectedTotalOps, snapshot.skippedOperations(), skippedOps, totalSentOps);

        if (requiredOpsTracker.getCheckpoint() < endingSeqNo) {
            throw new IllegalStateException("translog replay failed to cover required sequence numbers" +
                " (required range [" + requiredSeqNoRangeStart + ":" + endingSeqNo + "). first missing op is ["
                + (requiredOpsTracker.getCheckpoint() + 1) + "]");
        }

        logger.trace("sent final batch of [{}][{}] (total: [{}]) translog operations", ops, new ByteSizeValue(size), expectedTotalOps);

        stopWatch.stop();
        final TimeValue tookTime = stopWatch.totalTime();
        logger.trace("recovery [phase2]: took [{}]", tookTime);
        return new SendSnapshotResult(targetLocalCheckpoint.get(), totalSentOps, tookTime);
    }

    /*
     * finalizes the recovery process
     */
    public void finalizeRecovery(final long targetLocalCheckpoint) throws IOException {
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
        final long globalCheckpoint = shard.getGlobalCheckpoint();
        cancellableThreads.executeIO(() -> recoveryTarget.finalizeRecovery(globalCheckpoint));
        runUnderPrimaryPermit(() -> shard.updateGlobalCheckpointForShard(request.targetAllocationId(), globalCheckpoint),
            shardId + " updating " + request.targetAllocationId() + "'s global checkpoint", shard, cancellableThreads, logger);

        if (request.isPrimaryRelocation()) {
            logger.trace("performing relocation hand-off");
            // this acquires all IndexShard operation permits and will thus delay new recoveries until it is done
            cancellableThreads.execute(() -> shard.relocated(recoveryTarget::handoffPrimaryContext));
            /*
             * if the recovery process fails after disabling primary mode on the source shard, both relocation source and
             * target are failed (see {@link IndexShard#updateRoutingEntry}).
             */
        }
        stopWatch.stop();
        logger.trace("finalizing recovery took [{}]", stopWatch.totalTime());
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


    final class RecoveryOutputStream extends OutputStream {
        private final StoreFileMetaData md;
        private final Supplier<Integer> translogOps;
        private long position = 0;

        RecoveryOutputStream(StoreFileMetaData md, Supplier<Integer> translogOps) {
            this.md = md;
            this.translogOps = translogOps;
        }

        @Override
        public void write(int b) throws IOException {
            throw new UnsupportedOperationException("we can't send single bytes over the wire");
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            sendNextChunk(position, new BytesArray(b, offset, length), md.length() == position + length);
            position += length;
            assert md.length() >= position : "length: " + md.length() + " but positions was: " + position;
        }

        private void sendNextChunk(long position, BytesArray content, boolean lastChunk) throws IOException {
            // Actually send the file chunk to the target node, waiting for it to complete
            cancellableThreads.executeIO(() ->
                    recoveryTarget.writeFileChunk(md, position, content, lastChunk, translogOps.get())
            );
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                throw new IndexShardClosedException(request.shardId());
            }
        }
    }

    void sendFiles(Store store, StoreFileMetaData[] files, Function<StoreFileMetaData, OutputStream> outputStreamFactory) throws Exception {
        store.incRef();
        try {
            ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetaData::length)); // send smallest first
            for (int i = 0; i < files.length; i++) {
                final StoreFileMetaData md = files[i];
                try (IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE)) {
                    // it's fine that we are only having the indexInput in the try/with block. The copy methods handles
                    // exceptions during close correctly and doesn't hide the original exception.
                    Streams.copy(new InputStreamIndexInput(indexInput, md.length()), outputStreamFactory.apply(md));
                } catch (Exception e) {
                    final IOException corruptIndexException;
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                        if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                            logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                            failEngine(corruptIndexException);
                            throw corruptIndexException;
                        } else { // corruption has happened on the way to replica
                            RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but " +
                                    "checksums are ok", null);
                            exception.addSuppressed(e);
                            logger.warn(() -> new ParameterizedMessage(
                                    "{} Remote file corruption on node {}, recovering {}. local checksum OK",
                                    shardId, request.targetNode(), md), corruptIndexException);
                            throw exception;
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            store.decRef();
        }
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
