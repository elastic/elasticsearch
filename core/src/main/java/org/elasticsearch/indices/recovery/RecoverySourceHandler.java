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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
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
    private final Supplier<Long> currentClusterStateVersionSupplier;
    private final Function<String, Releasable> delayNewRecoveries;
    private final int chunkSizeInBytes;
    private final RecoveryTargetHandler recoveryTarget;

    protected final RecoveryResponse response;

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
                                 final Supplier<Long> currentClusterStateVersionSupplier,
                                 Function<String, Releasable> delayNewRecoveries,
                                 final int fileChunkSizeInBytes,
                                 final Settings nodeSettings) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.request = request;
        this.currentClusterStateVersionSupplier = currentClusterStateVersionSupplier;
        this.delayNewRecoveries = delayNewRecoveries;
        this.shardId = this.request.shardId().id();
        this.logger = Loggers.getLogger(getClass(), nodeSettings, request.shardId(), "recover to " + request.targetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        this.response = new RecoveryResponse();
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public RecoveryResponse recoverToTarget() throws IOException {
        try (Translog.View translogView = shard.acquireTranslogView()) {
            logger.trace("captured translog id [{}] for recovery", translogView.minTranslogGeneration());

            boolean isSequenceNumberBasedRecoveryPossible = request.startingSeqNo() != SequenceNumbersService.UNASSIGNED_SEQ_NO &&
                isTranslogReadyForSequenceNumberBasedRecovery(translogView);

            if (isSequenceNumberBasedRecoveryPossible) {
                logger.trace("performing sequence numbers based recovery. starting at [{}]", request.startingSeqNo());
            } else {
                final Engine.IndexCommitRef phase1Snapshot;
                try {
                    phase1Snapshot = shard.acquireIndexCommit(false);
                } catch (final Exception e) {
                    IOUtils.closeWhileHandlingException(translogView);
                    throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
                }
                try {
                    phase1(phase1Snapshot.getIndexCommit(), translogView);
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

            try {
                prepareTargetForTranslog(translogView.totalOperations());
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "prepare target for translog failed", e);
            }

            // engine was just started at the end of phase1
            if (shard.state() == IndexShardState.RELOCATED) {
                assert request.isPrimaryRelocation() == false :
                    "recovery target should not retry primary relocation if previous attempt made it past finalization step";
                /*
                 * The primary shard has been relocated while we copied files. This means that we can't guarantee any more that all
                 * operations that were replicated during the file copy (when the target engine was not yet opened) will be present in the
                 * local translog and thus will be resent on phase2. The reason is that an operation replicated by the target primary is
                 * sent to the recovery target and the local shard (old primary) concurrently, meaning it may have arrived at the recovery
                 * target before we opened the engine and is still in-flight on the local shard.
                 *
                 * Checking the relocated status here, after we opened the engine on the target, is safe because primary relocation waits
                 * for all ongoing operations to complete and be fully replicated. Therefore all future operation by the new primary are
                 * guaranteed to reach the target shard when its engine is open.
                 */
                throw new IndexShardRelocatedException(request.shardId());
            }

            logger.trace("snapshot translog for recovery; current size is [{}]", translogView.totalOperations());
            final long targetLocalCheckpoint;
            try {
                final long startingSeqNo =
                        isSequenceNumberBasedRecoveryPossible ? request.startingSeqNo() : SequenceNumbersService.UNASSIGNED_SEQ_NO;
                targetLocalCheckpoint = phase2(startingSeqNo, translogView.snapshot());
            } catch (Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery(targetLocalCheckpoint);
        }
        return response;
    }

    /**
     * Determines if the source translog is ready for a sequence-number-based peer recovery. The main condition here is that the source
     * translog contains all operations between the local checkpoint on the target and the current maximum sequence number on the source.
     *
     * @param translogView a view of the translog on the source
     * @return {@code true} if the source is ready for a sequence-number-based recovery
     * @throws IOException if an I/O exception occurred reading the translog snapshot
     */
    boolean isTranslogReadyForSequenceNumberBasedRecovery(final Translog.View translogView) throws IOException {
        final long startingSeqNo = request.startingSeqNo();
        assert startingSeqNo >= 0;
        final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
        logger.trace("testing sequence numbers in range: [{}, {}]", startingSeqNo, endingSeqNo);
        // the start recovery request is initialized with the starting sequence number set to the target shard's local checkpoint plus one
        if (startingSeqNo - 1 <= endingSeqNo) {
            /*
             * We need to wait for all operations up to the current max to complete, otherwise we can not guarantee that all
             * operations in the required range will be available for replaying from the translog of the source.
             */
            cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));

            logger.trace("all operations up to [{}] completed, checking translog content", endingSeqNo);

            final LocalCheckpointTracker tracker = new LocalCheckpointTracker(shard.indexSettings(), startingSeqNo, startingSeqNo - 1);
            final Translog.Snapshot snapshot = translogView.snapshot();
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (operation.seqNo() != SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                    tracker.markSeqNoAsCompleted(operation.seqNo());
                }
            }
            return tracker.getCheckpoint() >= endingSeqNo;
        } else {
            // norelease this can currently happen if a snapshot restore rolls the primary back to a previous commit point; in this
            // situation the local checkpoint on the replica can be far in advance of the maximum sequence number on the primary violating
            // all assumptions regarding local and global checkpoints
            return false;
        }
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
    public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
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
                    response.phase1ExistingFileNames.add(md.name());
                    response.phase1ExistingFileSizes.add(md.length());
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
                    response.phase1FileNames.add(md.name());
                    response.phase1FileSizes.add(md.length());
                    totalSize += md.length();
                }

                response.phase1TotalSize = totalSize;
                response.phase1ExistingTotalSize = existingTotalSize;

                logger.trace("recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                        response.phase1FileNames.size(),
                        new ByteSizeValue(totalSize), response.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));
                cancellableThreads.execute(() ->
                        recoveryTarget.receiveFileInfo(response.phase1FileNames, response.phase1FileSizes, response.phase1ExistingFileNames,
                                response.phase1ExistingFileSizes, translogView.totalOperations()));
                // How many bytes we've copied since we last called RateLimiter.pause
                final Function<StoreFileMetaData, OutputStream> outputStreamFactories =
                        md -> new BufferedOutputStream(new RecoveryOutputStream(md, translogView), chunkSizeInBytes);
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
                    cancellableThreads.executeIO(() -> recoveryTarget.cleanFiles(translogView.totalOperations(), recoverySourceMetadata));
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
                                    StreamSupport.stream(recoverySourceMetadata1.spliterator(), false).toArray(size -> new
                                            StoreFileMetaData[size]);
                            ArrayUtil.timSort(metadata, (o1, o2) -> {
                                return Long.compare(o1.length(), o2.length()); // check small files first
                            });
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
                        logger.warn(
                            (org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                                "{} Remote file corruption during finalization of recovery on node {}. local checksum OK",
                                shard.shardId(),
                                request.targetNode()),
                            corruptIndexException);
                        throw exception;
                    } else {
                        throw targetException;
                    }
                }
            }

            logger.trace("recovery [phase1]: took [{}]", stopWatch.totalTime());
            response.phase1Time = stopWatch.totalTime().millis();
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }

    void prepareTargetForTranslog(final int totalTranslogOps) throws IOException {
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        final long startEngineStart = stopWatch.totalTime().millis();
        // Send a request preparing the new shard's translog to receive operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        cancellableThreads.executeIO(() -> recoveryTarget.prepareForTranslogOperations(totalTranslogOps));
        stopWatch.stop();

        response.startTime = stopWatch.totalTime().millis() - startEngineStart;
        logger.trace("recovery [phase1]: remote engine start took [{}]", stopWatch.totalTime());
    }

    /**
     * Perform phase two of the recovery process.
     * <p>
     * Phase two uses a snapshot of the current translog *without* acquiring the write lock (however, the translog snapshot is
     * point-in-time view of the translog). It then sends each translog operation to the target node so it can be replayed into the new
     * shard.
     *
     * @param startingSeqNo the sequence number to start recovery from, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO} if all
     *                      ops should be sent
     * @param snapshot      a snapshot of the translog
     *
     * @return the local checkpoint on the target
     */
    long phase2(final long startingSeqNo, final Translog.Snapshot snapshot) throws IOException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();

        final StopWatch stopWatch = new StopWatch().start();

        logger.trace("recovery [phase2]: sending transaction log operations");

        // send all the snapshot's translog operations to the target
        final SendSnapshotResult result = sendSnapshot(startingSeqNo, snapshot);

        stopWatch.stop();
        logger.trace("recovery [phase2]: took [{}]", stopWatch.totalTime());
        response.phase2Time = stopWatch.totalTime().millis();
        response.phase2Operations = result.totalOperations;
        return result.targetLocalCheckpoint;
    }

    /*
     * finalizes the recovery process
     */
    public void finalizeRecovery(final long targetLocalCheckpoint) {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("finalizing recovery");
        cancellableThreads.execute(() -> {
            shard.markAllocationIdAsInSync(request.targetAllocationId(), targetLocalCheckpoint);
            recoveryTarget.finalizeRecovery(shard.getGlobalCheckpoint());
        });

        if (request.isPrimaryRelocation()) {
            // in case of primary relocation we have to ensure that the cluster state on the primary relocation target has all
            // replica shards that have recovered or are still recovering from the current primary, otherwise replication actions
            // will not be send to these replicas. To accomplish this, first block new recoveries, then take version of latest cluster
            // state. This means that no new recovery can be completed based on information of a newer cluster state than the current one.
            try (Releasable ignored = delayNewRecoveries.apply("primary relocation hand-off in progress or completed for " + shardId)) {
                final long currentClusterStateVersion = currentClusterStateVersionSupplier.get();
                logger.trace("waiting on remote node to have cluster state with version [{}]", currentClusterStateVersion);
                cancellableThreads.execute(() -> recoveryTarget.ensureClusterStateVersion(currentClusterStateVersion));

                logger.trace("performing relocation hand-off");
                cancellableThreads.execute(() -> shard.relocated("to " + request.targetNode()));
            }
            /*
             * if the recovery process fails after setting the shard state to RELOCATED, both relocation source and
             * target are failed (see {@link IndexShard#updateRoutingEntry}).
             */
        }
        stopWatch.stop();
        logger.trace("finalizing recovery took [{}]", stopWatch.totalTime());
    }

    static class SendSnapshotResult {

        final long targetLocalCheckpoint;
        final int totalOperations;

        SendSnapshotResult(final long targetLocalCheckpoint, final int totalOperations) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.totalOperations = totalOperations;
        }

    }

    /**
     * Send the given snapshot's operations with a sequence number greater than the specified staring sequence number to this handler's
     * target node.
     * <p>
     * Operations are bulked into a single request depending on an operation count limit or size-in-bytes limit.
     *
     * @param startingSeqNo the sequence number for which only operations with a sequence number greater than this will be sent
     * @param snapshot      the translog snapshot to replay operations from
     * @return the local checkpoint on the target and the total number of operations sent
     * @throws IOException if an I/O exception occurred reading the translog snapshot
     */
    protected SendSnapshotResult sendSnapshot(final long startingSeqNo, final Translog.Snapshot snapshot) throws IOException {
        int ops = 0;
        long size = 0;
        int skippedOps = 0;
        int totalSentOps = 0;
        final AtomicLong targetLocalCheckpoint = new AtomicLong(SequenceNumbersService.UNASSIGNED_SEQ_NO);
        final List<Translog.Operation> operations = new ArrayList<>();

        final int expectedTotalOps = snapshot.totalOperations();
        if (expectedTotalOps == 0) {
            logger.trace("no translog operations to send");
        }

        final CancellableThreads.Interruptable sendBatch =
                () -> targetLocalCheckpoint.set(recoveryTarget.indexTranslogOperations(operations, expectedTotalOps));

        // send operations in batches
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(request.shardId());
            }
            cancellableThreads.checkForCancel();
            /*
             * If we are doing a sequence-number-based recovery, we have to skip older ops for which no sequence number was assigned, and
             * any ops before the starting sequence number.
             */
            final long seqNo = operation.seqNo();
            if (startingSeqNo >= 0 && (seqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo)) {
                skippedOps++;
                continue;
            }
            operations.add(operation);
            ops++;
            size += operation.estimateSize();
            totalSentOps++;

            // check if this request is past bytes threshold, and if so, send it off
            if (size >= chunkSizeInBytes) {
                cancellableThreads.execute(sendBatch);
                logger.trace("sent batch of [{}][{}] (total: [{}]) translog operations", ops, new ByteSizeValue(size), expectedTotalOps);
                ops = 0;
                size = 0;
                operations.clear();
            }
        }

        if (!operations.isEmpty() || totalSentOps == 0) {
            // send the leftover operations or if no operations were sent, request the target to respond with its local checkpoint
            cancellableThreads.execute(sendBatch);
        }

        assert expectedTotalOps == skippedOps + totalSentOps
                : "expected total [" + expectedTotalOps + "], skipped [" + skippedOps + "], total sent [" + totalSentOps + "]";

        logger.trace("sent final batch of [{}][{}] (total: [{}]) translog operations", ops, new ByteSizeValue(size), expectedTotalOps);

        return new SendSnapshotResult(targetLocalCheckpoint.get(), totalSentOps);
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
        private final Translog.View translogView;
        private long position = 0;

        RecoveryOutputStream(StoreFileMetaData md, Translog.View translogView) {
            this.md = md;
            this.translogView = translogView;
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
                    recoveryTarget.writeFileChunk(md, position, content, lastChunk, translogView.totalOperations())
            );
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                throw new IndexShardClosedException(request.shardId());
            }
        }
    }

    void sendFiles(Store store, StoreFileMetaData[] files, Function<StoreFileMetaData, OutputStream> outputStreamFactory) throws Exception {
        store.incRef();
        try {
            ArrayUtil.timSort(files, (a, b) -> Long.compare(a.length(), b.length())); // send smallest first
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
                            logger.warn(
                                (org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                                    "{} Remote file corruption on node {}, recovering {}. local checksum OK",
                                    shardId,
                                    request.targetNode(),
                                    md),
                                corruptIndexException);
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
