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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.engine.RecoveryEngineException;
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

    protected final ESLogger logger;
    // Shard that is going to be recovered (the "source")
    private final IndexShard shard;
    private final String indexName;
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
                                 final ESLogger logger) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.request = request;
        this.currentClusterStateVersionSupplier = currentClusterStateVersionSupplier;
        this.delayNewRecoveries = delayNewRecoveries;
        this.logger = logger;
        this.indexName = this.request.shardId().getIndex().getName();
        this.shardId = this.request.shardId().id();
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        this.response = new RecoveryResponse();
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public RecoveryResponse recoverToTarget() throws IOException {
        try (Translog.View translogView = shard.acquireTranslogView()) {
            logger.trace("captured translog id [{}] for recovery", translogView.minTranslogGeneration());
            final IndexCommit phase1Snapshot;
            try {
                phase1Snapshot = shard.acquireIndexCommit(false);
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(translogView);
                throw new RecoveryEngineException(shard.shardId(), 1, "Snapshot failed", e);
            }

            try {
                phase1(phase1Snapshot, translogView);
            } catch (Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e);
            } finally {
                try {
                    shard.releaseIndexCommit(phase1Snapshot);
                } catch (IOException ex) {
                    logger.warn("releasing snapshot caused exception", ex);
                }
            }

            // engine was just started at the end of phase 1
            if (shard.state() == IndexShardState.RELOCATED) {
                /**
                 * The primary shard has been relocated while we copied files. This means that we can't guarantee any more that all
                 * operations that were replicated during the file copy (when the target engine was not yet opened) will be present in the
                 * local translog and thus will be resent on phase 2. The reason is that an operation replicated by the target primary is
                 * sent to the recovery target and the local shard (old primary) concurrently, meaning it may have arrived at the recovery
                 * target before we opened the engine and is still in-flight on the local shard.
                 *
                 * Checking the relocated status here, after we opened the engine on the target, is safe because primary relocation waits
                 * for all ongoing operations to complete and be fully replicated. Therefore all future operation by the new primary are
                 * guaranteed to reach the target shard when it's engine is open.
                 */
                throw new IndexShardRelocatedException(request.shardId());
            }

            logger.trace("{} snapshot translog for recovery. current size is [{}]", shard.shardId(), translogView.totalOperations());
            try {
                phase2(translogView.snapshot());
            } catch (Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery();
        }
        return response;
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
                            "of docs differ: " + numDocsTarget + " (" + request.sourceNode().getName() + ", primary) vs " + numDocsSource
                            + "(" + request.targetNode().getName() + ")");
                }
                // we shortcut recovery here because we have nothing to copy. but we must still start the engine on the target.
                // so we don't return here
                logger.trace("[{}][{}] skipping [phase1] to {} - identical sync id [{}] found on both source and target", indexName,
                        shardId,
                        request.targetNode(), recoverySourceSyncId);
            } else {
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetaData md : diff.identical) {
                    response.phase1ExistingFileNames.add(md.name());
                    response.phase1ExistingFileSizes.add(md.length());
                    existingTotalSize += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}][{}] recovery [phase1] to {}: not recovering [{}], exists in local store and has checksum [{}]," +
                                        " size [{}]",
                                indexName, shardId, request.targetNode(), md.name(), md.checksum(), md.length());
                    }
                    totalSize += md.length();
                }
                List<StoreFileMetaData> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
                phase1Files.addAll(diff.different);
                phase1Files.addAll(diff.missing);
                for (StoreFileMetaData md : phase1Files) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote " +
                                        "[{}], local [{}]",
                                indexName, shardId, request.targetNode(), md.name(), request.metadataSnapshot().asMap().get(md.name()), md);
                    } else {
                        logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], does not exists in remote",
                                indexName, shardId, request.targetNode(), md.name());
                    }
                    response.phase1FileNames.add(md.name());
                    response.phase1FileSizes.add(md.length());
                    totalSize += md.length();
                }

                response.phase1TotalSize = totalSize;
                response.phase1ExistingTotalSize = existingTotalSize;

                logger.trace("[{}][{}] recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with " +
                                "total_size [{}]",
                        indexName, shardId, request.targetNode(), response.phase1FileNames.size(),
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
                                logger.debug("{} checking integrity for file {} after remove corruption exception", shard.shardId(), md);
                                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                                    shard.failShard("recovery", corruptIndexException);
                                    logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
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
                        logger.warn("{} Remote file corruption during finalization of recovery on node {}. local checksum OK",
                                corruptIndexException, shard.shardId(), request.targetNode());
                        throw exception;
                    } else {
                        throw targetException;
                    }
                }
            }

            prepareTargetForTranslog(translogView.totalOperations());

            logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", indexName, shardId, request.targetNode(), stopWatch.totalTime());
            response.phase1Time = stopWatch.totalTime().millis();
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }


    protected void prepareTargetForTranslog(final int totalTranslogOps) throws IOException {
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("{} recovery [phase1] to {}: prepare remote engine for translog", request.shardId(), request.targetNode());
        final long startEngineStart = stopWatch.totalTime().millis();
        // Send a request preparing the new shard's translog to receive
        // operations. This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes
        cancellableThreads.executeIO(() -> recoveryTarget.prepareForTranslogOperations(totalTranslogOps));
        stopWatch.stop();

        response.startTime = stopWatch.totalTime().millis() - startEngineStart;
        logger.trace("{} recovery [phase1] to {}: remote engine start took [{}]",
                request.shardId(), request.targetNode(), stopWatch.totalTime());
    }

    /**
     * Perform phase2 of the recovery process
     * <p>
     * Phase2 takes a snapshot of the current translog *without* acquiring the
     * write lock (however, the translog snapshot is a point-in-time view of
     * the translog). It then sends each translog operation to the target node
     * so it can be replayed into the new shard.
     */
    public void phase2(Translog.Snapshot snapshot) {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();

        StopWatch stopWatch = new StopWatch().start();

        logger.trace("{} recovery [phase2] to {}: sending transaction log operations", request.shardId(), request.targetNode());
        // Send all the snapshot's translog operations to the target
        int totalOperations = sendSnapshot(snapshot);
        stopWatch.stop();
        logger.trace("{} recovery [phase2] to {}: took [{}]", request.shardId(), request.targetNode(), stopWatch.totalTime());
        response.phase2Time = stopWatch.totalTime().millis();
        response.phase2Operations = totalOperations;
    }

    /**
     * finalizes the recovery process
     */
    public void finalizeRecovery() {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("[{}][{}] finalizing recovery to {}", indexName, shardId, request.targetNode());
        cancellableThreads.execute(recoveryTarget::finalizeRecovery);

        if (isPrimaryRelocation()) {
            // in case of primary relocation we have to ensure that the cluster state on the primary relocation target has all
            // replica shards that have recovered or are still recovering from the current primary, otherwise replication actions
            // will not be send to these replicas. To accomplish this, first block new recoveries, then take version of latest cluster
            // state. This means that no new recovery can be completed based on information of a newer cluster state than the current one.
            try (Releasable ignored = delayNewRecoveries.apply("primary relocation hand-off in progress or completed for " + shardId)) {
                final long currentClusterStateVersion = currentClusterStateVersionSupplier.get();
                logger.trace("[{}][{}] waiting on {} to have cluster state with version [{}]", indexName, shardId, request.targetNode(),
                    currentClusterStateVersion);
                cancellableThreads.execute(() -> recoveryTarget.ensureClusterStateVersion(currentClusterStateVersion));

                logger.trace("[{}][{}] performing relocation hand-off to {}", indexName, shardId, request.targetNode());
                cancellableThreads.execute(() -> shard.relocated("to " + request.targetNode()));
            }
            /**
             * if the recovery process fails after setting the shard state to RELOCATED, both relocation source and
             * target are failed (see {@link IndexShard#updateRoutingEntry}).
             */
        }
        stopWatch.stop();
        logger.trace("[{}][{}] finalizing recovery to {}: took [{}]",
                indexName, shardId, request.targetNode(), stopWatch.totalTime());
    }

    protected boolean isPrimaryRelocation() {
        return request.recoveryType() == RecoveryState.Type.PRIMARY_RELOCATION;
    }

    /**
     * Send the given snapshot's operations to this handler's target node.
     * <p>
     * Operations are bulked into a single request depending on an operation
     * count limit or size-in-bytes limit
     *
     * @return the total number of translog operations that were sent
     */
    protected int sendSnapshot(final Translog.Snapshot snapshot) {
        int ops = 0;
        long size = 0;
        int totalOperations = 0;
        final List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation operation;
        try {
            operation = snapshot.next(); // this ex should bubble up
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to get next operation from translog", ex);
        }

        if (operation == null) {
            logger.trace("[{}][{}] no translog operations to send to {}",
                    indexName, shardId, request.targetNode());
        }
        while (operation != null) {
            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(request.shardId());
            }
            cancellableThreads.checkForCancel();
            operations.add(operation);
            ops += 1;
            size += operation.estimateSize();
            totalOperations++;

            // Check if this request is past bytes threshold, and
            // if so, send it off
            if (size >= chunkSizeInBytes) {

                // don't throttle translog, since we lock for phase3 indexing,
                // so we need to move it as fast as possible. Note, since we
                // index docs to replicas while the index files are recovered
                // the lock can potentially be removed, in which case, it might
                // make sense to re-enable throttling in this phase
                cancellableThreads.execute(() -> recoveryTarget.indexTranslogOperations(operations, snapshot.totalOperations()));
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}][{}] sent batch of [{}][{}] (total: [{}]) translog operations to {}",
                            indexName, shardId, ops, new ByteSizeValue(size),
                            snapshot.totalOperations(),
                            request.targetNode());
                }

                ops = 0;
                size = 0;
                operations.clear();
            }
            try {
                operation = snapshot.next(); // this ex should bubble up
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to get next operation from translog", ex);
            }
        }
        // send the leftover
        if (!operations.isEmpty()) {
            cancellableThreads.execute(() -> recoveryTarget.indexTranslogOperations(operations, snapshot.totalOperations()));

        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}][{}] sent final batch of [{}][{}] (total: [{}]) translog operations to {}",
                    indexName, shardId, ops, new ByteSizeValue(size),
                    snapshot.totalOperations(),
                    request.targetNode());
        }
        return totalOperations;
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
                try (final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE)) {
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
                            logger.warn("{} Remote file corruption on node {}, recovering {}. local checksum OK",
                                    corruptIndexException, shardId, request.targetNode(), md);
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
