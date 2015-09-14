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
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.CancellableThreads.Interruptable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 */
public class RecoverySourceHandler {

    protected final ESLogger logger;
    // Shard that is going to be recovered (the "source")
    private final IndexShard shard;
    private final String indexName;
    private final int shardId;
    // Request containing source and target node information
    private final StartRecoveryRequest request;
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;

    protected final RecoveryResponse response;
    private final CancellableThreads cancellableThreads = new CancellableThreads() {
        @Override
        protected void onCancel(String reason, @Nullable Throwable suppressedException) {
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


    public RecoverySourceHandler(final IndexShard shard, final StartRecoveryRequest request, final RecoverySettings recoverySettings,
                                 final TransportService transportService, final ESLogger logger) {
        this.shard = shard;
        this.request = request;
        this.recoverySettings = recoverySettings;
        this.logger = logger;
        this.transportService = transportService;
        this.indexName = this.request.shardId().index().name();
        this.shardId = this.request.shardId().id();

        this.response = new RecoveryResponse();
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public RecoveryResponse recoverToTarget() {
        final Engine engine = shard.engine();
        assert engine.getTranslog() != null : "translog must not be null";
        try (Translog.View translogView = engine.getTranslog().newView()) {
            logger.trace("captured translog id [{}] for recovery", translogView.minTranslogGeneration());
            final SnapshotIndexCommit phase1Snapshot;
            try {
                phase1Snapshot = shard.snapshotIndex(false);
            } catch (Throwable e) {
                IOUtils.closeWhileHandlingException(translogView);
                throw new RecoveryEngineException(shard.shardId(), 1, "Snapshot failed", e);
            }

            try {
                phase1(phase1Snapshot, translogView);
            } catch (Throwable e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e);
            } finally {
                Releasables.closeWhileHandlingException(phase1Snapshot);
            }

            logger.trace("snapshot translog for recovery. current size is [{}]", translogView.totalOperations());
            try (Translog.Snapshot phase2Snapshot = translogView.snapshot()) {
                phase2(phase2Snapshot);
            } catch (Throwable e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery();
        }
        return response;
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link SnapshotIndexCommit}
     * snapshot has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     * <p/>
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     */
    public void phase1(final SnapshotIndexCommit snapshot, final Translog.View translogView) {
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
                shard.engine().failEngine("recovery", ex);
                throw ex;
            }
            for (String name : snapshot.getFiles()) {
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
                    throw new IllegalStateException("try to recover " + request.shardId() + " from primary shard with sync id but number of docs differ: " + numDocsTarget + " (" + request.sourceNode().getName() + ", primary) vs " + numDocsSource + "(" + request.targetNode().getName() + ")");
                }
                // we shortcut recovery here because we have nothing to copy. but we must still start the engine on the target.
                // so we don't return here
                logger.trace("[{}][{}] skipping [phase1] to {} - identical sync id [{}] found on both source and target", indexName, shardId,
                        request.targetNode(), recoverySourceSyncId);
            } else {
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetaData md : diff.identical) {
                    response.phase1ExistingFileNames.add(md.name());
                    response.phase1ExistingFileSizes.add(md.length());
                    existingTotalSize += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}][{}] recovery [phase1] to {}: not recovering [{}], exists in local store and has checksum [{}], size [{}]",
                                indexName, shardId, request.targetNode(), md.name(), md.checksum(), md.length());
                    }
                    totalSize += md.length();
                }
                for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
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

                logger.trace("[{}][{}] recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                        indexName, shardId, request.targetNode(), response.phase1FileNames.size(),
                        new ByteSizeValue(totalSize), response.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));
                cancellableThreads.execute(new Interruptable() {
                    @Override
                    public void run() throws InterruptedException {
                        RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(request.recoveryId(), request.shardId(),
                                response.phase1FileNames, response.phase1FileSizes, response.phase1ExistingFileNames, response.phase1ExistingFileSizes,
                                translogView.totalOperations());
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILES_INFO, recoveryInfoFilesRequest,
                                TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()),
                                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                    }
                });

                // This latch will be used to wait until all files have been transferred to the target node
                final CountDownLatch latch = new CountDownLatch(response.phase1FileNames.size());
                final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
                final AtomicReference<Throwable> corruptedEngine = new AtomicReference<>();
                int fileIndex = 0;
                ThreadPoolExecutor pool;

                // How many bytes we've copied since we last called RateLimiter.pause
                final AtomicLong bytesSinceLastPause = new AtomicLong();

                for (final String name : response.phase1FileNames) {
                    long fileSize = response.phase1FileSizes.get(fileIndex);

                    // Files are split into two categories, files that are "small"
                    // (under 5mb) and other files. Small files are transferred
                    // using a separate thread pool dedicated to small files.
                    //
                    // The idea behind this is that while we are transferring an
                    // older, large index, a user may create a new index, but that
                    // index will not be able to recover until the large index
                    // finishes, by using two different thread pools we can allow
                    // tiny files (like segments for a brand new index) to be
                    // recovered while ongoing large segment recoveries are
                    // happening. It also allows these pools to be configured
                    // separately.
                    if (fileSize > RecoverySettings.SMALL_FILE_CUTOFF_BYTES) {
                        pool = recoverySettings.concurrentStreamPool();
                    } else {
                        pool = recoverySettings.concurrentSmallFileStreamPool();
                    }

                    pool.execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Throwable t) {
                            // we either got rejected or the store can't be incremented / we are canceled
                            logger.debug("Failed to transfer file [" + name + "] on recovery");
                        }

                        @Override
                        public void onAfter() {
                            // Signify this file has completed by decrementing the latch
                            latch.countDown();
                        }

                        @Override
                        protected void doRun() {
                            cancellableThreads.checkForCancel();
                            store.incRef();
                            final StoreFileMetaData md = recoverySourceMetadata.get(name);
                            try (final IndexInput indexInput = store.directory().openInput(name, IOContext.READONCE)) {
                                final int BUFFER_SIZE = (int) Math.max(1, recoverySettings.fileChunkSize().bytes()); // at least one!
                                final byte[] buf = new byte[BUFFER_SIZE];
                                boolean shouldCompressRequest = recoverySettings.compress();
                                if (CompressorFactory.isCompressed(indexInput)) {
                                    shouldCompressRequest = false;
                                }

                                final long len = indexInput.length();
                                long readCount = 0;
                                final TransportRequestOptions requestOptions = TransportRequestOptions.options()
                                        .withCompress(shouldCompressRequest)
                                        .withType(TransportRequestOptions.Type.RECOVERY)
                                        .withTimeout(recoverySettings.internalActionTimeout());

                                while (readCount < len) {
                                    if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                                        throw new IndexShardClosedException(shard.shardId());
                                    }
                                    int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                    final long position = indexInput.getFilePointer();

                                    // Pause using the rate limiter, if desired, to throttle the recovery
                                    RateLimiter rl = recoverySettings.rateLimiter();
                                    long throttleTimeInNanos = 0;
                                    if (rl != null) {
                                        long bytes = bytesSinceLastPause.addAndGet(toRead);
                                        if (bytes > rl.getMinPauseCheckBytes()) {
                                            // Time to pause
                                            bytesSinceLastPause.addAndGet(-bytes);
                                            throttleTimeInNanos = rl.pause(bytes);
                                            shard.recoveryStats().addThrottleTime(throttleTimeInNanos);
                                        }
                                    }
                                    indexInput.readBytes(buf, 0, toRead, false);
                                    final BytesArray content = new BytesArray(buf, 0, toRead);
                                    readCount += toRead;
                                    final boolean lastChunk = readCount == len;
                                    final RecoveryFileChunkRequest fileChunkRequest = new RecoveryFileChunkRequest(request.recoveryId(), request.shardId(), md, position,
                                            content, lastChunk, translogView.totalOperations(), throttleTimeInNanos);
                                    cancellableThreads.execute(new Interruptable() {
                                        @Override
                                        public void run() throws InterruptedException {
                                            // Actually send the file chunk to the target node, waiting for it to complete
                                            transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILE_CHUNK,
                                                    fileChunkRequest, requestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                                        }
                                    });

                                }
                            } catch (Throwable e) {
                                final Throwable corruptIndexException;
                                if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                                    if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                                        logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                                        if (corruptedEngine.compareAndSet(null, corruptIndexException) == false) {
                                            // if we are not the first exception, add ourselves as suppressed to the main one:
                                            corruptedEngine.get().addSuppressed(e);
                                        }
                                    } else { // corruption has happened on the way to replica
                                        RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but checksums are ok", null);
                                        exception.addSuppressed(e);
                                        exceptions.add(0, exception); // last exception first
                                        logger.warn("{} Remote file corruption on node {}, recovering {}. local checksum OK",
                                                corruptIndexException, shard.shardId(), request.targetNode(), md);

                                    }
                                } else {
                                    exceptions.add(0, e); // last exceptions first
                                }
                            } finally {
                                store.decRef();

                            }
                        }
                    });
                    fileIndex++;
                }

                cancellableThreads.execute(new Interruptable() {
                    @Override
                    public void run() throws InterruptedException {
                        // Wait for all files that need to be transferred to finish transferring
                        latch.await();
                    }
                });

                if (corruptedEngine.get() != null) {
                    shard.engine().failEngine("recovery", corruptedEngine.get());
                    throw corruptedEngine.get();
                } else {
                    ExceptionsHelper.rethrowAndSuppress(exceptions);
                }

                cancellableThreads.execute(new Interruptable() {
                    @Override
                    public void run() throws InterruptedException {
                        // Send the CLEAN_FILES request, which takes all of the files that
                        // were transferred and renames them from their temporary file
                        // names to the actual file names. It also writes checksums for
                        // the files after they have been renamed.
                        //
                        // Once the files have been renamed, any other files that are not
                        // related to this recovery (out of date segments, for example)
                        // are deleted
                        try {
                            transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.CLEAN_FILES,
                                    new RecoveryCleanFilesRequest(request.recoveryId(), shard.shardId(), recoverySourceMetadata, translogView.totalOperations()),
                                    TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()),
                                    EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                        } catch (RemoteTransportException remoteException) {
                            final IOException corruptIndexException;
                            // we realized that after the index was copied and we wanted to finalize the recovery
                            // the index was corrupted:
                            //   - maybe due to a broken segments file on an empty index (transferred with no checksum)
                            //   - maybe due to old segments without checksums or length only checks
                            if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(remoteException)) != null) {
                                try {
                                    final Store.MetadataSnapshot recoverySourceMetadata = store.getMetadata(snapshot);
                                    StoreFileMetaData[] metadata =
                                            StreamSupport.stream(recoverySourceMetadata.spliterator(), false).toArray(size -> new StoreFileMetaData[size]);
                                    ArrayUtil.timSort(metadata, new Comparator<StoreFileMetaData>() {
                                        @Override
                                        public int compare(StoreFileMetaData o1, StoreFileMetaData o2) {
                                            return Long.compare(o1.length(), o2.length()); // check small files first
                                        }
                                    });
                                    for (StoreFileMetaData md : metadata) {
                                        logger.debug("{} checking integrity for file {} after remove corruption exception", shard.shardId(), md);
                                        if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                                            shard.engine().failEngine("recovery", corruptIndexException);
                                            logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                                            throw corruptIndexException;
                                        }
                                    }
                                } catch (IOException ex) {
                                    remoteException.addSuppressed(ex);
                                    throw remoteException;
                                }
                                // corruption has happened on the way to replica
                                RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but checksums are ok", null);
                                exception.addSuppressed(remoteException);
                                logger.warn("{} Remote file corruption during finalization on node {}, recovering {}. local checksum OK",
                                        corruptIndexException, shard.shardId(), request.targetNode());
                                throw exception;
                            } else {
                                throw remoteException;
                            }
                        }
                    }
                });
            }

            prepareTargetForTranslog(translogView);

            logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", indexName, shardId, request.targetNode(), stopWatch.totalTime());
            response.phase1Time = stopWatch.totalTime().millis();
        } catch (Throwable e) {
            throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }

    protected void prepareTargetForTranslog(final Translog.View translogView) {
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("{} recovery [phase1] to {}: prepare remote engine for translog", request.shardId(), request.targetNode());
        final long startEngineStart = stopWatch.totalTime().millis();
        cancellableThreads.execute(new Interruptable() {
            @Override
            public void run() throws InterruptedException {
                // Send a request preparing the new shard's translog to receive
                // operations. This ensures the shard engine is started and disables
                // garbage collection (not the JVM's GC!) of tombstone deletes
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.PREPARE_TRANSLOG,
                        new RecoveryPrepareForTranslogOperationsRequest(request.recoveryId(), request.shardId(), translogView.totalOperations()),
                        TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
            }
        });

        stopWatch.stop();

        response.startTime = stopWatch.totalTime().millis() - startEngineStart;
        logger.trace("{} recovery [phase1] to {}: remote engine start took [{}]",
                request.shardId(), request.targetNode(), stopWatch.totalTime());
    }

    /**
     * Perform phase2 of the recovery process
     * <p/>
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


        cancellableThreads.execute(new Interruptable() {
            @Override
            public void run() throws InterruptedException {
                // Send the FINALIZE request to the target node. The finalize request
                // clears unreferenced translog files, refreshes the engine now that
                // new segments are available, and enables garbage collection of
                // tombstone files. The shard is also moved to the POST_RECOVERY phase
                // during this time
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FINALIZE,
                        new RecoveryFinalizeRecoveryRequest(request.recoveryId(), request.shardId()),
                        TransportRequestOptions.options().withTimeout(recoverySettings.internalActionLongTimeout()),
                        EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
            }
        });


        if (request.markAsRelocated()) {
            // TODO what happens if the recovery process fails afterwards, we need to mark this back to started
            try {
                shard.relocated("to " + request.targetNode());
            } catch (IllegalIndexShardStateException e) {
                // we can ignore this exception since, on the other node, when it moved to phase3
                // it will also send shard started, which might cause the index shard we work against
                // to move be closed by the time we get to the the relocated method
            }
        }
        stopWatch.stop();
        logger.trace("[{}][{}] finalizing recovery to {}: took [{}]",
                indexName, shardId, request.targetNode(), stopWatch.totalTime());
    }

    /**
     * Send the given snapshot's operations to this handler's target node.
     * <p/>
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

        final TransportRequestOptions recoveryOptions = TransportRequestOptions.options()
                .withCompress(recoverySettings.compress())
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout());

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

            // Check if this request is past the size or bytes threshold, and
            // if so, send it off
            if (ops >= recoverySettings.translogOps() || size >= recoverySettings.translogSize().bytes()) {

                // don't throttle translog, since we lock for phase3 indexing,
                // so we need to move it as fast as possible. Note, since we
                // index docs to replicas while the index files are recovered
                // the lock can potentially be removed, in which case, it might
                // make sense to re-enable throttling in this phase
//                if (recoverySettings.rateLimiter() != null) {
//                    recoverySettings.rateLimiter().pause(size);
//                }

                cancellableThreads.execute(new Interruptable() {
                    @Override
                    public void run() throws InterruptedException {
                        final RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(
                                request.recoveryId(), request.shardId(), operations, snapshot.estimatedTotalOperations());
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                                recoveryOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                    }
                });
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}][{}] sent batch of [{}][{}] (total: [{}]) translog operations to {}",
                            indexName, shardId, ops, new ByteSizeValue(size),
                            snapshot.estimatedTotalOperations(),
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
            cancellableThreads.execute(new Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(
                            request.recoveryId(), request.shardId(), operations, snapshot.estimatedTotalOperations());
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                            recoveryOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                }
            });

        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}][{}] sent final batch of [{}][{}] (total: [{}]) translog operations to {}",
                    indexName, shardId, ops, new ByteSizeValue(size),
                    snapshot.estimatedTotalOperations(),
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

}
