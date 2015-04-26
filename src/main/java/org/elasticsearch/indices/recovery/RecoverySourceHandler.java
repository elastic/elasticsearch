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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.CancellableThreads.Interruptable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 */
public class RecoverySourceHandler implements Engine.RecoveryHandler {

    protected final ESLogger logger;
    // Shard that is going to be recovered (the "source")
    private final IndexShard shard;
    private final String indexName;
    private final int shardId;
    // Request containing source and target node information
    private final StartRecoveryRequest request;
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexService indexService;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final RecoveryResponse response;
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
                                 final TransportService transportService, final ClusterService clusterService,
                                 final IndicesService indicesService, final MappingUpdatedAction mappingUpdatedAction, final ESLogger logger) {
        this.shard = shard;
        this.request = request;
        this.recoverySettings = recoverySettings;
        this.logger = logger;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexName = this.request.shardId().index().name();
        this.shardId = this.request.shardId().id();
        this.indexService = indicesService.indexServiceSafe(indexName);
        this.mappingUpdatedAction = mappingUpdatedAction;

        this.response = new RecoveryResponse();
    }

    /**
     * @return the {@link RecoveryResponse} after the recovery has completed all three phases
     */
    public RecoveryResponse getResponse() {
        return this.response;
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link SnapshotIndexCommit}
     * snapshot has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     *
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     *
     * {@code InternalEngine#recover} is responsible for snapshotting the index
     * and releasing the snapshot once all 3 phases of recovery are complete
     */
    @Override
    public void phase1(final SnapshotIndexCommit snapshot) throws ElasticsearchException {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
        final Store store = shard.store();
        store.incRef();
        try {
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata = store.getMetadata(snapshot);
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
            final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(new Store.MetadataSnapshot(request.existingFiles()));
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
                if (request.existingFiles().containsKey(md.name())) {
                    logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            indexName, shardId, request.targetNode(), md.name(), request.existingFiles().get(md.name()), md);
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
                            shard.translog().estimatedNumberOfOperations());
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
                            final int BUFFER_SIZE = (int) recoverySettings.fileChunkSize().bytes();
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
                                        content, lastChunk, shard.translog().estimatedNumberOfOperations(), throttleTimeInNanos);
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
                                new RecoveryCleanFilesRequest(request.recoveryId(), shard.shardId(), recoverySourceMetadata, shard.translog().estimatedNumberOfOperations()),
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
                                StoreFileMetaData[] metadata = Iterables.toArray(recoverySourceMetadata, StoreFileMetaData.class);
                                ArrayUtil.timSort(metadata, new Comparator<StoreFileMetaData>() {
                                    @Override
                                    public int compare(StoreFileMetaData o1, StoreFileMetaData o2) {
                                        return Long.compare(o1.length(), o2.length()); // check small files first
                                    }
                                });
                                for (StoreFileMetaData md : metadata) {
                                    logger.debug("{} checking integrity for file {} after remove corruption exception", shard.shardId(), md);
                                    if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
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
                        } else {
                            throw remoteException;
                        }
                    }
                }
            });
            stopWatch.stop();
            logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", indexName, shardId, request.targetNode(), stopWatch.totalTime());
            response.phase1Time = stopWatch.totalTime().millis();
        } catch (Throwable e) {
            throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }

    /**
     * Perform phase2 of the recovery process
     *
     * Phase2 takes a snapshot of the current translog *without* acquiring the
     * write lock (however, the translog snapshot is a point-in-time view of
     * the translog). It then sends each translog operation to the target node
     * so it can be replayed into the new shard.
     *
     * {@code InternalEngine#recover} is responsible for taking the snapshot
     * of the translog and releasing it once all 3 phases of recovery are complete
     */
    @Override
    public void phase2(Translog.Snapshot snapshot) throws ElasticsearchException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        logger.trace("{} recovery [phase2] to {}: start", request.shardId(), request.targetNode());
        StopWatch stopWatch = new StopWatch().start();
        cancellableThreads.execute(new Interruptable() {
            @Override
            public void run() throws InterruptedException {
                // Send a request preparing the new shard's translog to receive
                // operations. This ensures the shard engine is started and disables
                // garbage collection (not the JVM's GC!) of tombstone deletes
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.PREPARE_TRANSLOG,
                        new RecoveryPrepareForTranslogOperationsRequest(request.recoveryId(), request.shardId(), shard.translog().estimatedNumberOfOperations()),
                        TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
            }
        });

        stopWatch.stop();
        response.startTime = stopWatch.totalTime().millis();
        logger.trace("{} recovery [phase2] to {}: start took [{}]",
                request.shardId(), request.targetNode(), stopWatch.totalTime());


        logger.trace("{} recovery [phase2] to {}: updating current mapping to master", request.shardId(), request.targetNode());
        // Ensure that the mappings are synced with the master node
        updateMappingOnMaster();

        logger.trace("{} recovery [phase2] to {}: sending transaction log operations", request.shardId(), request.targetNode());
        stopWatch = new StopWatch().start();
        // Send all the snapshot's translog operations to the target
        int totalOperations = sendSnapshot(snapshot);
        stopWatch.stop();
        logger.trace("{} recovery [phase2] to {}: took [{}]", request.shardId(), request.targetNode(), stopWatch.totalTime());
        response.phase2Time = stopWatch.totalTime().millis();
        response.phase2Operations = totalOperations;
    }

    /**
     * Perform phase 3 of the recovery process
     *
     * Phase3 again takes a snapshot of the translog, however this time the
     * snapshot is acquired under a write lock. The translog operations are
     * sent to the target node where they are replayed.
     *
     * {@code InternalEngine#recover} is responsible for taking the snapshot
     * of the translog, and after phase 3 completes the snapshots from all
     * three phases are released.
     */
    @Override
    public void phase3(Translog.Snapshot snapshot) throws ElasticsearchException {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        final int totalOperations;
        logger.trace("[{}][{}] recovery [phase3] to {}: sending transaction log operations", indexName, shardId, request.targetNode());

        // Send the translog operations to the target node
        totalOperations = sendSnapshot(snapshot);

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
        logger.trace("[{}][{}] recovery [phase3] to {}: took [{}]",
                indexName, shardId, request.targetNode(), stopWatch.totalTime());
        response.phase3Time = stopWatch.totalTime().millis();
        response.phase3Operations = totalOperations;
    }

    /**
     * Ensures that the mapping in the cluster state is the same as the mapping
     * in our mapper service. If the mapping is not in sync, sends a request
     * to update it in the cluster state and blocks until it has finished
     * being updated.
     */
    private void updateMappingOnMaster() {
        // we test that the cluster state is in sync with our in memory mapping stored by the mapperService
        // we have to do it under the "cluster state update" thread to make sure that one doesn't modify it
        // while we're checking
        final BlockingQueue<DocumentMapper> documentMappersToUpdate = ConcurrentCollections.newBlockingQueue();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> mappingCheckException = new AtomicReference<>();

        // we use immediate as this is a very light weight check and we don't wait to delay recovery
        clusterService.submitStateUpdateTask("recovery_mapping_check", Priority.IMMEDIATE, new MappingUpdateTask(clusterService, indexService, recoverySettings, latch, documentMappersToUpdate, mappingCheckException, this.cancellableThreads));
        cancellableThreads.execute(new Interruptable() {
            @Override
            public void run() throws InterruptedException {
                latch.await();
            }
        });
        if (mappingCheckException.get() != null) {
            logger.warn("error during mapping check, failing recovery", mappingCheckException.get());
            throw new ElasticsearchException("error during mapping check", mappingCheckException.get());
        }
        if (documentMappersToUpdate.isEmpty()) {
            return;
        }
        final CountDownLatch updatedOnMaster = new CountDownLatch(documentMappersToUpdate.size());
        MappingUpdatedAction.MappingUpdateListener listener = new MappingUpdatedAction.MappingUpdateListener() {
            @Override
            public void onMappingUpdate() {
                updatedOnMaster.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("{} recovery to {}: failed to update mapping on master", request.shardId(), request.targetNode(), t);
                updatedOnMaster.countDown();
            }
        };
        for (DocumentMapper documentMapper : documentMappersToUpdate) {
            mappingUpdatedAction.updateMappingOnMaster(indexService.index().getName(), documentMapper.type(), documentMapper.mapping(), recoverySettings.internalActionTimeout(), listener);
        }
        cancellableThreads.execute(new Interruptable() {
            @Override
            public void run() throws InterruptedException {
                try {
                    if (!updatedOnMaster.await(recoverySettings.internalActionTimeout().millis(), TimeUnit.MILLISECONDS)) {
                        logger.debug("[{}][{}] recovery [phase2] to {}: waiting on pending mapping update timed out. waited [{}]",
                                indexName, shardId, request.targetNode(), recoverySettings.internalActionTimeout());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("interrupted while waiting for mapping to update on master");
                }
            }
        });
    }

    /**
     * Send the given snapshot's operations to this handler's target node.
     *
     * Operations are bulked into a single request depending on an operation
     * count limit or size-in-bytes limit
     *
     * @return the total number of translog operations that were sent
     */
    protected int sendSnapshot(Translog.Snapshot snapshot) {
        int ops = 0;
        long size = 0;
        int totalOperations = 0;
        final List<Translog.Operation> operations = Lists.newArrayList();
        Translog.Operation operation;
        try {
             operation = snapshot.next(); // this ex should bubble up
        } catch (IOException ex){
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
                                request.recoveryId(), request.shardId(), operations, shard.translog().estimatedNumberOfOperations());
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                                recoveryOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                    }
                });
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}][{}] sent batch of [{}][{}] (total: [{}]) translog operations to {}",
                            indexName, shardId, ops, new ByteSizeValue(size),
                            shard.translog().estimatedNumberOfOperations(),
                            request.targetNode());
                }

                ops = 0;
                size = 0;
                operations.clear();
            }
            try {
                operation = snapshot.next(); // this ex should bubble up
            } catch (IOException ex){
                throw new ElasticsearchException("failed to get next operation from translog", ex);
            }        }
        // send the leftover
        if (!operations.isEmpty()) {
            cancellableThreads.execute(new Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(
                            request.recoveryId(), request.shardId(), operations, shard.translog().estimatedNumberOfOperations());
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                            recoveryOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                }
            });

        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}][{}] sent final batch of [{}][{}] (total: [{}]) translog operations to {}",
                    indexName, shardId, ops, new ByteSizeValue(size),
                    shard.translog().estimatedNumberOfOperations(),
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

    // this is a static class since we are holding an instance to the IndexShard
    // on ShardRecoveryHandler which can not be GCed if the recovery is canceled
    // but this task is still stuck in the queue. This can be problematic if the
    // queue piles up and recoveries fail and can lead to OOM or memory pressure if lots of shards
    // are created and removed.
    private static class MappingUpdateTask extends TimeoutClusterStateUpdateTask {
        private final CountDownLatch latch;
        private final BlockingQueue<DocumentMapper> documentMappersToUpdate;
        private final AtomicReference<Throwable> mappingCheckException;
        private final CancellableThreads cancellableThreads;
        private ClusterService clusterService;
        private IndexService indexService;
        private RecoverySettings recoverySettings;

        public MappingUpdateTask(ClusterService clusterService, IndexService indexService, RecoverySettings recoverySettings, CountDownLatch latch, BlockingQueue<DocumentMapper> documentMappersToUpdate, AtomicReference<Throwable> mappingCheckException, CancellableThreads cancellableThreads) {
            this.latch = latch;
            this.documentMappersToUpdate = documentMappersToUpdate;
            this.mappingCheckException = mappingCheckException;
            this.clusterService = clusterService;
            this.indexService = indexService;
            this.recoverySettings = recoverySettings;
            this.cancellableThreads = cancellableThreads;
        }

        @Override
        public boolean runOnlyOnMaster() {
            return false;
        }

        @Override
        public TimeValue timeout() {
            return recoverySettings.internalActionTimeout();
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            latch.countDown();
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            if (cancellableThreads.isCancelled() == false) { // no need to run this if recovery is canceled
                IndexMetaData indexMetaData = clusterService.state().metaData().getIndices().get(indexService.index().getName());
                ImmutableOpenMap<String, MappingMetaData> metaDataMappings = null;
                if (indexMetaData != null) {
                    metaDataMappings = indexMetaData.getMappings();
                }
                // default mapping should not be sent back, it can only be updated by put mapping API, and its
                // a full in place replace, we don't want to override a potential update coming into it
                for (DocumentMapper documentMapper : indexService.mapperService().docMappers(false)) {

                    MappingMetaData mappingMetaData = metaDataMappings == null ? null : metaDataMappings.get(documentMapper.type());
                    if (mappingMetaData == null || !documentMapper.refreshSource().equals(mappingMetaData.source())) {
                        // not on master yet in the right form
                        documentMappersToUpdate.add(documentMapper);
                    }
                }
            }
            return currentState;
        }

        @Override
        public void onFailure(String source, Throwable t) {
            mappingCheckException.set(t);
            latch.countDown();
        }
    }
}
