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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p/>
 * <p>Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 */
public class RecoveryTarget extends AbstractComponent {

    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    private final RecoveriesCollection onGoingRecoveries;

    @Inject
    public RecoveryTarget(Settings settings, ThreadPool threadPool, TransportService transportService,
                          IndicesLifecycle indicesLifecycle, RecoverySettings recoverySettings, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.onGoingRecoveries = new RecoveriesCollection(logger, threadPool);

        transportService.registerRequestHandler(Actions.FILES_INFO, RecoveryFilesInfoRequest::new, ThreadPool.Names.GENERIC, new FilesInfoRequestHandler());
        transportService.registerRequestHandler(Actions.FILE_CHUNK, RecoveryFileChunkRequest::new, ThreadPool.Names.GENERIC, new FileChunkTransportRequestHandler());
        transportService.registerRequestHandler(Actions.CLEAN_FILES, RecoveryCleanFilesRequest::new, ThreadPool.Names.GENERIC, new CleanFilesRequestHandler());
        transportService.registerRequestHandler(Actions.PREPARE_TRANSLOG, RecoveryPrepareForTranslogOperationsRequest::new, ThreadPool.Names.GENERIC, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.TRANSLOG_OPS, RecoveryTranslogOperationsRequest::new, ThreadPool.Names.GENERIC, new TranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.FINALIZE, RecoveryFinalizeRecoveryRequest::new, ThreadPool.Names.GENERIC, new FinalizeRecoveryRequestHandler());

        indicesLifecycle.addListener(new IndicesLifecycle.Listener() {
            @Override
            public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                               @IndexSettings Settings indexSettings) {
                if (indexShard != null) {
                    onGoingRecoveries.cancelRecoveriesForShard(shardId, "shard closed");
                }
            }
        });
    }

    /**
     * cancel all ongoing recoveries for the given shard, if their status match a predicate
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel recoveries
     * @param shouldCancel a predicate to check if a recovery should be cancelled or not. Null means cancel without an extra check.
     *                     note that the recovery state can change after this check, but before it is being cancelled via other
     *                     already issued outstanding references.
     * @return true if a recovery was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason, @Nullable Predicate<RecoveryStatus> shouldCancel) {
        return onGoingRecoveries.cancelRecoveriesForShard(shardId, reason, shouldCancel);
    }

    public void startRecovery(final IndexShard indexShard, final RecoveryState.Type recoveryType, final DiscoveryNode sourceNode, final RecoveryListener listener) {
        try {
            indexShard.recovering("from " + sourceNode, recoveryType, sourceNode);
        } catch (IllegalIndexShardStateException e) {
            // that's fine, since we might be called concurrently, just ignore this, we are already recovering
            logger.debug("{} ignore recovery. already in recovering process, {}", indexShard.shardId(), e.getMessage());
            return;
        }
        // create a new recovery status, and process...
        final long recoveryId = onGoingRecoveries.startRecovery(indexShard, sourceNode, listener, recoverySettings.activityTimeout());
        threadPool.generic().execute(new RecoveryRunner(recoveryId));

    }

    protected void retryRecovery(final RecoveryStatus recoveryStatus, final String reason, TimeValue retryAfter, final StartRecoveryRequest currentRequest) {
        logger.trace("will retrying recovery with id [{}] in [{}] (reason [{}])", recoveryStatus.recoveryId(), retryAfter, reason);
        try {
            recoveryStatus.resetRecovery();
        } catch (Throwable e) {
            onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(), new RecoveryFailedException(currentRequest, e), true);
        }
        threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(recoveryStatus.recoveryId()));
    }

    private void doRecovery(final RecoveryStatus recoveryStatus) {
        assert recoveryStatus.sourceNode() != null : "can't do a recovery without a source node";

        logger.trace("collecting local files for {}", recoveryStatus);
        Store.MetadataSnapshot metadataSnapshot = null;
        try {
            metadataSnapshot = recoveryStatus.store().getMetadataOrEmpty();
        } catch (IOException e) {
            logger.warn("error while listing local files, recover as if there are none", e);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (Exception e) {
            // this will be logged as warning later on...
            logger.trace("unexpected error while listing local files, failing recovery", e);
            onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(),
                    new RecoveryFailedException(recoveryStatus.state(), "failed to list local files", e), true);
            return;
        }
        final StartRecoveryRequest request = new StartRecoveryRequest(recoveryStatus.shardId(), recoveryStatus.sourceNode(), clusterService.localNode(),
                false, metadataSnapshot, recoveryStatus.state().getType(), recoveryStatus.recoveryId());

        final AtomicReference<RecoveryResponse> responseHolder = new AtomicReference<>();
        try {
            logger.trace("[{}][{}] starting recovery from {}", request.shardId().index().name(), request.shardId().id(), request.sourceNode());
            recoveryStatus.indexShard().prepareForIndexRecovery();
            recoveryStatus.CancellableThreads().execute(new CancellableThreads.Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    responseHolder.set(transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request, new FutureTransportResponseHandler<RecoveryResponse>() {
                        @Override
                        public RecoveryResponse newInstance() {
                            return new RecoveryResponse();
                        }
                    }).txGet());
                }
            });
            final RecoveryResponse recoveryResponse = responseHolder.get();
            assert responseHolder != null;
            final TimeValue recoveryTime = new TimeValue(recoveryStatus.state().getTimer().time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingRecoveries.markRecoveryAsDone(recoveryStatus.recoveryId());
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[').append(request.shardId().index().name()).append(']').append('[').append(request.shardId().id()).append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime).append("]\n");
                sb.append("   phase1: recovered_files [").append(recoveryResponse.phase1FileNames.size()).append("]").append(" with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1TotalSize)).append("]")
                        .append(", took [").append(timeValueMillis(recoveryResponse.phase1Time)).append("], throttling_wait [").append(timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime)).append(']')
                        .append("\n");
                sb.append("         : reusing_files   [").append(recoveryResponse.phase1ExistingFileNames.size()).append("] with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1ExistingTotalSize)).append("]\n");
                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                sb.append("         : recovered [").append(recoveryResponse.phase2Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryResponse.phase2Time)).append("]")
                        .append("\n");
                logger.trace(sb.toString());
            } else {
                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), recoveryStatus.sourceNode(), recoveryTime);
            }
        } catch (CancellableThreads.ExecutionCancelledException e) {
            logger.trace("recovery cancelled", e);
        } catch (Throwable e) {

            if (logger.isTraceEnabled()) {
                logger.trace("[{}][{}] Got exception on recovery", e, request.shardId().index().name(), request.shardId().id());
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IllegalIndexShardStateException || cause instanceof IndexNotFoundException || cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(recoveryStatus, "remote shard not ready", recoverySettings.retryDelayStateSync(), request);
                return;
            }

            if (cause instanceof DelayRecoveryException) {
                retryRecovery(recoveryStatus, cause.getMessage(), recoverySettings.retryDelayStateSync(), request);
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.debug("delaying recovery of {} for [{}] due to networking error [{}]", recoveryStatus.shardId(), recoverySettings.retryDelayNetwork(), cause.getMessage());
                retryRecovery(recoveryStatus, cause.getMessage(), recoverySettings.retryDelayNetwork(), request);
                return;
            }

            if (cause instanceof IndexShardClosedException) {
                onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(), new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(), new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(), new RecoveryFailedException(request, e), true);
        }
    }

    public interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure);
    }

    class PrepareForTranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps());
                recoveryStatus.indexShard().skipTranslogRecovery();
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class FinalizeRecoveryRequestHandler implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.indexShard().finalizeRecovery();
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class TranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger);
                final RecoveryStatus recoveryStatus = statusRef.status();
                final RecoveryState.Translog translog = recoveryStatus.state().getTranslog();
                translog.totalOperations(request.totalTranslogOps());
                assert recoveryStatus.indexShard().recoveryState() == recoveryStatus.state();
                try {
                    recoveryStatus.indexShard().performBatchRecovery(request.operations());
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (TranslogRecoveryPerformer.BatchOperationException exception) {
                    MapperException mapperException = (MapperException) ExceptionsHelper.unwrap(exception, MapperException.class);
                    if (mapperException == null) {
                        throw exception;
                    }
                    // in very rare cases a translog replay from primary is processed before a mapping update on this node
                    // which causes local mapping changes. we want to wait until these mappings are processed.
                    logger.trace("delaying recovery due to missing mapping changes (rolling back stats for [{}] ops)", exception, exception.completedOperations());
                    translog.decrementRecoveredOperations(exception.completedOperations());
                    // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                    // canceled)
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            try {
                                messageReceived(request, channel);
                            } catch (Exception e) {
                                onFailure(e);
                            }
                        }

                        protected void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                logger.warn("failed to send error back to recovery source", e1);
                            }
                        }

                        @Override
                        public void onClusterServiceClose() {
                            onFailure(new ElasticsearchException("cluster service was closed while waiting for mapping updates"));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            // note that we do not use a timeout (see comment above)
                            onFailure(new ElasticsearchTimeoutException("timed out waiting for mapping updates (timeout [" + timeout + "])"));
                        }
                    });
                }
            }
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                final RecoveryState.Index index = recoveryStatus.state().getIndex();
                for (int i = 0; i < request.phase1ExistingFileNames.size(); i++) {
                    index.addFileDetail(request.phase1ExistingFileNames.get(i), request.phase1ExistingFileSizes.get(i), true);
                }
                for (int i = 0; i < request.phase1FileNames.size(); i++) {
                    index.addFileDetail(request.phase1FileNames.get(i), request.phase1FileSizes.get(i), false);
                }
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps);
                recoveryStatus.state().getTranslog().totalOperationsOnStart(request.totalTranslogOps);
                // recoveryBytesCount / recoveryFileCount will be set as we go...
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps());
                // first, we go and move files that were created with the recovery id suffix to
                // the actual names, its ok if we have a corrupted index here, since we have replicas
                // to recover from in case of a full cluster shutdown just when this code executes...
                recoveryStatus.indexShard().deleteShardState(); // we have to delete it first since even if we fail to rename the shard might be invalid
                recoveryStatus.renameAllTempFiles();
                final Store store = recoveryStatus.store();
                // now write checksums
                recoveryStatus.legacyChecksums().write(store);
                Store.MetadataSnapshot sourceMetaData = request.sourceMetaSnapshot();
                try {
                    store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
                } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                    // this is a fatal exception at this stage.
                    // this means we transferred files from the remote that have not be checksummed and they are
                    // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                    // source shard since this index might be broken there as well? The Source can handle this and checks
                    // its content on disk if possible.
                    try {
                        try {
                            store.removeCorruptionMarker();
                        } finally {
                            Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                        }
                    } catch (Throwable e) {
                        logger.debug("Failed to clean lucene index", e);
                        ex.addSuppressed(e);
                    }
                    RecoveryFailedException rfe = new RecoveryFailedException(recoveryStatus.state(), "failed to clean after recovery", ex);
                    recoveryStatus.fail(rfe, true);
                    throw rfe;
                } catch (Exception ex) {
                    RecoveryFailedException rfe = new RecoveryFailedException(recoveryStatus.state(), "failed to clean after recovery", ex);
                    recoveryStatus.fail(rfe, true);
                    throw rfe;
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                final Store store = recoveryStatus.store();
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps());
                final RecoveryState.Index indexState = recoveryStatus.state().getIndex();
                if (request.sourceThrottleTimeInNanos() != RecoveryState.Index.UNKNOWN) {
                    indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
                }
                IndexOutput indexOutput;
                if (request.position() == 0) {
                    indexOutput = recoveryStatus.openAndPutIndexOutput(request.name(), request.metadata(), store);
                } else {
                    indexOutput = recoveryStatus.getOpenIndexOutput(request.name());
                }
                BytesReference content = request.content();
                if (!content.hasArray()) {
                    content = content.toBytesArray();
                }
                RateLimiter rl = recoverySettings.rateLimiter();
                if (rl != null) {
                    long bytes = bytesSinceLastPause.addAndGet(content.length());
                    if (bytes > rl.getMinPauseCheckBytes()) {
                        // Time to pause
                        bytesSinceLastPause.addAndGet(-bytes);
                        long throttleTimeInNanos = rl.pause(bytes);
                        indexState.addTargetThrottling(throttleTimeInNanos);
                        recoveryStatus.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                    }
                }
                indexOutput.writeBytes(content.array(), content.arrayOffset(), content.length());
                indexState.addRecoveredBytesToFile(request.name(), content.length());
                if (indexOutput.getFilePointer() >= request.length() || request.lastChunk()) {
                    try {
                        Store.verify(indexOutput);
                    } finally {
                        // we are done
                        indexOutput.close();
                    }
                    // write the checksum
                    recoveryStatus.legacyChecksums().add(request.metadata());
                    final String temporaryFileName = recoveryStatus.getTempNameForFile(request.name());
                    assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName);
                    store.directory().sync(Collections.singleton(temporaryFileName));
                    IndexOutput remove = recoveryStatus.removeOpenIndexOutputs(request.name());
                    assert remove == null || remove == indexOutput; // remove maybe null if we got finished
                }
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class RecoveryRunner extends AbstractRunnable {

        final long recoveryId;

        RecoveryRunner(long recoveryId) {
            this.recoveryId = recoveryId;
        }

        @Override
        public void onFailure(Throwable t) {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatus(recoveryId)) {
                if (statusRef != null) {
                    logger.error("unexpected error during recovery [{}], failing shard", t, recoveryId);
                    onGoingRecoveries.failRecovery(recoveryId,
                            new RecoveryFailedException(statusRef.status().state(), "unexpected error", t),
                            true // be safe
                    );
                } else {
                    logger.debug("unexpected error during recovery, but recovery id [{}] is finished", t, recoveryId);
                }
            }
        }

        @Override
        public void doRun() {
            RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatus(recoveryId);
            if (statusRef == null) {
                logger.trace("not running recovery with id [{}] - can't find it (probably finished)", recoveryId);
                return;
            }
            try {
                doRecovery(statusRef.status());
            } finally {
                statusRef.close();
            }
        }
    }

}
