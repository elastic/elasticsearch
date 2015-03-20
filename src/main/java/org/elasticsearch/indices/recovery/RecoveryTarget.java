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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
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
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

        transportService.registerHandler(Actions.FILES_INFO, new FilesInfoRequestHandler());
        transportService.registerHandler(Actions.FILE_CHUNK, new FileChunkTransportRequestHandler());
        transportService.registerHandler(Actions.CLEAN_FILES, new CleanFilesRequestHandler());
        transportService.registerHandler(Actions.PREPARE_TRANSLOG, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.TRANSLOG_OPS, new TranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.FINALIZE, new FinalizeRecoveryRequestHandler());

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

    public RecoveryState recoveryState(IndexShard indexShard) {
        try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.findRecoveryByShard(indexShard)) {
            if (statusRef == null) {
                return null;
            }
            final RecoveryStatus recoveryStatus = statusRef.status();
            return recoveryStatus.state();
        } catch (Exception e) {
            // shouldn't really happen, but have to be here due to auto close
            throw new ElasticsearchException("error while getting recovery state", e);
        }
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
        } catch (IOException e) {
            onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(), new RecoveryFailedException(currentRequest, e), true);
        }
        threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(recoveryStatus.recoveryId()));
    }

    // pkd private for testing
    Map<String, StoreFileMetaData> existingFiles(DiscoveryNode sourceNode, Store store) throws IOException {
        final Version sourceNodeVersion = sourceNode.version();
        if (sourceNodeVersion.onOrAfter(Version.V_1_4_0)) {
                return store.getMetadataOrEmpty().asMap();
        } else {
            logger.debug("Force full recovery source node version {}", sourceNodeVersion);
            // force full recovery if we recover from nodes < 1.4.0
            return Collections.EMPTY_MAP;
        }
    }

    private void doRecovery(final RecoveryStatus recoveryStatus) {
        assert recoveryStatus.sourceNode() != null : "can't do a recovery without a source node";

        logger.trace("collecting local files for {}", recoveryStatus);
        final Map<String, StoreFileMetaData> existingFiles;
        try {
            existingFiles = existingFiles(recoveryStatus.sourceNode(), recoveryStatus.store());
        } catch (Exception e) {
            logger.debug("error while listing local files", e);
            onGoingRecoveries.failRecovery(recoveryStatus.recoveryId(),
                    new RecoveryFailedException(recoveryStatus.state(), "failed to list local files", e), true);
            return;
        }
        final Version sourceNodeVersion = recoveryStatus.sourceNode().version();
        if (sourceNodeVersion.before(Version.V_1_3_2) && recoverySettings.compress()) { // don't recover from pre 1.3.2 if compression is on?
            throw new ElasticsearchIllegalStateException("Can't recovery from node "
                    + recoveryStatus.sourceNode() + " with [" + RecoverySettings.INDICES_RECOVERY_COMPRESS
                    + " : true] due to compression bugs -  see issue #7210 for details" );
        }
        final StartRecoveryRequest request = new StartRecoveryRequest(recoveryStatus.shardId(), recoveryStatus.sourceNode(), clusterService.localNode(),
                false, existingFiles, recoveryStatus.state().getType(), recoveryStatus.recoveryId());

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
                sb.append("   phase3: recovered [").append(recoveryResponse.phase3Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryResponse.phase3Time)).append("]");
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

            if (cause instanceof IndexShardNotStartedException || cause instanceof IndexMissingException || cause instanceof IndexShardMissingException) {
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

    public static interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure);
    }

    class PrepareForTranslogOperationsRequestHandler extends BaseTransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public RecoveryPrepareForTranslogOperationsRequest newInstance() {
            return new RecoveryPrepareForTranslogOperationsRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps());
                recoveryStatus.indexShard().prepareForTranslogRecovery();
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class FinalizeRecoveryRequestHandler extends BaseTransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public RecoveryFinalizeRecoveryRequest newInstance() {
            return new RecoveryFinalizeRecoveryRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.indexShard().finalizeRecovery();
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class TranslogOperationsRequestHandler extends BaseTransportRequestHandler<RecoveryTranslogOperationsRequest> {


        @Override
        public RecoveryTranslogOperationsRequest newInstance() {
            return new RecoveryTranslogOperationsRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(RecoveryTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                final RecoveryState.Translog translog = recoveryStatus.state().getTranslog();
                translog.totalOperations(request.totalTranslogOps());
                for (Translog.Operation operation : request.operations()) {
                    recoveryStatus.indexShard().performRecoveryOperation(operation);
                    translog.incrementRecoveredOperations();
                }
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);

        }
    }

    class FilesInfoRequestHandler extends BaseTransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public RecoveryFilesInfoRequest newInstance() {
            return new RecoveryFilesInfoRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

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

     class CleanFilesRequestHandler extends BaseTransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public RecoveryCleanFilesRequest newInstance() {
            return new RecoveryCleanFilesRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final RecoveryStatus recoveryStatus = statusRef.status();
                recoveryStatus.state().getTranslog().totalOperations(request.totalTranslogOps());
                // first, we go and move files that were created with the recovery id suffix to
                // the actual names, its ok if we have a corrupted index here, since we have replicas
                // to recover from in case of a full cluster shutdown just when this code executes...
                recoveryStatus.renameAllTempFiles();
                final Store store = recoveryStatus.store();
                // now write checksums
                recoveryStatus.legacyChecksums().write(store);
                Store.MetadataSnapshot sourceMetaData = request.sourceMetaSnapshot();
                if (sourceMetaData == null) {
                    if (recoveryStatus.state().getSourceNode().getVersion().onOrAfter(Version.V_1_5_0)) {
                        throw new ElasticsearchIllegalStateException("Source node version is on or after 1.5 but has not metadata snapshot: " + recoveryStatus.state().getSourceNode());
                    }
                    if (request.legacySnapshotFiles() == null) {
                        throw new ElasticsearchIllegalStateException("Legacy snapshot files are null");
                    }
                    try {
                        store.cleanup("recovery CleanFilesRequestHandler", request.legacySnapshotFiles());
                    } catch (Exception ex) {
                        throw new RecoveryFailedException(recoveryStatus.state(), "failed to clean after recovery", ex);
                    }
                } else {
                    try {
                        store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
                    } catch (CorruptIndexException ex) {
                        // this is a fatal exception at this stage.
                        // this means we transferred files from the remote that have not be checksummed and they are
                        // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                        // source shard since this index might be broken there as well? The Source can handle this and checks
                        // its content on disk if possible.
                        try {
                            Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                        } catch (Throwable e) {
                            logger.debug("Failed to clean lucene index", e);
                            ex.addSuppressed(e);
                        }
                        throw new RecoveryFailedException(recoveryStatus.state(), "failed to clean after recovery", ex);
                    } catch (Exception ex) {
                        throw new RecoveryFailedException(recoveryStatus.state(), "failed to clean after recovery", ex);
                    }
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class FileChunkTransportRequestHandler extends BaseTransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public RecoveryFileChunkRequest newInstance() {
            return new RecoveryFileChunkRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

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
