/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.shard.recovery;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 *
 * <p>Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 *
 * @author kimchy (shay.banon)
 */
public class RecoveryTarget extends AbstractComponent {

    public static class Actions {
        public static final String FILES_INFO = "index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "index/shard/recovery/fileChunk";
        public static final String CLEAN_FILES = "index/shard/recovery/cleanFiles";
        public static final String TRANSLOG_OPS = "index/shard/recovery/translogOps";
        public static final String PREPARE_TRANSLOG = "index/shard/recovery/prepareTranslog";
        public static final String FINALIZE = "index/shard/recovery/finalize";
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final IndicesService indicesService;

    private final RecoveryThrottler recoveryThrottler;

    private final ConcurrentMap<ShardId, OnGoingRecovery> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    @Inject public RecoveryTarget(Settings settings, ThreadPool threadPool, TransportService transportService, IndicesService indicesService,
                                  IndicesLifecycle indicesLifecycle, RecoveryThrottler recoveryThrottler) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoveryThrottler = recoveryThrottler;

        transportService.registerHandler(Actions.FILES_INFO, new FilesInfoRequestHandler());
        transportService.registerHandler(Actions.FILE_CHUNK, new FileChunkTransportRequestHandler());
        transportService.registerHandler(Actions.CLEAN_FILES, new CleanFilesRequestHandler());
        transportService.registerHandler(Actions.PREPARE_TRANSLOG, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.TRANSLOG_OPS, new TranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.FINALIZE, new FinalizeRecoveryRequestHandler());

        indicesLifecycle.addListener(new IndicesLifecycle.Listener() {
            @Override public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
                removeAndCleanOnGoingRecovery(shardId);
            }
        });
    }

    public void startRecovery(final StartRecoveryRequest request, final boolean fromRetry, final RecoveryListener listener) {
        if (request.sourceNode() == null) {
            listener.onIgnoreRecovery(false, "No node to recovery from, retry on next cluster state update");
            return;
        }
        IndexService indexService = indicesService.indexService(request.shardId().index().name());
        if (indexService == null) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "index missing, stop recovery");
            return;
        }
        final InternalIndexShard shard = (InternalIndexShard) indexService.shard(request.shardId().id());
        if (shard == null) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "shard missing, stop recovery");
            return;
        }
        if (!fromRetry) {
            try {
                shard.recovering();
            } catch (IllegalIndexShardStateException e) {
                // that's fine, since we might be called concurrently, just ignore this, we are already recovering
                listener.onIgnoreRecovery(false, "already in recovering process, " + e.getMessage());
                return;
            }
        }
        if (shard.state() == IndexShardState.CLOSED) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "shard closed, stop recovery");
            return;
        }
        threadPool.cached().execute(new Runnable() {
            @Override public void run() {
                doRecovery(shard, request, fromRetry, listener);
            }
        });
    }

    private void doRecovery(final InternalIndexShard shard, final StartRecoveryRequest request, final boolean fromRetry, final RecoveryListener listener) {
        if (shard.state() == IndexShardState.CLOSED) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "shard closed, stop recovery");
            return;
        }

        OnGoingRecovery recovery;
        if (fromRetry) {
            recovery = onGoingRecoveries.get(request.shardId());
        } else {
            recovery = new OnGoingRecovery();
            onGoingRecoveries.put(request.shardId(), recovery);
        }

        if (!recoveryThrottler.tryRecovery(shard.shardId(), "peer recovery target")) {
            recovery.stage = OnGoingRecovery.Stage.RETRY;
            recovery.retryTimeInMillis = System.currentTimeMillis() - recovery.startTimeImMillis;
            listener.onRetryRecovery(recoveryThrottler.throttleInterval());
            return;
        }

        try {
            logger.trace("[{}][{}] starting recovery from {}", request.shardId().index().name(), request.shardId().id(), request.sourceNode());

            StopWatch stopWatch = new StopWatch().start();
            RecoveryResponse recoveryStatus = transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request, new FutureTransportResponseHandler<RecoveryResponse>() {
                @Override public RecoveryResponse newInstance() {
                    return new RecoveryResponse();
                }
            }).txGet();
            if (recoveryStatus.retry) {
                if (shard.state() == IndexShardState.CLOSED) {
                    listener.onIgnoreRecovery(false, "shard closed, stop recovery");
                    return;
                }
                logger.trace("[{}][{}] retrying recovery in [{}], source shard is busy", request.shardId().index().name(), request.shardId().id(), recoveryThrottler.throttleInterval());
                recovery.stage = OnGoingRecovery.Stage.RETRY;
                recovery.retryTimeInMillis = System.currentTimeMillis() - recovery.startTimeImMillis;
                listener.onRetryRecovery(recoveryThrottler.throttleInterval());
                return;
            }
            stopWatch.stop();
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[').append(request.shardId().index().name()).append(']').append('[').append(request.shardId().id()).append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(stopWatch.totalTime()).append("]\n");
                sb.append("   phase1: recovered_files [").append(recoveryStatus.phase1FileNames.size()).append("]").append(" with total_size of [").append(new ByteSizeValue(recoveryStatus.phase1TotalSize)).append("]")
                        .append(", took [").append(timeValueMillis(recoveryStatus.phase1Time)).append("], throttling_wait [").append(timeValueMillis(recoveryStatus.phase1ThrottlingWaitTime)).append(']')
                        .append("\n");
                sb.append("         : reusing_files   [").append(recoveryStatus.phase1ExistingFileNames.size()).append("] with total_size of [").append(new ByteSizeValue(recoveryStatus.phase1ExistingTotalSize)).append("]\n");
                sb.append("   phase2: recovered [").append(recoveryStatus.phase2Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryStatus.phase2Time)).append("]")
                        .append("\n");
                sb.append("   phase3: recovered [").append(recoveryStatus.phase3Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryStatus.phase3Time)).append("]");
                logger.debug(sb.toString());
            }
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onRecoveryDone();
        } catch (Exception e) {
            if (shard.state() == IndexShardState.CLOSED) {
                removeAndCleanOnGoingRecovery(request.shardId());
                listener.onIgnoreRecovery(false, "shard closed, stop recovery");
                return;
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

            if (cause instanceof IndexShardNotStartedException || cause instanceof IndexMissingException || cause instanceof IndexShardMissingException) {
                recovery.stage = OnGoingRecovery.Stage.RETRY;
                recovery.retryTimeInMillis = System.currentTimeMillis() - recovery.startTimeImMillis;
                listener.onRetryRecovery(recoveryThrottler.throttleInterval());
                return;
            }

            removeAndCleanOnGoingRecovery(request.shardId());
            logger.trace("[{}][{}] recovery from [{}] failed", e, request.shardId().index().name(), request.shardId().id(), request.sourceNode());

            if (cause instanceof ConnectTransportException) {
                listener.onIgnoreRecovery(true, "source node disconnected");
                return;
            }

            if (cause instanceof IndexShardClosedException) {
                listener.onIgnoreRecovery(true, "source node disconnected");
                return;
            }

            listener.onRecoveryFailure(new RecoveryFailedException(request, e), true);
        } finally {
            recoveryThrottler.recoveryDone(shard.shardId(), "peer recovery target");
        }
    }

    public static interface RecoveryListener {
        void onRecoveryDone();

        void onRetryRecovery(TimeValue retryAfter);

        void onIgnoreRecovery(boolean cleanShard, String reason);

        void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);
    }


    private void removeAndCleanOnGoingRecovery(ShardId shardId) {
        // clean it from the on going recoveries since it is being closed
        OnGoingRecovery onGoingRecovery = onGoingRecoveries.remove(shardId);
        if (onGoingRecovery != null) {
            // clean open index outputs
            for (Map.Entry<String, IndexOutput> entry : onGoingRecovery.openIndexOutputs.entrySet()) {
                synchronized (entry.getValue()) {
                    try {
                        entry.getValue().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    class PrepareForTranslogOperationsRequestHandler extends BaseTransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override public RecoveryPrepareForTranslogOperationsRequest newInstance() {
            return new RecoveryPrepareForTranslogOperationsRequest();
        }

        @Override public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());

            OnGoingRecovery onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.stage = OnGoingRecovery.Stage.TRANSLOG;

            shard.performRecoveryPrepareForTranslog();
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class FinalizeRecoveryRequestHandler extends BaseTransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override public RecoveryFinalizeRecoveryRequest newInstance() {
            return new RecoveryFinalizeRecoveryRequest();
        }

        @Override public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            OnGoingRecovery onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.stage = OnGoingRecovery.Stage.FINALIZE;
            shard.performRecoveryFinalization(false);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class TranslogOperationsRequestHandler extends BaseTransportRequestHandler<RecoveryTranslogOperationsRequest> {


        @Override public RecoveryTranslogOperationsRequest newInstance() {
            return new RecoveryTranslogOperationsRequest();
        }

        @Override public void messageReceived(RecoveryTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            for (Translog.Operation operation : request.operations()) {
                shard.performRecoveryOperation(operation);
            }

            OnGoingRecovery onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.currentTranslogOperations += request.operations().size();

            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class FilesInfoRequestHandler extends BaseTransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override public RecoveryFilesInfoRequest newInstance() {
            return new RecoveryFilesInfoRequest();
        }

        @Override public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId.index().name()).shardSafe(request.shardId.id());
            OnGoingRecovery onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.phase1FileNames = request.phase1FileNames;
            onGoingRecovery.phase1FileSizes = request.phase1FileSizes;
            onGoingRecovery.phase1ExistingFileNames = request.phase1ExistingFileNames;
            onGoingRecovery.phase1ExistingFileSizes = request.phase1ExistingFileSizes;
            onGoingRecovery.phase1TotalSize = request.phase1TotalSize;
            onGoingRecovery.phase1ExistingTotalSize = request.phase1ExistingTotalSize;
            onGoingRecovery.stage = OnGoingRecovery.Stage.FILES;
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class CleanFilesRequestHandler extends BaseTransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override public RecoveryCleanFilesRequest newInstance() {
            return new RecoveryCleanFilesRequest();
        }

        @Override public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            for (String existingFile : shard.store().directory().listAll()) {
                if (!request.snapshotFiles().contains(existingFile)) {
                    shard.store().directory().deleteFile(existingFile);
                }
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class FileChunkTransportRequestHandler extends BaseTransportRequestHandler<RecoveryFileChunkRequest> {


        @Override public RecoveryFileChunkRequest newInstance() {
            return new RecoveryFileChunkRequest();
        }

        @Override public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel) throws Exception {
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            OnGoingRecovery onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            IndexOutput indexOutput;
            if (request.position() == 0) {
                // first request
                indexOutput = onGoingRecovery.openIndexOutputs.remove(request.name());
                if (indexOutput != null) {
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
                indexOutput = shard.store().directory().createOutput(request.name());
                onGoingRecovery.openIndexOutputs.put(request.name(), indexOutput);
            } else {
                indexOutput = onGoingRecovery.openIndexOutputs.get(request.name());
            }
            if (indexOutput == null) {
                throw new ElasticSearchIllegalStateException("No ongoing output file to write to, request: " + request);
            }
            synchronized (indexOutput) {
                try {
                    indexOutput.writeBytes(request.content(), request.contentLength());
                    onGoingRecovery.currentFilesSize.addAndGet(request.contentLength());
                    if (indexOutput.getFilePointer() == request.length()) {
                        // we are done
                        indexOutput.close();
                        onGoingRecovery.openIndexOutputs.remove(request.name());
                    }
                } catch (IOException e) {
                    onGoingRecovery.openIndexOutputs.remove(request.name());
                    try {
                        indexOutput.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                }
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }
}
