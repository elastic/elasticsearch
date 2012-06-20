/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.indices.recovery;

import com.google.common.collect.Sets;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p/>
 * <p>Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
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

    private final RecoverySettings recoverySettings;

    private final ConcurrentMap<ShardId, RecoveryStatus> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    @Inject
    public RecoveryTarget(Settings settings, ThreadPool threadPool, TransportService transportService, IndicesService indicesService,
                          IndicesLifecycle indicesLifecycle, RecoverySettings recoverySettings) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;

        transportService.registerHandler(Actions.FILES_INFO, new FilesInfoRequestHandler());
        transportService.registerHandler(Actions.FILE_CHUNK, new FileChunkTransportRequestHandler());
        transportService.registerHandler(Actions.CLEAN_FILES, new CleanFilesRequestHandler());
        transportService.registerHandler(Actions.PREPARE_TRANSLOG, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.TRANSLOG_OPS, new TranslogOperationsRequestHandler());
        transportService.registerHandler(Actions.FINALIZE, new FinalizeRecoveryRequestHandler());

        indicesLifecycle.addListener(new IndicesLifecycle.Listener() {
            @Override
            public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
                removeAndCleanOnGoingRecovery(shardId);
            }
        });
    }

    public RecoveryStatus peerRecoveryStatus(ShardId shardId) {
        RecoveryStatus peerRecoveryStatus = onGoingRecoveries.get(shardId);
        if (peerRecoveryStatus == null) {
            return null;
        }
        // update how long it takes if we are still recovering...
        if (peerRecoveryStatus.startTime > 0 && peerRecoveryStatus.stage != RecoveryStatus.Stage.DONE) {
            peerRecoveryStatus.time = System.currentTimeMillis() - peerRecoveryStatus.startTime;
        }
        return peerRecoveryStatus;
    }

    public void startRecovery(final StartRecoveryRequest request, final boolean fromRetry, final RecoveryListener listener) {
        if (request.sourceNode() == null) {
            listener.onIgnoreRecovery(false, "No node to recover from, retry on next cluster state update");
            return;
        }
        IndexService indexService = indicesService.indexService(request.shardId().index().name());
        if (indexService == null) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "index missing locally, stop recovery");
            return;
        }
        final InternalIndexShard shard = (InternalIndexShard) indexService.shard(request.shardId().id());
        if (shard == null) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "shard missing locally, stop recovery");
            return;
        }
        if (!fromRetry) {
            try {
                shard.recovering("from " + request.sourceNode());
            } catch (IllegalIndexShardStateException e) {
                // that's fine, since we might be called concurrently, just ignore this, we are already recovering
                listener.onIgnoreRecovery(false, "already in recovering process, " + e.getMessage());
                return;
            }
        }
        if (shard.state() == IndexShardState.CLOSED) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
            return;
        }
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                doRecovery(shard, request, fromRetry, listener);
            }
        });
    }

    private void doRecovery(final InternalIndexShard shard, final StartRecoveryRequest request, final boolean fromRetry, final RecoveryListener listener) {
        if (shard.state() == IndexShardState.CLOSED) {
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
            return;
        }

        RecoveryStatus recovery;
        if (fromRetry) {
            recovery = onGoingRecoveries.get(request.shardId());
        } else {
            recovery = new RecoveryStatus();
            onGoingRecoveries.put(request.shardId(), recovery);
        }

        try {
            logger.trace("[{}][{}] starting recovery from {}", request.shardId().index().name(), request.shardId().id(), request.sourceNode());

            StopWatch stopWatch = new StopWatch().start();
            RecoveryResponse recoveryStatus = transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request, new FutureTransportResponseHandler<RecoveryResponse>() {
                @Override
                public RecoveryResponse newInstance() {
                    return new RecoveryResponse();
                }
            }).txGet();
            if (shard.state() == IndexShardState.CLOSED) {
                removeAndCleanOnGoingRecovery(shard.shardId());
                listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
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
                sb.append("   phase2: start took [").append(timeValueMillis(recoveryStatus.startTime)).append("]\n");
                sb.append("         : recovered [").append(recoveryStatus.phase2Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryStatus.phase2Time)).append("]")
                        .append("\n");
                sb.append("   phase3: recovered [").append(recoveryStatus.phase3Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(timeValueMillis(recoveryStatus.phase3Time)).append("]");
                logger.debug(sb.toString());
            }
            removeAndCleanOnGoingRecovery(request.shardId());
            listener.onRecoveryDone();
        } catch (Exception e) {
//            logger.trace("[{}][{}] Got exception on recovery", e, request.shardId().index().name(), request.shardId().id());
            if (shard.state() == IndexShardState.CLOSED) {
                removeAndCleanOnGoingRecovery(request.shardId());
                listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
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

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IndexShardNotStartedException || cause instanceof IndexMissingException || cause instanceof IndexShardMissingException) {
                // if the target is not ready yet, retry
                listener.onRetryRecovery(TimeValue.timeValueMillis(500));
                return;
            }

            // here, we check against ignore recovery options

            // in general, no need to clean the shard on ignored recovery, since we want to try and reuse it later
            // it will get deleted in the IndicesStore if all are allocated and no shard exists on this node...

            removeAndCleanOnGoingRecovery(request.shardId());

            if (cause instanceof ConnectTransportException) {
                listener.onIgnoreRecovery(true, "source node disconnected (" + request.sourceNode() + ")");
                return;
            }

            if (cause instanceof IndexShardClosedException) {
                listener.onIgnoreRecovery(true, "source shard is closed (" + request.sourceNode() + ")");
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                listener.onIgnoreRecovery(true, "source shard is closed (" + request.sourceNode() + ")");
                return;
            }

            logger.trace("[{}][{}] recovery from [{}] failed", e, request.shardId().index().name(), request.shardId().id(), request.sourceNode());
            listener.onRecoveryFailure(new RecoveryFailedException(request, e), true);
        }
    }

    public static interface RecoveryListener {
        void onRecoveryDone();

        void onRetryRecovery(TimeValue retryAfter);

        void onIgnoreRecovery(boolean removeShard, String reason);

        void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);
    }


    private void removeAndCleanOnGoingRecovery(ShardId shardId) {
        // clean it from the on going recoveries since it is being closed
        RecoveryStatus peerRecoveryStatus = onGoingRecoveries.remove(shardId);
        if (peerRecoveryStatus != null) {
            // clean open index outputs
            for (Map.Entry<String, IndexOutput> entry : peerRecoveryStatus.openIndexOutputs.entrySet()) {
                synchronized (entry.getValue()) {
                    try {
                        entry.getValue().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
            peerRecoveryStatus.openIndexOutputs = null;
            peerRecoveryStatus.checksums = null;
        }
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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());

            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.stage = RecoveryStatus.Stage.TRANSLOG;

            shard.performRecoveryPrepareForTranslog();
            channel.sendResponse(VoidStreamable.INSTANCE);
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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            RecoveryStatus peerRecoveryStatus = onGoingRecoveries.get(shard.shardId());
            if (peerRecoveryStatus == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            peerRecoveryStatus.stage = RecoveryStatus.Stage.FINALIZE;
            shard.performRecoveryFinalization(false, peerRecoveryStatus);
            peerRecoveryStatus.time = System.currentTimeMillis() - peerRecoveryStatus.startTime;
            peerRecoveryStatus.stage = RecoveryStatus.Stage.DONE;
            channel.sendResponse(VoidStreamable.INSTANCE);
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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            for (Translog.Operation operation : request.operations()) {
                shard.performRecoveryOperation(operation);
            }

            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            onGoingRecovery.currentTranslogOperations += request.operations().size();

            channel.sendResponse(VoidStreamable.INSTANCE);
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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId.index().name()).shardSafe(request.shardId.id());
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(shard.shardId());
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
            onGoingRecovery.stage = RecoveryStatus.Stage.INDEX;
            channel.sendResponse(VoidStreamable.INSTANCE);
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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }

            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            String suffix = "." + onGoingRecovery.startTime;
            Set<String> filesToRename = Sets.newHashSet();
            for (String existingFile : shard.store().directory().listAll()) {
                if (existingFile.endsWith(suffix)) {
                    filesToRename.add(existingFile.substring(0, existingFile.length() - suffix.length()));
                }
            }
            Exception failureToRename = null;
            if (!filesToRename.isEmpty()) {
                // first, go and delete the existing ones
                for (String fileToRename : filesToRename) {
                    shard.store().directory().deleteFile(fileToRename);
                }
                for (String fileToRename : filesToRename) {
                    // now, rename the files...
                    try {
                        shard.store().renameFile(fileToRename + suffix, fileToRename);
                    } catch (Exception e) {
                        failureToRename = e;
                        break;
                    }
                }
            }
            if (failureToRename != null) {
                throw failureToRename;
            }
            // now write checksums
            shard.store().writeChecksums(onGoingRecovery.checksums);

            for (String existingFile : shard.store().directory().listAll()) {
                // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete checksum)
                if (!request.snapshotFiles().contains(existingFile) && !Store.isChecksum(existingFile)) {
                    try {
                        shard.store().directory().deleteFile(existingFile);
                    } catch (Exception e) {
                        // ignore, we don't really care, will get deleted later on
                    }
                }
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    class FileChunkTransportRequestHandler extends BaseTransportRequestHandler<RecoveryFileChunkRequest> {


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
            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(shard.shardId());
            if (onGoingRecovery == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            IndexOutput indexOutput;
            if (request.position() == 0) {
                // first request
                onGoingRecovery.checksums.remove(request.name());
                indexOutput = onGoingRecovery.openIndexOutputs.remove(request.name());
                if (indexOutput != null) {
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
                // we create an output with no checksum, this is because the pure binary data of the file is not
                // the checksum (because of seek). We will create the checksum file once copying is done

                // also, we check if the file already exists, if it does, we create a file name based
                // on the current recovery "id" and later we make the switch, the reason for that is that
                // we only want to overwrite the index files once we copied all over, and not create a
                // case where the index is half moved

                String name = request.name();
                if (shard.store().directory().fileExists(name)) {
                    name = name + "." + onGoingRecovery.startTime;
                }

                indexOutput = shard.store().createOutputRaw(name);

                onGoingRecovery.openIndexOutputs.put(request.name(), indexOutput);
            } else {
                indexOutput = onGoingRecovery.openIndexOutputs.get(request.name());
            }
            if (indexOutput == null) {
                // shard is getting closed on us
                throw new IndexShardClosedException(shard.shardId());
            }
            synchronized (indexOutput) {
                try {
                    if (recoverySettings.rateLimiter() != null) {
                        recoverySettings.rateLimiter().pause(request.content().length());
                    }
                    indexOutput.writeBytes(request.content().bytes(), request.content().offset(), request.content().length());
                    onGoingRecovery.currentFilesSize.addAndGet(request.length());
                    if (indexOutput.getFilePointer() == request.length()) {
                        // we are done
                        indexOutput.close();
                        // write the checksum
                        if (request.checksum() != null) {
                            onGoingRecovery.checksums.put(request.name(), request.checksum());
                        }
                        shard.store().directory().sync(Collections.singleton(request.name()));
                        onGoingRecovery.openIndexOutputs.remove(request.name());
                    }
                } catch (IOException e) {
                    onGoingRecovery.openIndexOutputs.remove(request.name());
                    try {
                        indexOutput.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                    throw e;
                }
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }
}
