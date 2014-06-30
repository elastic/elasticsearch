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

import com.google.common.collect.Sets;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.engine.RecoveryEngineException;
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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

    private final ConcurrentMapLong<RecoveryStatus> onGoingRecoveries = ConcurrentCollections.newConcurrentMapLong();

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
            public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard) {
                if (indexShard != null) {
                    removeAndCleanOnGoingRecovery(findRecoveryByShard(indexShard));
                }
            }
        });
    }

    public RecoveryStatus recoveryStatus(ShardId shardId) {
        RecoveryStatus peerRecoveryStatus = findRecoveryByShardId(shardId);
        if (peerRecoveryStatus == null) {
            return null;
        }
        // update how long it takes if we are still recovering...
        if (peerRecoveryStatus.recoveryState().getTimer().startTime() > 0 && peerRecoveryStatus.stage() != RecoveryState.Stage.DONE) {
            peerRecoveryStatus.recoveryState().getTimer().time(System.currentTimeMillis() - peerRecoveryStatus.recoveryState().getTimer().startTime());
        }
        return peerRecoveryStatus;
    }

    public void cancelRecovery(IndexShard indexShard) {
        RecoveryStatus recoveryStatus = findRecoveryByShard(indexShard);
        // it might be if the recovery source got canceled first
        if (recoveryStatus == null) {
            return;
        }
        if (recoveryStatus.sentCanceledToSource) {
            return;
        }
        recoveryStatus.cancel();
        try {
            if (recoveryStatus.recoveryThread != null) {
                recoveryStatus.recoveryThread.interrupt();
            }
            // give it a grace period of actually getting the sent ack part
            final long sleepTime = 100;
            final long maxSleepTime = 10000;
            long rounds = Math.round(maxSleepTime / sleepTime);
            while (!recoveryStatus.sentCanceledToSource && rounds > 0) {
                rounds--;
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break; // interrupted - step out!
                }
            }
        } finally {
            removeAndCleanOnGoingRecovery(recoveryStatus);
        }

    }

    public void startRecovery(final StartRecoveryRequest request, final InternalIndexShard indexShard, final RecoveryListener listener) {
        try {
            indexShard.recovering("from " + request.sourceNode());
        } catch (IllegalIndexShardStateException e) {
            // that's fine, since we might be called concurrently, just ignore this, we are already recovering
            listener.onIgnoreRecovery(false, "already in recovering process, " + e.getMessage());
            return;
        }
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                // create a new recovery status, and process...
                RecoveryStatus recoveryStatus = new RecoveryStatus(request.recoveryId(), indexShard);
                recoveryStatus.recoveryState.setType(request.recoveryType());
                recoveryStatus.recoveryState.setSourceNode(request.sourceNode());
                recoveryStatus.recoveryState.setTargetNode(request.targetNode());
                recoveryStatus.recoveryState.setPrimary(indexShard.routingEntry().primary());
                onGoingRecoveries.put(recoveryStatus.recoveryId, recoveryStatus);
                doRecovery(request, recoveryStatus, listener);
            }
        });
    }

    public void retryRecovery(final StartRecoveryRequest request, TimeValue retryAfter, final RecoveryStatus status, final RecoveryListener listener) {
        threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC ,new Runnable() {
            @Override
            public void run() {
                doRecovery(request, status, listener);
            }
        });
    }

    private void doRecovery(final StartRecoveryRequest request, final RecoveryStatus recoveryStatus, final RecoveryListener listener) {

        assert request.sourceNode() != null : "can't do a recovery without a source node";

        final InternalIndexShard shard = recoveryStatus.indexShard;
        if (shard == null) {
            listener.onIgnoreRecovery(false, "shard missing locally, stop recovery");
            return;
        }
        if (shard.state() == IndexShardState.CLOSED) {
            listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
            return;
        }
        if (recoveryStatus.isCanceled()) {
            // don't remove it, the cancellation code will remove it...
            listener.onIgnoreRecovery(false, "canceled recovery");
            return;
        }

        recoveryStatus.recoveryThread = Thread.currentThread();

        try {
            logger.trace("[{}][{}] starting recovery from {}", request.shardId().index().name(), request.shardId().id(), request.sourceNode());

            StopWatch stopWatch = new StopWatch().start();
            RecoveryResponse recoveryResponse = transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request, new FutureTransportResponseHandler<RecoveryResponse>() {
                @Override
                public RecoveryResponse newInstance() {
                    return new RecoveryResponse();
                }
            }).txGet();
            if (shard.state() == IndexShardState.CLOSED) {
                removeAndCleanOnGoingRecovery(recoveryStatus);
                listener.onIgnoreRecovery(false, "local shard closed, stop recovery");
                return;
            }
            stopWatch.stop();
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[').append(request.shardId().index().name()).append(']').append('[').append(request.shardId().id()).append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(stopWatch.totalTime()).append("]\n");
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
            } else if (logger.isDebugEnabled()) {
                logger.debug("{} recovery completed from [{}], took [{}]", request.shardId(), request.sourceNode(), stopWatch.totalTime());
            }
            removeAndCleanOnGoingRecovery(recoveryStatus);
            listener.onRecoveryDone();
        } catch (Throwable e) {
//            logger.trace("[{}][{}] Got exception on recovery", e, request.shardId().index().name(), request.shardId().id());
            if (recoveryStatus.isCanceled()) {
                // don't remove it, the cancellation code will remove it...
                listener.onIgnoreRecovery(false, "canceled recovery");
                return;
            }
            if (shard.state() == IndexShardState.CLOSED) {
                removeAndCleanOnGoingRecovery(recoveryStatus);
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
                listener.onRetryRecovery(TimeValue.timeValueMillis(500), recoveryStatus);
                return;
            }

            if (cause instanceof DelayRecoveryException) {
                listener.onRetryRecovery(TimeValue.timeValueMillis(500), recoveryStatus);
                return;
            }

            // here, we check against ignore recovery options

            // in general, no need to clean the shard on ignored recovery, since we want to try and reuse it later
            // it will get deleted in the IndicesStore if all are allocated and no shard exists on this node...

            removeAndCleanOnGoingRecovery(recoveryStatus);

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

            logger.warn("[{}][{}] recovery from [{}] failed", e, request.shardId().index().name(), request.shardId().id(), request.sourceNode());
            listener.onRecoveryFailure(new RecoveryFailedException(request, e), true);
        }
    }

    public static interface RecoveryListener {
        void onRecoveryDone();

        void onRetryRecovery(TimeValue retryAfter, RecoveryStatus status);

        void onIgnoreRecovery(boolean removeShard, String reason);

        void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);
    }

    @Nullable
    private RecoveryStatus findRecoveryByShardId(ShardId shardId) {
        for (RecoveryStatus recoveryStatus : onGoingRecoveries.values()) {
            if (recoveryStatus.shardId.equals(shardId)) {
                return recoveryStatus;
            }
        }
        return null;
    }

    @Nullable
    private RecoveryStatus findRecoveryByShard(IndexShard indexShard) {
        for (RecoveryStatus recoveryStatus : onGoingRecoveries.values()) {
            if (recoveryStatus.indexShard == indexShard) {
                return recoveryStatus;
            }
        }
        return null;
    }

    private void removeAndCleanOnGoingRecovery(@Nullable RecoveryStatus status) {
        if (status == null) {
            return;
        }
        // clean it from the on going recoveries since it is being closed
        status = onGoingRecoveries.remove(status.recoveryId);
        if (status == null) {
            return;
        }
        // just mark it as canceled as well, just in case there are in flight requests
        // coming from the recovery target
        status.cancel();
        // clean open index outputs
        Set<Entry<String, IndexOutput>> entrySet = status.cancleAndClearOpenIndexInputs();
        Iterator<Entry<String, IndexOutput>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, IndexOutput> entry = iterator.next();
            synchronized (entry.getValue()) {
                IOUtils.closeWhileHandlingException(entry.getValue());
            }
            iterator.remove();

        }
        status.checksums = null;
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            onGoingRecovery.indexShard.performRecoveryPrepareForTranslog();
            onGoingRecovery.stage(RecoveryState.Stage.TRANSLOG);
            onGoingRecovery.recoveryState.getStart().checkIndexTime(onGoingRecovery.indexShard.checkIndexTook());
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            onGoingRecovery.stage(RecoveryState.Stage.FINALIZE);
            onGoingRecovery.indexShard.performRecoveryFinalization(false, onGoingRecovery);
            onGoingRecovery.recoveryState().getTimer().time(System.currentTimeMillis() - onGoingRecovery.recoveryState().getTimer().startTime());
            onGoingRecovery.stage(RecoveryState.Stage.DONE);
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
            for (Translog.Operation operation : request.operations()) {
                validateRecoveryStatus(onGoingRecovery, request.shardId());
                shard.performRecoveryOperation(operation);
                onGoingRecovery.recoveryState.getTranslog().incrementTranslogOperations();
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            onGoingRecovery.recoveryState().getIndex().addFileDetails(request.phase1FileNames, request.phase1FileSizes);
            onGoingRecovery.recoveryState().getIndex().addReusedFileDetails(request.phase1ExistingFileNames, request.phase1ExistingFileSizes);
            onGoingRecovery.recoveryState().getIndex().totalByteCount(request.phase1TotalSize);
            onGoingRecovery.recoveryState().getIndex().reusedByteCount(request.phase1ExistingTotalSize);
            onGoingRecovery.recoveryState().getIndex().totalFileCount(request.phase1FileNames.size());
            onGoingRecovery.stage(RecoveryState.Stage.INDEX);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            final Store store = onGoingRecovery.indexShard.store();
            store.incRef();
            try {
                // first, we go and move files that were created with the recovery id suffix to
                // the actual names, its ok if we have a corrupted index here, since we have replicas
                // to recover from in case of a full cluster shutdown just when this code executes...
                String prefix = "recovery." + onGoingRecovery.recoveryState().getTimer().startTime() + ".";
                Set<String> filesToRename = Sets.newHashSet();
                for (String existingFile : store.directory().listAll()) {
                    if (existingFile.startsWith(prefix)) {
                        filesToRename.add(existingFile.substring(prefix.length(), existingFile.length()));
                    }
                }
                Exception failureToRename = null;
                if (!filesToRename.isEmpty()) {
                    // first, go and delete the existing ones
                    final Directory directory = store.directory();
                    for (String file : filesToRename) {
                        try {
                            directory.deleteFile(file);
                        } catch (Throwable ex) {
                            logger.debug("failed to delete file [{}]", ex, file);
                        }
                    }
                    for (String fileToRename : filesToRename) {
                        // now, rename the files... and fail it it won't work
                        store.renameFile(prefix + fileToRename, fileToRename);
                    }
                }
                // now write checksums
                store.writeChecksums(onGoingRecovery.checksums);

                for (String existingFile : store.directory().listAll()) {
                    // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete checksum)
                    if (!request.snapshotFiles().contains(existingFile) && !Store.isChecksum(existingFile)) {
                        try {
                            store.directory().deleteFile(existingFile);
                        } catch (Exception e) {
                            // ignore, we don't really care, will get deleted later on
                        }
                    }
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } finally {
                store.decRef();
            }
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
            RecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            validateRecoveryStatus(onGoingRecovery, request.shardId());

            Store store = onGoingRecovery.indexShard.store();
            store.incRef();
            try {
                IndexOutput indexOutput;
                if (request.position() == 0) {
                    // first request
                    onGoingRecovery.checksums.remove(request.name());
                    indexOutput = onGoingRecovery.removeOpenIndexOutputs(request.name());
                    IOUtils.closeWhileHandlingException(indexOutput);
                    // we create an output with no checksum, this is because the pure binary data of the file is not
                    // the checksum (because of seek). We will create the checksum file once copying is done

                    // also, we check if the file already exists, if it does, we create a file name based
                    // on the current recovery "id" and later we make the switch, the reason for that is that
                    // we only want to overwrite the index files once we copied all over, and not create a
                    // case where the index is half moved

                    String fileName = request.name();
                    if (store.directory().fileExists(fileName)) {
                        fileName = "recovery." + onGoingRecovery.recoveryState().getTimer().startTime() + "." + fileName;
                    }
                    indexOutput = onGoingRecovery.openAndPutIndexOutput(request.name(), fileName, store);
                } else {
                    indexOutput = onGoingRecovery.getOpenIndexOutput(request.name());
                }
                if (indexOutput == null) {
                    // shard is getting closed on us
                    throw new IndexShardClosedException(request.shardId());
                }
                boolean success = false;
                synchronized (indexOutput) {
                    try {
                        if (recoverySettings.rateLimiter() != null) {
                            recoverySettings.rateLimiter().pause(request.content().length());
                        }
                        BytesReference content = request.content();
                        if (!content.hasArray()) {
                            content = content.toBytesArray();
                        }
                        indexOutput.writeBytes(content.array(), content.arrayOffset(), content.length());
                        onGoingRecovery.recoveryState.getIndex().addRecoveredByteCount(content.length());
                        RecoveryState.File file = onGoingRecovery.recoveryState.getIndex().file(request.name());
                        if (file != null) {
                            file.updateRecovered(request.length());
                        }
                        if (indexOutput.getFilePointer() == request.length()) {
                            // we are done
                            indexOutput.close();
                            // write the checksum
                            if (request.checksum() != null) {
                                onGoingRecovery.checksums.put(request.name(), request.checksum());
                            }
                            store.directory().sync(Collections.singleton(request.name()));
                            IndexOutput remove = onGoingRecovery.removeOpenIndexOutputs(request.name());
                            onGoingRecovery.recoveryState.getIndex().addRecoveredFileCount(1);
                            assert remove == null || remove == indexOutput; // remove maybe null if we got canceled
                        }
                        success = true;
                    } finally {
                        if (!success || onGoingRecovery.isCanceled()) {
                            IndexOutput remove = onGoingRecovery.removeOpenIndexOutputs(request.name());
                            assert remove == null || remove == indexOutput;
                            IOUtils.closeWhileHandlingException(indexOutput);
                        }
                    }
                }
                if (onGoingRecovery.isCanceled()) {
                    onGoingRecovery.sentCanceledToSource = true;
                    throw new IndexShardClosedException(request.shardId());
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } finally {
                store.decRef();
            }
        }
    }

    private void validateRecoveryStatus(RecoveryStatus onGoingRecovery, ShardId shardId) {
        if (onGoingRecovery == null) {
            // shard is getting closed on us
            throw new IndexShardClosedException(shardId);
        }
        if (onGoingRecovery.indexShard.state() == IndexShardState.CLOSED) {
            cancelRecovery(onGoingRecovery.indexShard);
            onGoingRecovery.sentCanceledToSource = true;
            throw new IndexShardClosedException(shardId);
        }
        if (onGoingRecovery.isCanceled()) {
            onGoingRecovery.sentCanceledToSource = true;
            throw new IndexShardClosedException(shardId);
        }
    }
}
