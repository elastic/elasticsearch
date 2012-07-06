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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class RecoverySource extends AbstractComponent {

    public static class Actions {
        public static final String START_RECOVERY = "index/shard/recovery/startRecovery";
    }

    private final TransportService transportService;

    private final IndicesService indicesService;

    private final RecoverySettings recoverySettings;


    @Inject
    public RecoverySource(Settings settings, TransportService transportService, IndicesService indicesService,
                          RecoverySettings recoverySettings) {
        super(settings);
        this.transportService = transportService;
        this.indicesService = indicesService;

        this.recoverySettings = recoverySettings;

        transportService.registerHandler(Actions.START_RECOVERY, new StartRecoveryTransportRequestHandler());
    }

    private RecoveryResponse recover(final StartRecoveryRequest request) {
        final InternalIndexShard shard = (InternalIndexShard) indicesService.indexServiceSafe(request.shardId().index().name()).shardSafe(request.shardId().id());
        logger.trace("[{}][{}] starting recovery to {}, mark_as_relocated {}", request.shardId().index().name(), request.shardId().id(), request.targetNode(), request.markAsRelocated());
        final RecoveryResponse response = new RecoveryResponse();
        shard.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(final SnapshotIndexCommit snapshot) throws ElasticSearchException {
                long totalSize = 0;
                long existingTotalSize = 0;
                try {
                    StopWatch stopWatch = new StopWatch().start();

                    for (String name : snapshot.getFiles()) {
                        StoreFileMetaData md = shard.store().metaData(name);
                        boolean useExisting = false;
                        if (request.existingFiles().containsKey(name)) {
                            // we don't compute checksum for segments, so always recover them
                            if (!name.startsWith("segments") && md.isSame(request.existingFiles().get(name))) {
                                response.phase1ExistingFileNames.add(name);
                                response.phase1ExistingFileSizes.add(md.length());
                                existingTotalSize += md.length();
                                useExisting = true;
                                if (logger.isTraceEnabled()) {
                                    logger.trace("[{}][{}] recovery [phase1] to {}: not recovering [{}], exists in local store and has checksum [{}], size [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), name, md.checksum(), md.length());
                                }
                            }
                        }
                        if (!useExisting) {
                            if (request.existingFiles().containsKey(name)) {
                                logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote [{}], local [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), name, request.existingFiles().get(name), md);
                            } else {
                                logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], does not exists in remote", request.shardId().index().name(), request.shardId().id(), request.targetNode(), name);
                            }
                            response.phase1FileNames.add(name);
                            response.phase1FileSizes.add(md.length());
                        }
                        totalSize += md.length();
                    }
                    response.phase1TotalSize = totalSize;
                    response.phase1ExistingTotalSize = existingTotalSize;

                    logger.trace("[{}][{}] recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), response.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));

                    RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(request.shardId(), response.phase1FileNames, response.phase1FileSizes,
                            response.phase1ExistingFileNames, response.phase1ExistingFileSizes, response.phase1TotalSize, response.phase1ExistingTotalSize);
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILES_INFO, recoveryInfoFilesRequest, VoidTransportResponseHandler.INSTANCE_SAME).txGet();

                    final CountDownLatch latch = new CountDownLatch(response.phase1FileNames.size());
                    final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
                    for (final String name : response.phase1FileNames) {
                        recoverySettings.concurrentStreamPool().execute(new Runnable() {
                            @Override
                            public void run() {
                                IndexInput indexInput = null;
                                try {
                                    final int BUFFER_SIZE = (int) recoverySettings.fileChunkSize().bytes();
                                    byte[] buf = new byte[BUFFER_SIZE];
                                    StoreFileMetaData md = shard.store().metaData(name);
                                    indexInput = shard.store().openInputRaw(name);
                                    boolean shouldCompressRequest = recoverySettings.compress();
                                    if (CompressorFactory.isCompressed(indexInput)) {
                                        shouldCompressRequest = false;
                                    }

                                    long len = indexInput.length();
                                    long readCount = 0;
                                    while (readCount < len) {
                                        if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                                            throw new IndexShardClosedException(shard.shardId());
                                        }
                                        int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                        long position = indexInput.getFilePointer();

                                        if (recoverySettings.rateLimiter() != null) {
                                            recoverySettings.rateLimiter().pause(toRead);
                                        }

                                        indexInput.readBytes(buf, 0, toRead, false);
                                        BytesArray content = new BytesArray(buf, 0, toRead);
                                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILE_CHUNK, new RecoveryFileChunkRequest(request.shardId(), name, position, len, md.checksum(), content),
                                                TransportRequestOptions.options().withCompress(shouldCompressRequest).withLowType(), VoidTransportResponseHandler.INSTANCE_SAME).txGet();
                                        readCount += toRead;
                                    }
                                    indexInput.close();
                                } catch (Exception e) {
                                    lastException.set(e);
                                } finally {
                                    if (indexInput != null) {
                                        try {
                                            indexInput.close();
                                        } catch (IOException e) {
                                            // ignore
                                        }
                                    }
                                    latch.countDown();
                                }
                            }
                        });
                    }

                    latch.await();

                    if (lastException.get() != null) {
                        throw lastException.get();
                    }

                    // now, set the clean files request
                    Set<String> snapshotFiles = Sets.newHashSet(snapshot.getFiles());
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.CLEAN_FILES, new RecoveryCleanFilesRequest(shard.shardId(), snapshotFiles), VoidTransportResponseHandler.INSTANCE_SAME).txGet();

                    stopWatch.stop();
                    logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());
                    response.phase1Time = stopWatch.totalTime().millis();
                } catch (Throwable e) {
                    throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
                }
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws ElasticSearchException {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                logger.trace("[{}][{}] recovery [phase2] to {}: start", request.shardId().index().name(), request.shardId().id(), request.targetNode());
                StopWatch stopWatch = new StopWatch().start();
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.PREPARE_TRANSLOG, new RecoveryPrepareForTranslogOperationsRequest(request.shardId()), VoidTransportResponseHandler.INSTANCE_SAME).txGet();
                stopWatch.stop();
                response.startTime = stopWatch.totalTime().millis();
                logger.trace("[{}][{}] recovery [phase2] to {}: start took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());

                logger.trace("[{}][{}] recovery [phase2] to {}: sending transaction log operations", request.shardId().index().name(), request.shardId().id(), request.targetNode());
                stopWatch = new StopWatch().start();
                int totalOperations = sendSnapshot(snapshot);
                stopWatch.stop();
                logger.trace("[{}][{}] recovery [phase2] to {}: took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());
                response.phase2Time = stopWatch.totalTime().millis();
                response.phase2Operations = totalOperations;
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws ElasticSearchException {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                logger.trace("[{}][{}] recovery [phase3] to {}: sending transaction log operations", request.shardId().index().name(), request.shardId().id(), request.targetNode());
                StopWatch stopWatch = new StopWatch().start();
                int totalOperations = sendSnapshot(snapshot);
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FINALIZE, new RecoveryFinalizeRecoveryRequest(request.shardId()), VoidTransportResponseHandler.INSTANCE_SAME).txGet();
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
                logger.trace("[{}][{}] recovery [phase3] to {}: took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());
                response.phase3Time = stopWatch.totalTime().millis();
                response.phase3Operations = totalOperations;
            }

            private int sendSnapshot(Translog.Snapshot snapshot) throws ElasticSearchException {
                int ops = 0;
                long size = 0;
                int totalOperations = 0;
                List<Translog.Operation> operations = Lists.newArrayList();
                while (snapshot.hasNext()) {
                    if (shard.state() == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(request.shardId());
                    }
                    Translog.Operation operation = snapshot.next();
                    operations.add(operation);
                    ops += 1;
                    size += operation.estimateSize();
                    totalOperations++;
                    if (ops >= recoverySettings.translogOps() || size >= recoverySettings.translogSize().bytes()) {

                        if (recoverySettings.rateLimiter() != null) {
                            recoverySettings.rateLimiter().pause(size);
                        }

                        RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(request.shardId(), operations);
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest, TransportRequestOptions.options().withCompress(recoverySettings.compress()).withLowType(), VoidTransportResponseHandler.INSTANCE_SAME).txGet();
                        ops = 0;
                        size = 0;
                        operations.clear();
                    }
                }
                // send the leftover
                if (!operations.isEmpty()) {
                    RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(request.shardId(), operations);
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest, TransportRequestOptions.options().withCompress(recoverySettings.compress()).withLowType(), VoidTransportResponseHandler.INSTANCE_SAME).txGet();
                }
                return totalOperations;
            }
        });
        return response;
    }

    class StartRecoveryTransportRequestHandler extends BaseTransportRequestHandler<StartRecoveryRequest> {

        @Override
        public StartRecoveryRequest newInstance() {
            return new StartRecoveryRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }
}

