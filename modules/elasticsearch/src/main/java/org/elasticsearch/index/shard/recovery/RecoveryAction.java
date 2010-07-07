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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.*;

/**
 * @author kimchy (shay.banon)
 */
public class RecoveryAction extends AbstractIndexShardComponent implements CloseableComponent {

    private final ByteSizeValue fileChunkSize;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final InternalIndexShard indexShard;

    private final Store store;

    private final RecoveryThrottler recoveryThrottler;

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = newConcurrentMap();

    private final String startTransportAction;

    private final String fileChunkTransportAction;

    private final String cleanFilesTransportAction;

    private final String prepareForTranslogOperationsTransportAction;

    private final String translogOperationsTransportAction;

    private final String finalizeRecoveryTransportAction;

    private volatile boolean closed = false;

    private volatile Thread sendStartRecoveryThread;

    private volatile Thread receiveSnapshotRecoveryThread;

    private volatile Thread sendSnapshotRecoveryThread;

    private final CopyOnWriteArrayList<Future> sendFileChunksRecoveryFutures = new CopyOnWriteArrayList<Future>();

    @Inject public RecoveryAction(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, TransportService transportService,
                                  IndexShard indexShard, Store store, RecoveryThrottler recoveryThrottler) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;
        this.recoveryThrottler = recoveryThrottler;

        startTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/start";
        transportService.registerHandler(startTransportAction, new StartRecoveryTransportRequestHandler());

        fileChunkTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/fileChunk";
        transportService.registerHandler(fileChunkTransportAction, new FileChunkTransportRequestHandler());

        cleanFilesTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/cleanFiles";
        transportService.registerHandler(cleanFilesTransportAction, new CleanFilesRequestHandler());

        prepareForTranslogOperationsTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/prepareForTranslog";
        transportService.registerHandler(prepareForTranslogOperationsTransportAction, new PrepareForTranslogOperationsRequestHandler());

        translogOperationsTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/translogOperations";
        transportService.registerHandler(translogOperationsTransportAction, new TranslogOperationsRequestHandler());

        finalizeRecoveryTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/finalizeRecovery";
        transportService.registerHandler(finalizeRecoveryTransportAction, new FinalizeRecoveryRequestHandler());

        this.fileChunkSize = componentSettings.getAsBytesSize("file_chunk_size", new ByteSizeValue(100, ByteSizeUnit.KB));
        logger.trace("recovery action registered, using file_chunk_size[{}]", fileChunkSize);
    }

    public void close() {
        closed = true;
        transportService.removeHandler(startTransportAction);
        transportService.removeHandler(fileChunkTransportAction);
        transportService.removeHandler(cleanFilesTransportAction);
        transportService.removeHandler(prepareForTranslogOperationsTransportAction);
        transportService.removeHandler(translogOperationsTransportAction);
        transportService.removeHandler(finalizeRecoveryTransportAction);

        cleanOpenIndex();

        // interrupt the startRecovery thread if its performing recovery
        if (sendStartRecoveryThread != null) {
            sendStartRecoveryThread.interrupt();
        }
        if (receiveSnapshotRecoveryThread != null) {
            receiveSnapshotRecoveryThread.interrupt();
        }
        if (sendSnapshotRecoveryThread != null) {
            sendSnapshotRecoveryThread.interrupt();
        }
        for (Future future : sendFileChunksRecoveryFutures) {
            future.cancel(true);
        }
    }

    public synchronized void startRecovery(DiscoveryNode node, DiscoveryNode targetNode, boolean markAsRelocated) throws ElasticSearchException {
        if (targetNode == null) {
            throw new IgnoreRecoveryException("No node to recovery from, retry next time...");
        }
        sendStartRecoveryThread = Thread.currentThread();
        try {
            // mark the shard as recovering
            IndexShardState preRecoveringState;
            try {
                preRecoveringState = indexShard.recovering();
            } catch (IndexShardRecoveringException e) {
                // that's fine, since we might be called concurrently, just ignore this, we are already recovering
                throw new IgnoreRecoveryException("Already in recovering process", e);
            } catch (IndexShardStartedException e) {
                // that's fine, since we might be called concurrently, just ignore this, we are already started
                throw new IgnoreRecoveryException("Already in recovering process", e);
            } catch (IndexShardRelocatedException e) {
                // that's fine, since we might be called concurrently, just ignore this, we are already relocated
                throw new IgnoreRecoveryException("Already in recovering process", e);
            } catch (IndexShardClosedException e) {
                throw new IgnoreRecoveryException("Can't recover a closed shard.", e);
            }

            // we know we are on a thread, we can spin till we can engage in recovery
            StopWatch throttlingWaitTime = new StopWatch().start();
            while (!recoveryThrottler.tryRecovery(shardId, "peer recovery target")) {
                try {
                    Thread.sleep(recoveryThrottler.throttleInterval().millis());
                } catch (InterruptedException e) {
                    if (indexShard.ignoreRecoveryAttempt()) {
                        throw new IgnoreRecoveryException("Interrupted while waiting for recovery, but we should ignore ...");
                    }
                    // we got interrupted, mark it as failed
                    throw new RecoveryFailedException(shardId, node, targetNode, e);
                }
            }
            throttlingWaitTime.stop();

            try {
                if (closed) {
                    throw new IgnoreRecoveryException("Recovery closed");
                }

                logger.debug("starting recovery from {}", targetNode);
                // build a list of the current files located locally, maybe we don't need to recover them...
                StartRecoveryRequest startRecoveryRequest = new StartRecoveryRequest(node, markAsRelocated, store.listWithMd5());

                StopWatch stopWatch = null;
                RecoveryStatus recoveryStatus = null;
                boolean retry = true;
                while (retry) {
                    stopWatch = new StopWatch().start();
                    recoveryStatus = transportService.submitRequest(targetNode, startTransportAction, startRecoveryRequest, new FutureTransportResponseHandler<RecoveryStatus>() {
                        @Override public RecoveryStatus newInstance() {
                            return new RecoveryStatus();
                        }
                    }).txGet();
                    retry = recoveryStatus.retry;
                    if (retry) {
                        try {
                            Thread.sleep(recoveryThrottler.throttleInterval().millis());
                        } catch (InterruptedException e) {
                            if (indexShard.ignoreRecoveryAttempt()) {
                                throw new IgnoreRecoveryException("Interrupted while waiting for remote recovery, but we should ignore ...");
                            }
                            // we got interrupted, mark it as failed
                            throw new RecoveryFailedException(shardId, node, targetNode, e);
                        }
                    }
                }
                stopWatch.stop();
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("recovery completed from ").append(targetNode).append(", took[").append(stopWatch.totalTime()).append("], throttling_wait [").append(throttlingWaitTime.totalTime()).append("]\n");
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
            } catch (RemoteTransportException e) {
                if (closed) {
                    throw new IgnoreRecoveryException("Recovery closed", e);
                }
                logger.trace("recovery from [{}] failed", e, targetNode);
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ActionNotFoundTransportException || cause instanceof IndexShardNotStartedException) {
                    // the remote shard has not yet registered the action or not started yet, we need to ignore this recovery attempt, and restore the state previous to recovering
                    indexShard.restoreRecoveryState(preRecoveringState);
                    throw new IgnoreRecoveryException("Ignoring recovery attempt, remote shard not started", e);
                } else if (cause instanceof RecoveryEngineException) {
                    // it might be wrapped
                    if (cause.getCause() instanceof IgnoreRecoveryException) {
                        throw (IgnoreRecoveryException) cause.getCause();
                    }
                } else if (cause instanceof IgnoreRecoveryException) {
                    throw (IgnoreRecoveryException) cause;
                } else if (cause instanceof NodeNotConnectedException) {
                    throw new IgnoreRecoveryException("Ignore recovery attemot, remote node not connected", e);
                }
                throw new RecoveryFailedException(shardId, node, targetNode, e);
            } catch (Exception e) {
                if (closed) {
                    throw new IgnoreRecoveryException("Recovery closed", e);
                }
                throw new RecoveryFailedException(shardId, node, targetNode, e);
            } finally {
                recoveryThrottler.recoveryDone(shardId, "peer recovery target");
            }
        } finally {
            sendStartRecoveryThread = null;
        }
    }

    private void cleanOpenIndex() {
        for (IndexOutput indexOutput : openIndexOutputs.values()) {
            try {
                synchronized (indexOutput) {
                    indexOutput.close();
                }
            } catch (Exception e) {
                // ignore
            }
        }
        openIndexOutputs.clear();
    }

    static class StartRecoveryRequest implements Streamable {

        DiscoveryNode node;

        boolean markAsRelocated;

        // name -> (md5, size)
        Map<String, StoreFileMetaData> existingFiles;

        private StartRecoveryRequest() {
        }

        private StartRecoveryRequest(DiscoveryNode node, boolean markAsRelocated, Map<String, StoreFileMetaData> existingFiles) {
            this.node = node;
            this.markAsRelocated = markAsRelocated;
            this.existingFiles = existingFiles;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            node = DiscoveryNode.readNode(in);
            markAsRelocated = in.readBoolean();
            int size = in.readVInt();
            existingFiles = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                StoreFileMetaData md = StoreFileMetaData.readStoreFileMetaData(in);
                existingFiles.put(md.name(), md);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            out.writeBoolean(markAsRelocated);
            out.writeVInt(existingFiles.size());
            for (StoreFileMetaData md : existingFiles.values()) {
                md.writeTo(out);
            }
        }
    }

    private class StartRecoveryTransportRequestHandler extends BaseTransportRequestHandler<StartRecoveryRequest> {

        @Override public StartRecoveryRequest newInstance() {
            return new StartRecoveryRequest();
        }

        @Override public void messageReceived(final StartRecoveryRequest startRecoveryRequest, final TransportChannel channel) throws Exception {
            if (!recoveryThrottler.tryRecovery(shardId, "peer recovery source")) {
                RecoveryStatus retry = new RecoveryStatus();
                retry.retry = true;
                channel.sendResponse(retry);
                return;
            }
            try {
                logger.trace("starting recovery to {}, mark_as_relocated {}", startRecoveryRequest.node, startRecoveryRequest.markAsRelocated);
                final DiscoveryNode node = startRecoveryRequest.node;
                cleanOpenIndex();
                final RecoveryStatus recoveryStatus = new RecoveryStatus();
                indexShard.recover(new Engine.RecoveryHandler() {
                    @Override public void phase1(final SnapshotIndexCommit snapshot) throws ElasticSearchException {
                        long totalSize = 0;
                        long existingTotalSize = 0;
                        try {
                            StopWatch stopWatch = new StopWatch().start();

                            for (String name : snapshot.getFiles()) {
                                StoreFileMetaData md = store.metaDataWithMd5(name);
                                boolean useExisting = false;
                                if (startRecoveryRequest.existingFiles.containsKey(name)) {
                                    if (md.md5().equals(startRecoveryRequest.existingFiles.get(name).md5())) {
                                        recoveryStatus.phase1ExistingFileNames.add(name);
                                        recoveryStatus.phase1ExistingFileSizes.add(md.sizeInBytes());
                                        existingTotalSize += md.sizeInBytes();
                                        useExisting = true;
                                        if (logger.isTraceEnabled()) {
                                            logger.trace("recovery [phase1] to {}: not recovering [{}], exists in local store and has md5 [{}]", node, name, md.md5());
                                        }
                                    }
                                }
                                if (!useExisting) {
                                    if (startRecoveryRequest.existingFiles.containsKey(name)) {
                                        logger.trace("recovery [phase1] to {}: recovering [{}], exists in local store, but has different md5: remote [{}], local [{}]", node, name, startRecoveryRequest.existingFiles.get(name).md5(), md.md5());
                                    } else {
                                        logger.trace("recovery [phase1] to {}: recovering [{}], does not exists in remote", node, name);
                                    }
                                    recoveryStatus.phase1FileNames.add(name);
                                    recoveryStatus.phase1FileSizes.add(md.sizeInBytes());
                                    totalSize += md.sizeInBytes();
                                }
                            }
                            recoveryStatus.phase1TotalSize = totalSize;
                            recoveryStatus.phase1ExistingTotalSize = existingTotalSize;

                            final AtomicLong throttlingWaitTime = new AtomicLong();

                            logger.trace("recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]", node, recoveryStatus.phase1FileNames.size(), new ByteSizeValue(totalSize), recoveryStatus.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));

                            final CountDownLatch latch = new CountDownLatch(recoveryStatus.phase1FileNames.size());
                            final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
                            for (final String name : recoveryStatus.phase1FileNames) {
                                sendFileChunksRecoveryFutures.add(threadPool.submit(new Runnable() {
                                    @Override public void run() {
                                        IndexInput indexInput = null;
                                        try {
                                            long throttlingStartTime = System.currentTimeMillis();
                                            while (!recoveryThrottler.tryStream(shardId, name)) {
                                                Thread.sleep(recoveryThrottler.throttleInterval().millis());
                                            }
                                            throttlingWaitTime.addAndGet(System.currentTimeMillis() - throttlingStartTime);

                                            final int BUFFER_SIZE = (int) fileChunkSize.bytes();
                                            byte[] buf = new byte[BUFFER_SIZE];
                                            indexInput = snapshot.getDirectory().openInput(name);
                                            long len = indexInput.length();
                                            long readCount = 0;
                                            while (readCount < len) {
                                                int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                                long position = indexInput.getFilePointer();
                                                indexInput.readBytes(buf, 0, toRead, false);
                                                transportService.submitRequest(node, fileChunkTransportAction, new FileChunk(name, position, len, buf, toRead), VoidTransportResponseHandler.INSTANCE).txGet(120, SECONDS);
                                                readCount += toRead;
                                            }
                                            indexInput.close();
                                        } catch (Exception e) {
                                            lastException.set(e);
                                        } finally {
                                            recoveryThrottler.streamDone(shardId, name);
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
                                }));
                            }

                            latch.await();

                            if (lastException.get() != null) {
                                throw lastException.get();
                            }

                            // now, set the clean files request
                            CleanFilesRequest cleanFilesRequest = new CleanFilesRequest();
                            cleanFilesRequest.snapshotFiles.addAll(Arrays.asList(snapshot.getFiles()));
                            transportService.submitRequest(node, cleanFilesTransportAction, cleanFilesRequest, VoidTransportResponseHandler.INSTANCE).txGet();

                            stopWatch.stop();
                            logger.trace("recovery [phase1] to {}: took [{}], throttling_wait [{}]", node, stopWatch.totalTime(), timeValueMillis(throttlingWaitTime.get()));
                            recoveryStatus.phase1Time = stopWatch.totalTime().millis();
                        } catch (ElasticSearchInterruptedException e) {
                            // we got interrupted since we are closing, ignore the recovery
                            throw new IgnoreRecoveryException("Interrupted while recovering files");
                        } catch (Throwable e) {
                            throw new RecoverFilesRecoveryException(shardId, recoveryStatus.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
                        } finally {
                            sendFileChunksRecoveryFutures.clear();
                        }
                    }

                    @Override public void phase2(Translog.Snapshot snapshot) throws ElasticSearchException {
                        sendSnapshotRecoveryThread = Thread.currentThread();
                        try {
                            if (closed) {
                                throw new IndexShardClosedException(shardId);
                            }
                            logger.trace("recovery [phase2] to {}: sending transaction log operations", node);
                            StopWatch stopWatch = new StopWatch().start();

                            transportService.submitRequest(node, prepareForTranslogOperationsTransportAction, VoidStreamable.INSTANCE, VoidTransportResponseHandler.INSTANCE).txGet();

                            int totalOperations = sendSnapshot(snapshot);

                            stopWatch.stop();
                            logger.trace("recovery [phase2] to {}: took [{}]", node, stopWatch.totalTime());
                            recoveryStatus.phase2Time = stopWatch.totalTime().millis();
                            recoveryStatus.phase2Operations = totalOperations;
                        } catch (ElasticSearchInterruptedException e) {
                            // we got interrupted since we are closing, ignore the recovery
                            throw new IgnoreRecoveryException("Interrupted in phase 2 files");
                        } finally {
                            sendSnapshotRecoveryThread = null;
                        }
                    }

                    @Override public void phase3(Translog.Snapshot snapshot) throws ElasticSearchException {
                        sendSnapshotRecoveryThread = Thread.currentThread();
                        try {
                            if (closed) {
                                throw new IndexShardClosedException(shardId);
                            }
                            logger.trace("recovery [phase3] to {}: sending transaction log operations", node);
                            StopWatch stopWatch = new StopWatch().start();
                            int totalOperations = sendSnapshot(snapshot);
                            transportService.submitRequest(node, finalizeRecoveryTransportAction, VoidStreamable.INSTANCE, VoidTransportResponseHandler.INSTANCE).txGet();
                            if (startRecoveryRequest.markAsRelocated) {
                                // TODO what happens if the recovery process fails afterwards, we need to mark this back to started
                                try {
                                    indexShard.relocated();
                                } catch (IllegalIndexShardStateException e) {
                                    // we can ignore this exception since, on the other node, when it moved to phase3
                                    // it will also send shard started, which might cause the index shard we work against
                                    // to move be closed by the time we get to the the relocated method
                                }
                            }
                            stopWatch.stop();
                            logger.trace("recovery [phase3] to {}: took [{}]", node, stopWatch.totalTime());
                            recoveryStatus.phase3Time = stopWatch.totalTime().millis();
                            recoveryStatus.phase3Operations = totalOperations;
                        } catch (ElasticSearchInterruptedException e) {
                            // we got interrupted since we are closing, ignore the recovery
                            throw new IgnoreRecoveryException("Interrupted in phase 2 files");
                        } finally {
                            sendSnapshotRecoveryThread = null;
                        }
                    }

                    private int sendSnapshot(Translog.Snapshot snapshot) throws ElasticSearchException {
                        TranslogOperationsRequest request = new TranslogOperationsRequest();
                        int translogBatchSize = 10; // TODO make this configurable
                        int counter = 0;
                        int totalOperations = 0;
                        while (snapshot.hasNext()) {
                            request.operations.add(snapshot.next());
                            totalOperations++;
                            if (++counter == translogBatchSize) {
                                transportService.submitRequest(node, translogOperationsTransportAction, request, VoidTransportResponseHandler.INSTANCE).txGet();
                                counter = 0;
                                request.operations.clear();
                            }
                        }
                        // send the leftover
                        if (!request.operations.isEmpty()) {
                            transportService.submitRequest(node, translogOperationsTransportAction, request, VoidTransportResponseHandler.INSTANCE).txGet();
                        }
                        return totalOperations;
                    }
                });
                channel.sendResponse(recoveryStatus);
            } finally {
                recoveryThrottler.recoveryDone(shardId, "peer recovery source");
            }
        }
    }

    private static class RecoveryStatus implements Streamable {

        boolean retry = false;
        List<String> phase1FileNames = Lists.newArrayList();
        List<Long> phase1FileSizes = Lists.newArrayList();
        List<String> phase1ExistingFileNames = Lists.newArrayList();
        List<Long> phase1ExistingFileSizes = Lists.newArrayList();
        long phase1TotalSize;
        long phase1ExistingTotalSize;
        long phase1Time;
        long phase1ThrottlingWaitTime;

        int phase2Operations;
        long phase2Time;

        int phase3Operations;
        long phase3Time;

        private RecoveryStatus() {
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            retry = in.readBoolean();
            int size = in.readVInt();
            phase1FileNames = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                phase1FileNames.add(in.readUTF());
            }
            size = in.readVInt();
            phase1FileSizes = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                phase1FileSizes.add(in.readVLong());
            }

            size = in.readVInt();
            phase1ExistingFileNames = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                phase1ExistingFileNames.add(in.readUTF());
            }
            size = in.readVInt();
            phase1ExistingFileSizes = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                phase1ExistingFileSizes.add(in.readVLong());
            }

            phase1TotalSize = in.readVLong();
            phase1ExistingTotalSize = in.readVLong();
            phase1Time = in.readVLong();
            phase1ThrottlingWaitTime = in.readVLong();
            phase2Operations = in.readVInt();
            phase2Time = in.readVLong();
            phase3Operations = in.readVInt();
            phase3Time = in.readVLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(retry);
            out.writeVInt(phase1FileNames.size());
            for (String name : phase1FileNames) {
                out.writeUTF(name);
            }
            out.writeVInt(phase1FileSizes.size());
            for (long size : phase1FileSizes) {
                out.writeVLong(size);
            }

            out.writeVInt(phase1ExistingFileNames.size());
            for (String name : phase1ExistingFileNames) {
                out.writeUTF(name);
            }
            out.writeVInt(phase1ExistingFileSizes.size());
            for (long size : phase1ExistingFileSizes) {
                out.writeVLong(size);
            }

            out.writeVLong(phase1TotalSize);
            out.writeVLong(phase1ExistingTotalSize);
            out.writeVLong(phase1Time);
            out.writeVLong(phase1ThrottlingWaitTime);
            out.writeVInt(phase2Operations);
            out.writeVLong(phase2Time);
            out.writeVInt(phase3Operations);
            out.writeVLong(phase3Time);
        }
    }

    static class CleanFilesRequest implements Streamable {

        Set<String> snapshotFiles = Sets.newHashSet();

        @Override public void readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                snapshotFiles.add(in.readUTF());
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(snapshotFiles.size());
            for (String snapshotFile : snapshotFiles) {
                out.writeUTF(snapshotFile);
            }
        }
    }

    class CleanFilesRequestHandler extends BaseTransportRequestHandler<CleanFilesRequest> {

        @Override public CleanFilesRequest newInstance() {
            return new CleanFilesRequest();
        }

        @Override public void messageReceived(CleanFilesRequest request, TransportChannel channel) throws Exception {
            receiveSnapshotRecoveryThread = Thread.currentThread();
            try {
                for (String existingFile : store.directory().listAll()) {
                    if (!request.snapshotFiles.contains(existingFile)) {
                        store.directory().deleteFile(existingFile);
                    }
                }
                channel.sendResponse(VoidStreamable.INSTANCE);
            } finally {
                receiveSnapshotRecoveryThread = null;
            }
        }
    }


    class PrepareForTranslogOperationsRequestHandler extends BaseTransportRequestHandler<VoidStreamable> {

        @Override public VoidStreamable newInstance() {
            return VoidStreamable.INSTANCE;
        }

        @Override public void messageReceived(VoidStreamable stream, TransportChannel channel) throws Exception {
            receiveSnapshotRecoveryThread = Thread.currentThread();
            try {
                indexShard.performRecoveryPrepareForTranslog();
                channel.sendResponse(VoidStreamable.INSTANCE);
            } finally {
                receiveSnapshotRecoveryThread = null;
            }
        }
    }

    class FinalizeRecoveryRequestHandler extends BaseTransportRequestHandler<VoidStreamable> {

        @Override public VoidStreamable newInstance() {
            return VoidStreamable.INSTANCE;
        }

        @Override public void messageReceived(VoidStreamable stream, TransportChannel channel) throws Exception {
            receiveSnapshotRecoveryThread = Thread.currentThread();
            try {
                indexShard.performRecoveryFinalization();
                channel.sendResponse(VoidStreamable.INSTANCE);
            } finally {
                receiveSnapshotRecoveryThread = null;
            }
        }
    }

    class TranslogOperationsRequestHandler extends BaseTransportRequestHandler<TranslogOperationsRequest> {

        @Override public TranslogOperationsRequest newInstance() {
            return new TranslogOperationsRequest();
        }

        @Override public void messageReceived(TranslogOperationsRequest snapshot, TransportChannel channel) throws Exception {
            receiveSnapshotRecoveryThread = Thread.currentThread();
            try {
                if (closed) {
                    throw new IndexShardClosedException(shardId);
                }
                for (Translog.Operation operation : snapshot.operations) {
                    indexShard.performRecoveryOperation(operation);
                }
                channel.sendResponse(VoidStreamable.INSTANCE);
            } finally {
                receiveSnapshotRecoveryThread = null;
            }
        }
    }

    static class TranslogOperationsRequest implements Streamable {

        List<Translog.Operation> operations = Lists.newArrayList();

        TranslogOperationsRequest() {
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                operations.add(TranslogStreams.readTranslogOperation(in));
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(operations.size());
            for (Translog.Operation operation : operations) {
                TranslogStreams.writeTranslogOperation(out, operation);
            }
        }
    }

    private class FileChunkTransportRequestHandler extends BaseTransportRequestHandler<FileChunk> {

        @Override public FileChunk newInstance() {
            return new FileChunk();
        }

        @Override public void messageReceived(FileChunk request, TransportChannel channel) throws Exception {
            if (closed) {
                throw new IndexShardClosedException(shardId);
            }
            IndexOutput indexOutput;
            if (request.position == 0) {
                // first request
                indexOutput = openIndexOutputs.remove(request.name);
                if (indexOutput != null) {
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
                indexOutput = store.directory().createOutput(request.name);
                openIndexOutputs.put(request.name, indexOutput);
            } else {
                indexOutput = openIndexOutputs.get(request.name);
            }
            synchronized (indexOutput) {
                try {
                    indexOutput.writeBytes(request.content, request.content.length);
                    if (indexOutput.getFilePointer() == request.length) {
                        // we are done
                        indexOutput.close();
                        openIndexOutputs.remove(request.name);
                    }
                } catch (IOException e) {
                    openIndexOutputs.remove(request.name);
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

    private static class FileChunk implements Streamable {
        String name;
        long position;
        long length;
        byte[] content;

        transient int contentLength;

        private FileChunk() {
        }

        private FileChunk(String name, long position, long length, byte[] content, int contentLength) {
            this.name = name;
            this.position = position;
            this.length = length;
            this.content = content;
            this.contentLength = contentLength;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            position = in.readVLong();
            length = in.readVLong();
            content = new byte[in.readVInt()];
            in.readFully(content);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeVLong(position);
            out.writeVLong(length);
            out.writeVInt(contentLength);
            out.writeBytes(content, 0, contentLength);
        }
    }
}
