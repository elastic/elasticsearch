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

import com.google.inject.Inject;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.memory.MemorySnapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.io.VoidStreamable;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;
import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RecoveryAction extends AbstractIndexShardComponent {

    private final SizeValue fileChunkSize;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final InternalIndexShard indexShard;

    private final Store store;

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = newConcurrentMap();

    private final String startTransportAction;

    private final String fileChunkTransportAction;

    private final String snapshotTransportAction;

    @Inject public RecoveryAction(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, TransportService transportService, IndexShard indexShard, Store store) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;

        startTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/start";
        transportService.registerHandler(startTransportAction, new StartRecoveryTransportRequestHandler());
        fileChunkTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/fileChunk";
        transportService.registerHandler(fileChunkTransportAction, new FileChunkTransportRequestHandler());
        snapshotTransportAction = shardId.index().name() + "/" + shardId.id() + "/recovery/snapshot";
        transportService.registerHandler(snapshotTransportAction, new SnapshotTransportRequestHandler());

        this.fileChunkSize = componentSettings.getAsSize("fileChunkSize", new SizeValue(16, SizeUnit.KB));
        logger.trace("Recovery Action registered, using fileChunkSize[{}]", fileChunkSize);
    }

    public void close() {
        transportService.removeHandler(startTransportAction);
        transportService.removeHandler(fileChunkTransportAction);
        transportService.removeHandler(snapshotTransportAction);
    }

    public synchronized void startRecovery(Node node, Node targetNode, boolean markAsRelocated) throws ElasticSearchException {
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
            throw new IgnoreRecoveryException("can't recover a closed shard.", e);
        }
        logger.debug("Starting recovery from {}", targetNode);
        StopWatch stopWatch = new StopWatch().start();
        try {
            RecoveryStatus recoveryStatus = transportService.submitRequest(targetNode, startTransportAction, new StartRecoveryRequest(node, markAsRelocated), new FutureTransportResponseHandler<RecoveryStatus>() {
                @Override public RecoveryStatus newInstance() {
                    return new RecoveryStatus();
                }
            }).txGet();
            stopWatch.stop();
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Recovery completed from ").append(targetNode).append(", took [").append(stopWatch.totalTime()).append("]\n");
                sb.append("   Phase1: recovered [").append(recoveryStatus.phase1FileNames.size()).append("]")
                        .append(" files with total size of [").append(new SizeValue(recoveryStatus.phase1TotalSize)).append("]")
                        .append(", took [").append(new TimeValue(recoveryStatus.phase1Time, MILLISECONDS)).append("]")
                        .append("\n");
                sb.append("   Phase2: recovered [").append(recoveryStatus.phase2Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(new TimeValue(recoveryStatus.phase2Time, MILLISECONDS)).append("]")
                        .append("\n");
                sb.append("   Phase3: recovered [").append(recoveryStatus.phase3Operations).append("]").append(" transaction log operations")
                        .append(", took [").append(new TimeValue(recoveryStatus.phase3Time, MILLISECONDS)).append("]");
                logger.debug(sb.toString());
            }
        } catch (RemoteTransportException e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof ActionNotFoundTransportException ||
                    cause instanceof IndexShardNotStartedException) {
                // the remote shard has not yet registered the action or not started yet, we need to ignore this recovery attempt, and restore the state previous to recovering
                indexShard.restoreRecoveryState(preRecoveringState);
                throw new IgnoreRecoveryException("Ignoring recovery attempt, remote shard not started", e);
            }
            throw new RecoveryFailedException(shardId, node, targetNode, e);
        } catch (Exception e) {
            throw new RecoveryFailedException(shardId, node, targetNode, e);
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

    private static class StartRecoveryRequest implements Streamable {

        private Node node;

        private boolean markAsRelocated;

        private StartRecoveryRequest() {
        }

        private StartRecoveryRequest(Node node, boolean markAsRelocated) {
            this.node = node;
            this.markAsRelocated = markAsRelocated;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            node = Node.readNode(in);
            markAsRelocated = in.readBoolean();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            node.writeTo(out);
            out.writeBoolean(markAsRelocated);
        }
    }

    private class StartRecoveryTransportRequestHandler extends BaseTransportRequestHandler<StartRecoveryRequest> {

        @Override public StartRecoveryRequest newInstance() {
            return new StartRecoveryRequest();
        }

        @Override public void messageReceived(final StartRecoveryRequest startRecoveryRequest, final TransportChannel channel) throws Exception {
            logger.trace("Starting recovery to {}, markAsRelocated {}", startRecoveryRequest.node, startRecoveryRequest.markAsRelocated);
            final Node node = startRecoveryRequest.node;
            cleanOpenIndex();
            final RecoveryStatus recoveryStatus = new RecoveryStatus();
            indexShard.recover(new Engine.RecoveryHandler() {
                @Override public void phase1(SnapshotIndexCommit snapshot) throws ElasticSearchException {
                    long totalSize = 0;
                    try {
                        StopWatch stopWatch = new StopWatch().start();

                        for (String name : snapshot.getFiles()) {
                            IndexInput indexInput = store.directory().openInput(name);
                            recoveryStatus.phase1FileNames.add(name);
                            recoveryStatus.phase1FileSizes.add(indexInput.length());
                            totalSize += indexInput.length();
                            indexInput.close();
                        }
                        recoveryStatus.phase1TotalSize = totalSize;

                        logger.trace("Recovery [phase1] to {}: recovering [{}] files with total size of [{}]", new Object[]{node, snapshot.getFiles().length, new SizeValue(totalSize)});

                        final CountDownLatch latch = new CountDownLatch(snapshot.getFiles().length);
                        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
                        for (final String name : snapshot.getFiles()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    try {
                                        final int BUFFER_SIZE = (int) fileChunkSize.bytes();
                                        byte[] buf = new byte[BUFFER_SIZE];
                                        IndexInput indexInput = store.directory().openInput(name);
                                        long len = indexInput.length();
                                        long readCount = 0;
                                        while (readCount < len) {
                                            int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                            long position = indexInput.getFilePointer();
                                            indexInput.readBytes(buf, 0, toRead, false);
                                            transportService.submitRequest(node, fileChunkTransportAction, new FileChunk(name, position, len, buf, toRead), VoidTransportResponseHandler.INSTANCE).txGet(30, SECONDS);
                                            readCount += toRead;
                                        }
                                        indexInput.close();
                                    } catch (Exception e) {
                                        lastException.set(e);
                                    } finally {
                                        latch.countDown();
                                    }
                                }
                            });
                        }

                        latch.await();

                        if (lastException.get() != null) {
                            throw lastException.get();
                        }

                        stopWatch.stop();
                        logger.trace("Recovery [phase1] to {}: took [{}]", node, stopWatch.totalTime());
                        recoveryStatus.phase1Time = stopWatch.totalTime().millis();
                    } catch (Throwable e) {
                        throw new RecoverFilesRecoveryException(shardId, snapshot.getFiles().length, new SizeValue(totalSize), e);
                    }
                }

                @Override public void phase2(Translog.Snapshot snapshot) throws ElasticSearchException {
                    logger.trace("Recovery [phase2] to {}: sending [{}] transaction log operations", node, snapshot.size());
                    StopWatch stopWatch = new StopWatch().start();
                    sendSnapshot(snapshot, false);
                    stopWatch.stop();
                    logger.trace("Recovery [phase2] to {}: took [{}]", node, stopWatch.totalTime());
                    recoveryStatus.phase2Time = stopWatch.totalTime().millis();
                    recoveryStatus.phase2Operations = snapshot.size();
                }

                @Override public void phase3(Translog.Snapshot snapshot) throws ElasticSearchException {
                    logger.trace("Recovery [phase3] to {}: sending [{}] transaction log operations", node, snapshot.size());
                    StopWatch stopWatch = new StopWatch().start();
                    sendSnapshot(snapshot, true);
                    if (startRecoveryRequest.markAsRelocated) {
                        // TODO what happens if the recovery process fails afterwards, we need to mark this back to started
                        indexShard.relocated();
                    }
                    stopWatch.stop();
                    logger.trace("Recovery [phase3] to {}: took [{}]", node, stopWatch.totalTime());
                    recoveryStatus.phase3Time = stopWatch.totalTime().millis();
                    recoveryStatus.phase3Operations = snapshot.size();
                }

                private void sendSnapshot(Translog.Snapshot snapshot, boolean phase3) throws ElasticSearchException {
                    MemorySnapshot memorySnapshot;
                    if (snapshot instanceof MemorySnapshot) {
                        memorySnapshot = (MemorySnapshot) snapshot;
                    } else {
                        memorySnapshot = new MemorySnapshot(snapshot);
                    }
                    transportService.submitRequest(node, snapshotTransportAction, new SnapshotWrapper(memorySnapshot, phase3), VoidTransportResponseHandler.INSTANCE).txGet();
                }
            });
            channel.sendResponse(recoveryStatus);
        }
    }

    private static class RecoveryStatus implements Streamable {

        List<String> phase1FileNames = new ArrayList<String>();
        List<Long> phase1FileSizes = new ArrayList<Long>();
        long phase1TotalSize;
        long phase1Time;

        int phase2Operations;
        long phase2Time;

        int phase3Operations;
        long phase3Time;

        private RecoveryStatus() {
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size = in.readInt();
            phase1FileNames = new ArrayList<String>(size);
            for (int i = 0; i < size; i++) {
                phase1FileNames.add(in.readUTF());
            }
            size = in.readInt();
            phase1FileSizes = new ArrayList<Long>(size);
            for (int i = 0; i < size; i++) {
                phase1FileSizes.add(in.readLong());
            }
            phase1TotalSize = in.readLong();
            phase1Time = in.readLong();
            phase2Operations = in.readInt();
            phase2Time = in.readLong();
            phase3Operations = in.readInt();
            phase3Time = in.readLong();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeInt(phase1FileNames.size());
            for (String name : phase1FileNames) {
                out.writeUTF(name);
            }
            out.writeInt(phase1FileSizes.size());
            for (long size : phase1FileSizes) {
                out.writeLong(size);
            }
            out.writeLong(phase1TotalSize);
            out.writeLong(phase1Time);
            out.writeInt(phase2Operations);
            out.writeLong(phase2Time);
            out.writeInt(phase3Operations);
            out.writeLong(phase3Time);
        }
    }

    private class SnapshotTransportRequestHandler extends BaseTransportRequestHandler<SnapshotWrapper> {

        @Override public SnapshotWrapper newInstance() {
            return new SnapshotWrapper();
        }

        @Override public void messageReceived(SnapshotWrapper snapshot, TransportChannel channel) throws Exception {
            if (!snapshot.phase3) {
                // clean open index outputs in any case (there should not be any open, we close then in the chunk)
                cleanOpenIndex();
            }
            indexShard.performRecovery(snapshot.snapshot, snapshot.phase3);
            if (snapshot.phase3) {
                indexShard.refresh(true);
                // probably need to do more here...
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private static class SnapshotWrapper implements Streamable {

        private MemorySnapshot snapshot;

        private boolean phase3;

        private SnapshotWrapper() {
        }

        private SnapshotWrapper(MemorySnapshot snapshot, boolean phase3) {
            this.snapshot = snapshot;
            this.phase3 = phase3;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            snapshot = new MemorySnapshot();
            snapshot.readFrom(in);
            phase3 = in.readBoolean();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            snapshot.writeTo(out);
            out.writeBoolean(phase3);
        }
    }

    private class FileChunkTransportRequestHandler extends BaseTransportRequestHandler<FileChunk> {

        @Override public FileChunk newInstance() {
            return new FileChunk();
        }

        @Override public void messageReceived(FileChunk request, TransportChannel channel) throws Exception {
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
                indexOutput.writeBytes(request.content, request.content.length);
                if (indexOutput.getFilePointer() == request.length) {
                    // we are done
                    indexOutput.close();
                    openIndexOutputs.remove(request.name);
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

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            name = in.readUTF();
            position = in.readLong();
            length = in.readLong();
            content = new byte[in.readInt()];
            in.readFully(content);
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(name);
            out.writeLong(position);
            out.writeLong(length);
            out.writeInt(contentLength);
            out.write(content, 0, contentLength);
        }
    }
}
