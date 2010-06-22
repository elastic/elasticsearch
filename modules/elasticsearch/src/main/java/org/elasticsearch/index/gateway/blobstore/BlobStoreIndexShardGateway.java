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

package org.elasticsearch.index.gateway.blobstore;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.lucene.store.ThreadSafeInputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.TObjectLongHashMap;
import org.elasticsearch.common.trove.TObjectLongIterator;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewaySnapshotFailedException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.translog.TranslogStreams.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class BlobStoreIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    protected final ThreadPool threadPool;

    protected final InternalIndexShard indexShard;

    protected final Store store;

    protected final RecoveryThrottler recoveryThrottler;


    protected final ByteSizeValue chunkSize;

    protected final BlobStore blobStore;

    protected final BlobPath shardPath;

    protected final ImmutableBlobContainer indexContainer;

    protected final AppendableBlobContainer translogContainer;

    private volatile AppendableBlobContainer.AppendableBlob translogBlob;

    protected BlobStoreIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway indexGateway,
                                         IndexShard indexShard, Store store, RecoveryThrottler recoveryThrottler) {
        super(shardId, indexSettings);

        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;
        this.recoveryThrottler = recoveryThrottler;

        this.chunkSize = ((BlobStoreIndexGateway) indexGateway).chunkSize(); // can be null -> no chunking
        this.blobStore = ((BlobStoreIndexGateway) indexGateway).blobStore();
        this.shardPath = ((BlobStoreIndexGateway) indexGateway).indexPath().add(Integer.toString(shardId.id()));

        this.indexContainer = blobStore.immutableBlobContainer(shardPath.add("index"));
        this.translogContainer = blobStore.appendableBlobContainer(shardPath.add("translog"));
    }

    @Override public String toString() {
        return type() + "://" + blobStore + "/" + shardPath;
    }

    @Override public boolean requiresSnapshotScheduling() {
        return true;
    }

    @Override public void close(boolean delete) throws ElasticSearchException {
        if (translogBlob != null) {
            translogBlob.close();
            translogBlob = null;
        }
        if (delete) {
            blobStore.delete(shardPath);
        }
    }

    @Override public SnapshotStatus snapshot(final Snapshot snapshot) throws IndexShardGatewaySnapshotFailedException {
        long totalTimeStart = System.currentTimeMillis();
        boolean indexDirty = false;

        final SnapshotIndexCommit snapshotIndexCommit = snapshot.indexCommit();
        final Translog.Snapshot translogSnapshot = snapshot.translogSnapshot();

        ImmutableMap<String, BlobMetaData> indicesBlobs = null;
        TObjectLongHashMap<String> combinedIndicesBlobs = null;

        int indexNumberOfFiles = 0;
        long indexTotalFilesSize = 0;
        long indexTime = 0;
        if (snapshot.indexChanged()) {
            long time = System.currentTimeMillis();
            indexDirty = true;

            try {
                indicesBlobs = indexContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to list indices files from gateway", e);
            }
            combinedIndicesBlobs = buildCombinedPartsBlobs(indicesBlobs);

            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

            for (final String fileName : snapshotIndexCommit.getFiles()) {
                // don't copy over the segments file, it will be copied over later on as part of the
                // final snapshot phase
                if (fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                    latch.countDown();
                    continue;
                }
                // if the file exists in the gateway, and has the same length, don't copy it over
                long fileSize;
                try {
                    fileSize = snapshotIndexCommit.getDirectory().fileLength(fileName);
                } catch (IOException e) {
                    throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to get length on local store", e);
                }
                if (combinedIndicesBlobs.contains(fileName) && combinedIndicesBlobs.get(fileName) == fileSize) {
                    latch.countDown();
                    continue;
                }

                // we are snapshotting the file

                indexNumberOfFiles++;
                indexTotalFilesSize += fileSize;

                if (combinedIndicesBlobs.contains(fileName)) {
                    try {
                        indexContainer.deleteBlobsByPrefix(fileName);
                    } catch (IOException e) {
                        logger.debug("failed to delete [" + fileName + "] before snapshotting, ignoring...");
                    }
                }

                try {
                    snapshotFile(snapshotIndexCommit.getDirectory(), fileName, latch, failures);
                } catch (IOException e) {
                    failures.add(e);
                    latch.countDown();
                }
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                failures.add(e);
            }
            if (!failures.isEmpty()) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", failures.get(failures.size() - 1));
            }
            indexTime = System.currentTimeMillis() - time;
        }


        // handle if snapshot has changed
        final AtomicInteger translogNumberOfOperations = new AtomicInteger();
        long translogTime = 0;

        if (snapshot.newTranslogCreated() || snapshot.sameTranslogNewOperations()) {
            long time = System.currentTimeMillis();

            if (snapshot.newTranslogCreated() && translogBlob != null) {
                translogBlob.close();
                translogBlob = null;
            }

            if (translogBlob == null) {
                try {
                    translogBlob = translogContainer.appendBlob("translog-" + translogSnapshot.translogId());
                } catch (IOException e) {
                    throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to create translog", e);
                }
            }

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            translogBlob.append(new AppendableBlobContainer.AppendBlobListener() {
                @Override public void withStream(StreamOutput os) throws IOException {
                    int deltaNumberOfOperations;
                    Iterable<Translog.Operation> operationsIt;
                    if (snapshot.newTranslogCreated()) {
                        deltaNumberOfOperations = translogSnapshot.size();
                        operationsIt = translogSnapshot;
                    } else {
                        deltaNumberOfOperations = translogSnapshot.size() - snapshot.lastTranslogSize();
                        operationsIt = translogSnapshot.skipTo(snapshot.lastTranslogSize());
                    }
                    os.writeInt(deltaNumberOfOperations);
                    for (Translog.Operation operation : operationsIt) {
                        writeTranslogOperation(os, operation);
                    }
                    translogNumberOfOperations.set(deltaNumberOfOperations);
                }

                @Override public void onCompleted() {
                    latch.countDown();
                }

                @Override public void onFailure(Throwable t) {
                    failure.set(t);
                    latch.countDown();
                }
            });

            try {
                latch.await();
            } catch (InterruptedException e) {
                failure.set(e);
            }

            if (failure.get() != null) {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to snapshot translog", failure.get());
            }

            translogTime = System.currentTimeMillis() - time;
        }

        // now write the segments file
        if (indexDirty) {
            try {
                indexNumberOfFiles++;
                if (indicesBlobs.containsKey(snapshotIndexCommit.getSegmentsFileName())) {
                    indexContainer.deleteBlob(snapshotIndexCommit.getSegmentsFileName());
                }
                indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(snapshotIndexCommit.getSegmentsFileName());
                long time = System.currentTimeMillis();

                IndexInput indexInput = snapshotIndexCommit.getDirectory().openInput(snapshotIndexCommit.getSegmentsFileName());
                try {
                    InputStreamIndexInput is = new InputStreamIndexInput(indexInput, Long.MAX_VALUE);
                    indexContainer.writeBlob(snapshotIndexCommit.getSegmentsFileName(), is, is.actualSizeToRead());
                } finally {
                    try {
                        indexInput.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                indexTime += (System.currentTimeMillis() - time);
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize index snapshot into [" + snapshotIndexCommit.getSegmentsFileName() + "]", e);
            }
        }

        // delete the old translog
        if (snapshot.newTranslogCreated()) {
            try {
                translogContainer.deleteBlob("translog-" + snapshot.lastTranslogId());
            } catch (IOException e) {
                // ignore
            }
        }

        // delete old index files
        if (indexDirty) {
            for (TObjectLongIterator<String> it = combinedIndicesBlobs.iterator(); it.hasNext();) {
                it.advance();
                boolean found = false;
                for (final String fileName : snapshotIndexCommit.getFiles()) {
                    if (it.key().equals(fileName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    try {
                        indexContainer.deleteBlobsByPrefix(it.key());
                    } catch (IOException e) {
                        logger.debug("failed to delete unused index files, will retry later...", e);
                    }
                }
            }
        }

        return new SnapshotStatus(new TimeValue(System.currentTimeMillis() - totalTimeStart),
                new SnapshotStatus.Index(indexNumberOfFiles, new ByteSizeValue(indexTotalFilesSize), new TimeValue(indexTime)),
                new SnapshotStatus.Translog(translogNumberOfOperations.get(), new TimeValue(translogTime)));
    }

    @Override public RecoveryStatus recover() throws IndexShardGatewayRecoveryException {
        RecoveryStatus.Index recoveryStatusIndex = recoverIndex();
        RecoveryStatus.Translog recoveryStatusTranslog = recoverTranslog();
        return new RecoveryStatus(recoveryStatusIndex, recoveryStatusTranslog);
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        final ImmutableMap<String, BlobMetaData> blobs;
        try {
            blobs = translogContainer.listBlobsByPrefix("translog-");
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to list content of gateway", e);
        }

        List<Long> translogIds = Lists.newArrayList();
        for (BlobMetaData blob : blobs.values()) {
            long translogId = Long.parseLong(blob.name().substring(blob.name().indexOf('-') + 1));
            translogIds.add(translogId);
        }

        if (translogIds.isEmpty()) {
            // no recovery file found, start the shard and bail
            indexShard.start();
            return new RecoveryStatus.Translog(-1, 0, new ByteSizeValue(0, ByteSizeUnit.BYTES));
        }

        Collections.sort(translogIds, new Comparator<Long>() {
            @Override public int compare(Long o1, Long o2) {
                return (int) (o2 - o1);
            }
        });

        // try and recover from the latest translog id down to the first
        Exception lastException = null;
        for (Long translogId : translogIds) {
            try {
                ArrayList<Translog.Operation> operations = Lists.newArrayList();
                byte[] translogData = translogContainer.readBlobFully("translog-" + translogId);
                BytesStreamInput si = new BytesStreamInput(translogData);
                while (true) {
                    // we recover them in parts, each part container the number of operations, and then the list of them
                    int numberOfOperations = si.readInt();
                    for (int i = 0; i < numberOfOperations; i++) {
                        operations.add(readTranslogOperation(si));
                    }
                    if (si.position() == translogData.length) {
                        // we have reached the end of the stream, bail
                        break;
                    }
                }
                indexShard.performRecovery(operations);

                // clean all the other translogs
                for (Long translogIdToDelete : translogIds) {
                    if (!translogId.equals(translogIdToDelete)) {
                        try {
                            translogContainer.deleteBlob("translog-" + translogIdToDelete);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }

                // only if we can append to an existing translog we should use the current id and continue to append to it
                long lastTranslogId = indexShard.translog().currentId();
                if (!translogContainer.canAppendToExistingBlob()) {
                    lastTranslogId = -1;
                }

                return new RecoveryStatus.Translog(lastTranslogId, operations.size(), new ByteSizeValue(translogData.length, ByteSizeUnit.BYTES));
            } catch (Exception e) {
                lastException = e;
                logger.debug("Failed to read translog, will try the next one", e);
            }
        }
        throw new IndexShardGatewayRecoveryException(shardId, "Failed to recovery translog", lastException);
    }

    private RecoveryStatus.Index recoverIndex() throws IndexShardGatewayRecoveryException {
        final ImmutableMap<String, BlobMetaData> blobs;
        try {
            blobs = indexContainer.listBlobs();
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to list content of gateway", e);
        }
        TObjectLongHashMap<String> combinedBlobs = buildCombinedPartsBlobs(blobs);

        // filter out only the files that we need to recover, and reuse ones that exists in the store
        List<String> filesToRecover = new ArrayList<String>();
        for (TObjectLongIterator<String> it = combinedBlobs.iterator(); it.hasNext();) {
            it.advance();
            // if the store has the file, and it has the same length, don't recover it
            try {
                if (store.directory().fileExists(it.key()) && store.directory().fileLength(it.key()) == it.value()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("not recovering [{}], exists in local store and has same size [{}]", it.key(), new ByteSizeValue(it.value()));
                    }
                } else {
                    filesToRecover.add(it.key());
                }
            } catch (Exception e) {
                filesToRecover.add(it.key());
                logger.debug("failed to check local store for existence of [{}]", it.key());
            }
        }

        long totalSize = 0;
        final AtomicLong throttlingWaitTime = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(filesToRecover.size());
        final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
        for (final String fileToRecover : filesToRecover) {
            totalSize += combinedBlobs.get(fileToRecover);
            if (recoveryThrottler.tryStream(shardId, fileToRecover)) {
                // we managed to get a recovery going
                recoverFile(fileToRecover, blobs, latch, failures);
            } else {
                // lets reschedule to do it next time
                threadPool.schedule(new Runnable() {
                    @Override public void run() {
                        throttlingWaitTime.addAndGet(recoveryThrottler.throttleInterval().millis());
                        if (recoveryThrottler.tryStream(shardId, fileToRecover)) {
                            // we managed to get a recovery going
                            recoverFile(fileToRecover, blobs, latch, failures);
                        } else {
                            threadPool.schedule(this, recoveryThrottler.throttleInterval());
                        }
                    }
                }, recoveryThrottler.throttleInterval());
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Interrupted while recovering index", e);
        }

        if (!failures.isEmpty()) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to recovery index", failures.get(0));
        }

        long version = -1;
        try {
            if (IndexReader.indexExists(store.directory())) {
                version = IndexReader.getCurrentVersion(store.directory());
            }
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to fetch index version after copying it over", e);
        }

        return new RecoveryStatus.Index(version, filesToRecover.size(), new ByteSizeValue(totalSize, ByteSizeUnit.BYTES), TimeValue.timeValueMillis(throttlingWaitTime.get()));
    }

    private void recoverFile(final String fileToRecover, final ImmutableMap<String, BlobMetaData> blobs, final CountDownLatch latch, final List<Throwable> failures) {
        final IndexOutput indexOutput;
        try {
            indexOutput = store.directory().createOutput(fileToRecover);
        } catch (IOException e) {
            recoveryThrottler.streamDone(shardId, fileToRecover);
            failures.add(e);
            latch.countDown();
            return;
        }
        final AtomicInteger partIndex = new AtomicInteger();
        indexContainer.readBlob(fileToRecover, new BlobContainer.ReadBlobListener() {
            @Override public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                indexOutput.writeBytes(data, offset, size);
            }

            @Override public synchronized void onCompleted() {
                int part = partIndex.incrementAndGet();
                String partName = fileToRecover + ".part" + part;
                if (blobs.containsKey(partName)) {
                    // continue with the new part
                    indexContainer.readBlob(partName, this);
                } else {
                    // we are done...
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }
                recoveryThrottler.streamDone(shardId, fileToRecover);
                latch.countDown();
            }

            @Override public void onFailure(Throwable t) {
                recoveryThrottler.streamDone(shardId, fileToRecover);
                failures.add(t);
                latch.countDown();
            }
        });
    }

    private void snapshotFile(Directory dir, String fileName, final CountDownLatch latch, final List<Throwable> failures) throws IOException {
        long chunkBytes = Long.MAX_VALUE;
        if (chunkSize != null) {
            chunkBytes = chunkSize.bytes();
        }

        long totalLength = dir.fileLength(fileName);
        long numberOfChunks = totalLength / chunkBytes;
        if (totalLength % chunkBytes > 0) {
            numberOfChunks++;
        }
        if (numberOfChunks == 0) {
            numberOfChunks++;
        }

        final AtomicLong counter = new AtomicLong(numberOfChunks);
        for (long i = 0; i < numberOfChunks; i++) {
            final long chunkNumber = i;

            IndexInput indexInput = null;
            try {
                indexInput = dir.openInput(fileName);
                indexInput.seek(chunkNumber * chunkBytes);
                InputStreamIndexInput is = new ThreadSafeInputStreamIndexInput(indexInput, chunkBytes);

                String blobName = fileName;
                if (chunkNumber > 0) {
                    blobName += ".part" + chunkNumber;
                }

                final IndexInput fIndexInput = indexInput;
                indexContainer.writeBlob(blobName, is, is.actualSizeToRead(), new ImmutableBlobContainer.WriterListener() {
                    @Override public void onCompleted() {
                        try {
                            fIndexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }
                        if (counter.decrementAndGet() == 0) {
                            latch.countDown();
                        }
                    }

                    @Override public void onFailure(Throwable t) {
                        failures.add(t);
                        if (counter.decrementAndGet() == 0) {
                            latch.countDown();
                        }
                    }
                });
            } catch (Exception e) {
                if (indexInput != null) {
                    try {
                        indexInput.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                }
                failures.add(e);
                latch.countDown();
            }
        }
    }

    private TObjectLongHashMap<String> buildCombinedPartsBlobs(ImmutableMap<String, BlobMetaData> blobs) {
        TObjectLongHashMap<String> combinedBlobs = new TObjectLongHashMap<String>();
        for (BlobMetaData blob : blobs.values()) {
            String cleanName;
            int partIndex = blob.name().indexOf(".part");
            if (partIndex == -1) {
                cleanName = blob.name();
            } else {
                cleanName = blob.name().substring(0, partIndex);
            }
            combinedBlobs.adjustOrPutValue(cleanName, blob.sizeInBytes(), blob.sizeInBytes());
        }
        return combinedBlobs;
    }
}
