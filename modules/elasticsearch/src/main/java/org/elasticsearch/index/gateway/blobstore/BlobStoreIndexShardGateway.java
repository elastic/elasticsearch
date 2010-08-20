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
import org.elasticsearch.common.Digest;
import org.elasticsearch.common.Hex;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.lucene.store.ThreadSafeInputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.gateway.*;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    protected final ConcurrentMap<String, String> cachedMd5 = ConcurrentCollections.newConcurrentMap();

    private volatile AppendableBlobContainer.AppendableBlob translogBlob;

    private volatile RecoveryStatus recoveryStatus;

    private volatile SnapshotStatus lastSnapshotStatus;

    private volatile SnapshotStatus currentSnapshotStatus;

    protected BlobStoreIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway indexGateway,
                                         IndexShard indexShard, Store store, RecoveryThrottler recoveryThrottler) {
        super(shardId, indexSettings);

        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;
        this.recoveryThrottler = recoveryThrottler;

        BlobStoreIndexGateway blobStoreIndexGateway = (BlobStoreIndexGateway) indexGateway;

        this.chunkSize = blobStoreIndexGateway.chunkSize(); // can be null -> no chunking
        this.blobStore = blobStoreIndexGateway.blobStore();
        this.shardPath = blobStoreIndexGateway.shardPath(shardId.id());

        this.indexContainer = blobStore.immutableBlobContainer(blobStoreIndexGateway.shardIndexPath(shardId.id()));
        this.translogContainer = blobStore.appendableBlobContainer(blobStoreIndexGateway.shardTranslogPath(shardId.id()));

        this.recoveryStatus = new RecoveryStatus();
    }

    @Override public RecoveryStatus recoveryStatus() {
        return this.recoveryStatus;
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

    @Override public SnapshotStatus lastSnapshotStatus() {
        return this.lastSnapshotStatus;
    }

    @Override public SnapshotStatus currentSnapshotStatus() {
        SnapshotStatus snapshotStatus = this.currentSnapshotStatus;
        if (snapshotStatus == null) {
            return snapshotStatus;
        }
        if (snapshotStatus.stage() != SnapshotStatus.Stage.DONE || snapshotStatus.stage() != SnapshotStatus.Stage.FAILURE) {
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
        }
        return snapshotStatus;
    }

    @Override public SnapshotStatus snapshot(final Snapshot snapshot) throws IndexShardGatewaySnapshotFailedException {
        currentSnapshotStatus = new SnapshotStatus();
        currentSnapshotStatus.startTime(System.currentTimeMillis());

        try {
            doSnapshot(snapshot);
            currentSnapshotStatus.time(System.currentTimeMillis() - currentSnapshotStatus.startTime());
            currentSnapshotStatus.updateStage(SnapshotStatus.Stage.DONE);
        } catch (Exception e) {
            currentSnapshotStatus.time(System.currentTimeMillis() - currentSnapshotStatus.startTime());
            currentSnapshotStatus.updateStage(SnapshotStatus.Stage.FAILURE);
            currentSnapshotStatus.failed(e);
            if (e instanceof IndexShardGatewaySnapshotFailedException) {
                throw (IndexShardGatewaySnapshotFailedException) e;
            } else {
                throw new IndexShardGatewaySnapshotFailedException(shardId, e.getMessage(), e);
            }
        } finally {
            this.lastSnapshotStatus = currentSnapshotStatus;
            this.currentSnapshotStatus = null;
        }
        return this.lastSnapshotStatus;
    }

    private void doSnapshot(final Snapshot snapshot) throws IndexShardGatewaySnapshotFailedException {
        currentSnapshotStatus.index().startTime(System.currentTimeMillis());
        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.INDEX);

        boolean indexDirty = false;

        final SnapshotIndexCommit snapshotIndexCommit = snapshot.indexCommit();
        final Translog.Snapshot translogSnapshot = snapshot.translogSnapshot();

        ImmutableMap<String, BlobMetaData> indicesBlobs = null;
        ImmutableMap<String, BlobMetaData> virtualIndicesBlobs = null;

        int indexNumberOfFiles = 0;
        long indexTotalFilesSize = 0;
        if (snapshot.indexChanged()) {
            long time = System.currentTimeMillis();
            indexDirty = true;

            try {
                indicesBlobs = indexContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to list indices files from gateway", e);
            }
            virtualIndicesBlobs = buildVirtualBlobs(indexContainer, indicesBlobs, cachedMd5);

            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

            for (final String fileName : snapshotIndexCommit.getFiles()) {
                StoreFileMetaData snapshotFileMetaData;
                try {
                    snapshotFileMetaData = store.metaDataWithMd5(fileName);
                } catch (IOException e) {
                    throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to get store file metadata", e);
                }
                // don't copy over the segments file, it will be copied over later on as part of the
                // final snapshot phase
                if (fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                    latch.countDown();
                    continue;
                }
                // if the file exists in the gateway, and has the same length, don't copy it over
                if (virtualIndicesBlobs.containsKey(fileName) && virtualIndicesBlobs.get(fileName).md5().equals(snapshotFileMetaData.md5())) {
                    latch.countDown();
                    continue;
                }

                // we are snapshotting the file

                indexNumberOfFiles++;
                indexTotalFilesSize += snapshotFileMetaData.sizeInBytes();

                if (virtualIndicesBlobs.containsKey(fileName)) {
                    try {
                        cachedMd5.remove(fileName);
                        indexContainer.deleteBlobsByPrefix(fileName);
                    } catch (IOException e) {
                        logger.debug("failed to delete [" + fileName + "] before snapshotting, ignoring...");
                    }
                }

                try {
                    snapshotFile(snapshotIndexCommit.getDirectory(), snapshotFileMetaData, latch, failures);
                } catch (IOException e) {
                    failures.add(e);
                    latch.countDown();
                }
            }

            currentSnapshotStatus.index().files(indexNumberOfFiles + 1 /* for the segment */, indexTotalFilesSize);

            try {
                latch.await();
            } catch (InterruptedException e) {
                failures.add(e);
            }
            if (!failures.isEmpty()) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", failures.get(failures.size() - 1));
            }
        }

        currentSnapshotStatus.index().time(System.currentTimeMillis() - currentSnapshotStatus.index().startTime());

        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.TRANSLOG);
        currentSnapshotStatus.translog().startTime(System.currentTimeMillis());

        if (snapshot.newTranslogCreated() || snapshot.sameTranslogNewOperations()) {
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
                    if (!snapshot.newTranslogCreated()) {
                        translogSnapshot.seekForward(snapshot.lastTranslogPosition());
                    }
                    BytesStreamOutput bout = CachedStreamOutput.cachedBytes();
                    while (translogSnapshot.hasNext()) {
                        bout.reset();

                        bout.writeInt(0);
                        TranslogStreams.writeTranslogOperation(bout, translogSnapshot.next());
                        bout.flush();

                        int size = bout.size();
                        bout.seek(0);
                        bout.writeInt(size - 4);

                        os.writeBytes(bout.unsafeByteArray(), size);
                        currentSnapshotStatus.translog().addTranslogOperations(1);
                    }
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

        }
        currentSnapshotStatus.translog().time(System.currentTimeMillis() - currentSnapshotStatus.translog().startTime());

        // now write the segments file
        if (indexDirty) {
            try {
                if (indicesBlobs.containsKey(snapshotIndexCommit.getSegmentsFileName())) {
                    cachedMd5.remove(snapshotIndexCommit.getSegmentsFileName());
                    indexContainer.deleteBlob(snapshotIndexCommit.getSegmentsFileName());
                }

                StoreFileMetaData snapshotFileMetaData = store.metaDataWithMd5(snapshotIndexCommit.getSegmentsFileName());
                indexTotalFilesSize += snapshotFileMetaData.sizeInBytes();

                long time = System.currentTimeMillis();
                CountDownLatch latch = new CountDownLatch(1);
                CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
                snapshotFile(snapshotIndexCommit.getDirectory(), snapshotFileMetaData, latch, failures);
                latch.await();
                if (!failures.isEmpty()) {
                    throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (segment index file)", failures.get(failures.size() - 1));
                }
            } catch (Exception e) {
                if (e instanceof IndexShardGatewaySnapshotFailedException) {
                    throw (IndexShardGatewaySnapshotFailedException) e;
                }
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize index snapshot into [" + snapshotIndexCommit.getSegmentsFileName() + "]", e);
            }
        }

        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.FINALIZE);

        // delete the old translog
        if (snapshot.newTranslogCreated()) {
            try {
                translogContainer.deleteBlobsByFilter(new BlobContainer.BlobNameFilter() {
                    @Override public boolean accept(String blobName) {
                        // delete all the ones that are not this translog
                        return !blobName.equals("translog-" + translogSnapshot.translogId());
                    }
                });
            } catch (Exception e) {
                // ignore
            }
            // NOT doing this one, the above allows us to clean the translog properly
//            try {
//                translogContainer.deleteBlob("translog-" + snapshot.lastTranslogId());
//            } catch (Exception e) {
//                // ignore
//            }
        }

        // delete old index files
        if (indexDirty) {
            for (BlobMetaData md : virtualIndicesBlobs.values()) {
                boolean found = false;
                for (final String fileName : snapshotIndexCommit.getFiles()) {
                    if (md.name().equals(fileName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    try {
                        cachedMd5.remove(md.name());
                        indexContainer.deleteBlobsByPrefix(md.name());
                    } catch (IOException e) {
                        logger.debug("failed to delete unused index files, will retry later...", e);
                    }
                }
            }
        }
    }

    @Override public void recover(RecoveryStatus recoveryStatus) throws IndexShardGatewayRecoveryException {
        this.recoveryStatus = recoveryStatus;

        recoveryStatus.index().startTime(System.currentTimeMillis());
        recoveryStatus.updateStage(RecoveryStatus.Stage.INDEX);
        recoverIndex();
        recoveryStatus.index().time(System.currentTimeMillis() - recoveryStatus.index().startTime());

        recoveryStatus.translog().startTime(System.currentTimeMillis());
        recoveryStatus.updateStage(RecoveryStatus.Stage.TRANSLOG);
        recoverTranslog();
        recoveryStatus.translog().time(System.currentTimeMillis() - recoveryStatus.index().startTime());
    }

    private void recoverTranslog() throws IndexShardGatewayRecoveryException {
        long translogId;
        try {
            translogId = IndexReader.getCurrentVersion(store.directory());
        } catch (FileNotFoundException e) {
            // no index, that fine
            indexShard.start();
            return;
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to recover translog, can't read current index version", e);
        }
        if (!translogContainer.blobExists("translog-" + translogId)) {
            // no recovery file found, start the shard and bail
            indexShard.start();
            return;
        }


        try {
            indexShard.performRecoveryPrepareForTranslog();

            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            final CountDownLatch latch = new CountDownLatch(1);

            translogContainer.readBlob("translog-" + translogId, new BlobContainer.ReadBlobListener() {
                FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
                boolean ignore = false;

                @Override public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                    if (ignore) {
                        return;
                    }
                    bos.write(data, offset, size);
                    // if we don't have enough to read the header size of the first translog, bail and wait for the next one
                    if (bos.size() < 4) {
                        return;
                    }
                    BytesStreamInput si = new BytesStreamInput(bos.unsafeByteArray(), 0, bos.size());
                    int position;
                    while (true) {
                        try {
                            position = si.position();
                            if (position + 4 > bos.size()) {
                                break;
                            }
                            int opSize = si.readInt();
                            int curPos = si.position();
                            if ((si.position() + opSize) > bos.size()) {
                                break;
                            }
                            Translog.Operation operation = TranslogStreams.readTranslogOperation(si);
                            if ((si.position() - curPos) != opSize) {
                                logger.warn("mismatch in size, expected [{}], got [{}]", opSize, si.position() - curPos);
                            }
                            recoveryStatus.translog().addTranslogOperations(1);
                            indexShard.performRecoveryOperation(operation);
                            if (si.position() >= bos.size()) {
                                position = si.position();
                                break;
                            }
                        } catch (Exception e) {
                            logger.warn("failed to retrieve translog after [{}] operations, ignoring the rest, considered corrupted", e, recoveryStatus.translog().currentTranslogOperations());
                            ignore = true;
                            latch.countDown();
                            return;
                        }
                    }

                    FastByteArrayOutputStream newBos = new FastByteArrayOutputStream();

                    int leftOver = bos.size() - position;
                    if (leftOver > 0) {
                        newBos.write(bos.unsafeByteArray(), position, leftOver);
                    }

                    bos = newBos;
                }

                @Override public synchronized void onCompleted() {
                    latch.countDown();
                }

                @Override public synchronized void onFailure(Throwable t) {
                    failure.set(t);
                    latch.countDown();
                }
            });

            latch.await();
            if (failure.get() != null) {
                throw failure.get();
            }

            indexShard.performRecoveryFinalization(true);
        } catch (Throwable e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to recover translog", e);
        }
    }

    private void recoverIndex() throws IndexShardGatewayRecoveryException {
        final ImmutableMap<String, BlobMetaData> indicesBlobs;
        try {
            indicesBlobs = indexContainer.listBlobs();
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to list content of gateway", e);
        }
        ImmutableMap<String, BlobMetaData> virtualIndicesBlobs = buildVirtualBlobs(indexContainer, indicesBlobs, cachedMd5);

        int numberOfFiles = 0;
        long totalSize = 0;
        int numberOfExistingFiles = 0;
        long existingTotalSize = 0;

        // filter out only the files that we need to recover, and reuse ones that exists in the store
        List<BlobMetaData> filesToRecover = new ArrayList<BlobMetaData>();
        for (BlobMetaData virtualMd : virtualIndicesBlobs.values()) {
            // if the store has the file, and it has the same length, don't recover it
            try {
                StoreFileMetaData storeMd = store.metaDataWithMd5(virtualMd.name());
                if (storeMd != null && storeMd.md5().equals(virtualMd.md5())) {
                    numberOfExistingFiles++;
                    existingTotalSize += virtualMd.sizeInBytes();
                    totalSize += virtualMd.sizeInBytes();
                    if (logger.isTraceEnabled()) {
                        logger.trace("not_recovering [{}], exists in local store and has same md5 [{}]", virtualMd.name(), virtualMd.md5());
                    }
                } else {
                    if (logger.isTraceEnabled()) {
                        if (storeMd == null) {
                            logger.trace("recovering [{}], does not exists in local store", virtualMd.name());
                        } else {
                            logger.trace("recovering [{}], exists in local store but has different md5: gateway [{}], local [{}]", virtualMd.name(), virtualMd.md5(), storeMd.md5());
                        }
                    }
                    numberOfFiles++;
                    totalSize += virtualMd.sizeInBytes();
                    filesToRecover.add(virtualMd);
                }
            } catch (Exception e) {
                filesToRecover.add(virtualMd);
                logger.debug("failed to check local store for existence of [{}]", e, virtualMd.name());
            }
        }

        recoveryStatus.index().files(numberOfFiles, totalSize, numberOfExistingFiles, existingTotalSize);

        if (logger.isTraceEnabled()) {
            logger.trace("recovering_files [{}] with total_size [{}], reusing_files [{}] with reused_size [{}]", numberOfFiles, new ByteSizeValue(totalSize), numberOfExistingFiles, new ByteSizeValue(existingTotalSize));
        }

        final CountDownLatch latch = new CountDownLatch(filesToRecover.size());
        final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
        for (final BlobMetaData fileToRecover : filesToRecover) {
            if (recoveryThrottler.tryStream(shardId, fileToRecover.name())) {
                // we managed to get a recovery going
                recoverFile(fileToRecover, indicesBlobs, latch, failures);
            } else {
                // lets reschedule to do it next time
                threadPool.schedule(new Runnable() {
                    @Override public void run() {
                        recoveryStatus.index().addRetryTime(recoveryThrottler.throttleInterval().millis());
                        if (recoveryThrottler.tryStream(shardId, fileToRecover.name())) {
                            // we managed to get a recovery going
                            recoverFile(fileToRecover, indicesBlobs, latch, failures);
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

        // read the gateway data persisted
        long version = -1;
        try {
            if (IndexReader.indexExists(store.directory())) {
                version = IndexReader.getCurrentVersion(store.directory());
            }
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to fetch index version after copying it over", e);
        }
        recoveryStatus.index().updateVersion(version);

        /// now, go over and clean files that are in the store, but were not in the gateway
        try {
            for (String storeFile : store.directory().listAll()) {
                if (!virtualIndicesBlobs.containsKey(storeFile)) {
                    try {
                        store.directory().deleteFile(storeFile);
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        } catch (IOException e) {
            // ignore
        }
    }

    private void recoverFile(final BlobMetaData fileToRecover, final ImmutableMap<String, BlobMetaData> blobs, final CountDownLatch latch, final List<Throwable> failures) {
        final IndexOutput indexOutput;
        try {
            indexOutput = store.directory().createOutput(fileToRecover.name());
        } catch (IOException e) {
            recoveryThrottler.streamDone(shardId, fileToRecover.name());
            failures.add(e);
            latch.countDown();
            return;
        }

        String firstFileToRecover = fileToRecover.name();
        if (!blobs.containsKey(fileToRecover.name())) {
            // chunking, append part0 to it
            firstFileToRecover = fileToRecover.name() + ".part0";
        }
        if (!blobs.containsKey(firstFileToRecover)) {
            // no file, what to do, what to do?
            recoveryThrottler.streamDone(shardId, fileToRecover.name());
            logger.warn("no file [{}] to recover, even though it has md5, ignoring it", fileToRecover.name());
            latch.countDown();
            return;
        }
        final AtomicInteger partIndex = new AtomicInteger();
        final MessageDigest digest = Digest.getMd5Digest();
        indexContainer.readBlob(firstFileToRecover, new BlobContainer.ReadBlobListener() {
            @Override public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                recoveryStatus.index().addCurrentFilesSize(size);
                indexOutput.writeBytes(data, offset, size);
                digest.update(data, offset, size);
            }

            @Override public synchronized void onCompleted() {
                int part = partIndex.incrementAndGet();
                String partName = fileToRecover.name() + ".part" + part;
                if (blobs.containsKey(partName)) {
                    // continue with the new part
                    indexContainer.readBlob(partName, this);
                    return;
                } else {
                    // we are done...
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        onFailure(e);
                        return;
                    }
                }
                // double check the md5, warn if it does not equal...
                String md5 = Hex.encodeHexString(digest.digest());
                if (!md5.equals(fileToRecover.md5())) {
                    logger.warn("file [{}] has different md5, actual read content [{}], store [{}]", fileToRecover.name(), md5, fileToRecover.md5());
                }

                recoveryThrottler.streamDone(shardId, fileToRecover.name());
                latch.countDown();
            }

            @Override public void onFailure(Throwable t) {
                recoveryThrottler.streamDone(shardId, fileToRecover.name());
                failures.add(t);
                latch.countDown();
            }
        });
    }

    private void snapshotFile(Directory dir, final StoreFileMetaData fileMetaData, final CountDownLatch latch, final List<Throwable> failures) throws IOException {
        long chunkBytes = Long.MAX_VALUE;
        if (chunkSize != null) {
            chunkBytes = chunkSize.bytes();
        }

        long totalLength = fileMetaData.sizeInBytes();
        long numberOfChunks = totalLength / chunkBytes;
        if (totalLength % chunkBytes > 0) {
            numberOfChunks++;
        }
        if (numberOfChunks == 0) {
            numberOfChunks++;
        }

        final long fNumberOfChunks = numberOfChunks;
        final AtomicLong counter = new AtomicLong(numberOfChunks);
        for (long i = 0; i < fNumberOfChunks; i++) {
            final long chunkNumber = i;

            IndexInput indexInput = null;
            try {
                indexInput = dir.openInput(fileMetaData.name());
                indexInput.seek(chunkNumber * chunkBytes);
                InputStreamIndexInput is = new ThreadSafeInputStreamIndexInput(indexInput, chunkBytes);

                String blobName = fileMetaData.name();
                if (fNumberOfChunks > 1) {
                    // if we do chunks, then all of them are in the form of "[xxx].part[N]".
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
                            // now, write the expected md5
                            byte[] md5 = Digest.md5HexToByteArray(fileMetaData.md5());
                            indexContainer.writeBlob(fileMetaData.name() + ".md5", new ByteArrayInputStream(md5), md5.length, new ImmutableBlobContainer.WriterListener() {
                                @Override public void onCompleted() {
                                    latch.countDown();
                                }

                                @Override public void onFailure(Throwable t) {
                                    failures.add(t);
                                    latch.countDown();
                                }
                            });
                        }
                    }

                    @Override public void onFailure(Throwable t) {
                        try {
                            fIndexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }
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

    public static ImmutableMap<String, BlobMetaData> buildVirtualBlobs(ImmutableBlobContainer container, ImmutableMap<String, BlobMetaData> blobs, @Nullable Map<String, String> cachedMd5) {
        // create a set of all the actual files based on .md5 extension
        Set<String> names = Sets.newHashSet();
        for (BlobMetaData blob : blobs.values()) {
            if (blob.name().endsWith(".md5")) {
                names.add(blob.name().substring(0, blob.name().lastIndexOf(".md5")));
            }
        }
        ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
        for (String name : names) {
            long sizeInBytes = 0;
            if (blobs.containsKey(name)) {
                // no chunking
                sizeInBytes = blobs.get(name).sizeInBytes();
            } else {
                // chunking...
                int part = 0;
                while (true) {
                    BlobMetaData md = blobs.get(name + ".part" + part);
                    if (md == null) {
                        break;
                    }
                    sizeInBytes += md.sizeInBytes();
                    part++;
                }
            }

            if (cachedMd5 != null && cachedMd5.containsKey(name)) {
                builder.put(name, new PlainBlobMetaData(name, sizeInBytes, cachedMd5.get(name)));
            } else {
                // no md5, get it
                try {
                    String md5 = Digest.md5HexFromByteArray(container.readBlobFully(name + ".md5"));
                    if (cachedMd5 != null) {
                        cachedMd5.put(name, md5);
                    }
                    builder.put(name, new PlainBlobMetaData(name, sizeInBytes, md5));
                } catch (Exception e) {
                    // don't add it!
                }
            }
        }
        return builder.build();
    }
}
