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

package org.elasticsearch.index.gateway.blobstore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.io.FastByteArrayInputStream;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.lucene.store.ThreadSafeInputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public abstract class BlobStoreIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    protected final ThreadPool threadPool;

    protected final InternalIndexShard indexShard;

    protected final Store store;

    protected final ByteSizeValue chunkSize;

    protected final BlobStore blobStore;

    protected final BlobPath shardPath;

    protected final ImmutableBlobContainer blobContainer;

    private volatile RecoveryStatus recoveryStatus;

    private volatile SnapshotStatus lastSnapshotStatus;

    private volatile SnapshotStatus currentSnapshotStatus;

    protected BlobStoreIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway indexGateway,
                                         IndexShard indexShard, Store store) {
        super(shardId, indexSettings);

        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.store = store;

        BlobStoreIndexGateway blobStoreIndexGateway = (BlobStoreIndexGateway) indexGateway;

        this.chunkSize = blobStoreIndexGateway.chunkSize(); // can be null -> no chunking
        this.blobStore = blobStoreIndexGateway.blobStore();
        this.shardPath = blobStoreIndexGateway.shardPath(shardId.id());

        this.blobContainer = blobStore.immutableBlobContainer(shardPath);

        this.recoveryStatus = new RecoveryStatus();
    }

    @Override
    public RecoveryStatus recoveryStatus() {
        return this.recoveryStatus;
    }

    @Override
    public String toString() {
        return type() + "://" + blobStore + "/" + shardPath;
    }

    @Override
    public boolean requiresSnapshot() {
        return true;
    }

    @Override
    public boolean requiresSnapshotScheduling() {
        return true;
    }

    @Override
    public SnapshotLock obtainSnapshotLock() throws Exception {
        return NO_SNAPSHOT_LOCK;
    }

    @Override
    public void close(boolean delete) throws ElasticSearchException {
        if (delete) {
            blobStore.delete(shardPath);
        }
    }

    @Override
    public SnapshotStatus lastSnapshotStatus() {
        return this.lastSnapshotStatus;
    }

    @Override
    public SnapshotStatus currentSnapshotStatus() {
        SnapshotStatus snapshotStatus = this.currentSnapshotStatus;
        if (snapshotStatus == null) {
            return snapshotStatus;
        }
        if (snapshotStatus.stage() != SnapshotStatus.Stage.DONE || snapshotStatus.stage() != SnapshotStatus.Stage.FAILURE) {
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
        }
        return snapshotStatus;
    }

    @Override
    public SnapshotStatus snapshot(final Snapshot snapshot) throws IndexShardGatewaySnapshotFailedException {
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
        ImmutableMap<String, BlobMetaData> blobs;
        try {
            blobs = blobContainer.listBlobs();
        } catch (IOException e) {
            throw new IndexShardGatewaySnapshotFailedException(shardId, "failed to list blobs", e);
        }

        long generation = findLatestFileNameGeneration(blobs);
        CommitPoints commitPoints = buildCommitPoints(blobs);

        currentSnapshotStatus.index().startTime(System.currentTimeMillis());
        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.INDEX);

        final SnapshotIndexCommit snapshotIndexCommit = snapshot.indexCommit();
        final Translog.Snapshot translogSnapshot = snapshot.translogSnapshot();

        final CountDownLatch indexLatch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
        final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
        final List<CommitPoint.FileInfo> indexCommitPointFiles = Lists.newArrayList();

        int indexNumberOfFiles = 0;
        long indexTotalFilesSize = 0;
        for (final String fileName : snapshotIndexCommit.getFiles()) {
            StoreFileMetaData md;
            try {
                md = store.metaData(fileName);
            } catch (IOException e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to get store file metadata", e);
            }

            boolean snapshotRequired = false;
            if (snapshot.indexChanged() && fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                snapshotRequired = true; // we want to always snapshot the segment file if the index changed
            }

            CommitPoint.FileInfo fileInfo = commitPoints.findPhysicalIndexFile(fileName);
            if (fileInfo == null || !fileInfo.isSame(md) || !commitPointFileExistsInBlobs(fileInfo, blobs)) {
                // commit point file does not exists in any commit point, or has different length, or does not fully exists in the listed blobs
                snapshotRequired = true;
            }

            if (snapshotRequired) {
                indexNumberOfFiles++;
                indexTotalFilesSize += md.length();
                // create a new FileInfo
                try {
                    CommitPoint.FileInfo snapshotFileInfo = new CommitPoint.FileInfo(fileNameFromGeneration(++generation), fileName, md.length(), md.checksum());
                    indexCommitPointFiles.add(snapshotFileInfo);
                    snapshotFile(snapshotIndexCommit.getDirectory(), snapshotFileInfo, indexLatch, failures);
                } catch (IOException e) {
                    failures.add(e);
                    indexLatch.countDown();
                }
            } else {
                indexCommitPointFiles.add(fileInfo);
                indexLatch.countDown();
            }
        }
        currentSnapshotStatus.index().files(indexNumberOfFiles, indexTotalFilesSize);

        try {
            indexLatch.await();
        } catch (InterruptedException e) {
            failures.add(e);
        }
        if (!failures.isEmpty()) {
            throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", failures.get(failures.size() - 1));
        }

        currentSnapshotStatus.index().time(System.currentTimeMillis() - currentSnapshotStatus.index().startTime());

        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.TRANSLOG);
        currentSnapshotStatus.translog().startTime(System.currentTimeMillis());

        // Note, we assume the snapshot is always started from "base 0". We need to seek forward if we want to lastTranslogPosition if we want the delta
        List<CommitPoint.FileInfo> translogCommitPointFiles = Lists.newArrayList();
        int expectedNumberOfOperations = 0;
        boolean snapshotRequired = false;
        if (snapshot.newTranslogCreated()) {
            if (translogSnapshot.lengthInBytes() > 0) {
                snapshotRequired = true;
                expectedNumberOfOperations = translogSnapshot.estimatedTotalOperations();
            }
        } else {
            // if we have a commit point, check that we have all the files listed in it in the blob store
            if (!commitPoints.commits().isEmpty()) {
                CommitPoint commitPoint = commitPoints.commits().get(0);
                boolean allTranslogFilesExists = true;
                for (CommitPoint.FileInfo fileInfo : commitPoint.translogFiles()) {
                    if (!commitPointFileExistsInBlobs(fileInfo, blobs)) {
                        allTranslogFilesExists = false;
                        break;
                    }
                }
                // if everything exists, we can seek forward in case there are new operations, otherwise, we copy over all again...
                if (allTranslogFilesExists) {
                    translogCommitPointFiles.addAll(commitPoint.translogFiles());
                    if (snapshot.sameTranslogNewOperations()) {
                        translogSnapshot.seekForward(snapshot.lastTranslogLength());
                        if (translogSnapshot.lengthInBytes() > 0) {
                            snapshotRequired = true;
                            expectedNumberOfOperations = translogSnapshot.estimatedTotalOperations() - snapshot.lastTotalTranslogOperations();
                        }
                    } // else (no operations, nothing to snapshot)
                } else {
                    // a full translog snapshot is required
                    if (translogSnapshot.lengthInBytes() > 0) {
                        expectedNumberOfOperations = translogSnapshot.estimatedTotalOperations();
                        snapshotRequired = true;
                    }
                }
            } else {
                // no commit point, snapshot all the translog
                if (translogSnapshot.lengthInBytes() > 0) {
                    expectedNumberOfOperations = translogSnapshot.estimatedTotalOperations();
                    snapshotRequired = true;
                }
            }
        }
        currentSnapshotStatus.translog().expectedNumberOfOperations(expectedNumberOfOperations);

        if (snapshotRequired) {
            CommitPoint.FileInfo addedTranslogFileInfo = new CommitPoint.FileInfo(fileNameFromGeneration(++generation), "translog-" + translogSnapshot.translogId(), translogSnapshot.lengthInBytes(), null /* no need for checksum in translog */);
            translogCommitPointFiles.add(addedTranslogFileInfo);
            try {
                snapshotTranslog(translogSnapshot, addedTranslogFileInfo);
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to snapshot translog", e);
            }
        }
        currentSnapshotStatus.translog().time(System.currentTimeMillis() - currentSnapshotStatus.translog().startTime());

        // now create and write the commit point
        currentSnapshotStatus.updateStage(SnapshotStatus.Stage.FINALIZE);
        long version = 0;
        if (!commitPoints.commits().isEmpty()) {
            version = commitPoints.commits().iterator().next().version() + 1;
        }
        String commitPointName = "commit-" + Long.toString(version, Character.MAX_RADIX);
        CommitPoint commitPoint = new CommitPoint(version, commitPointName, CommitPoint.Type.GENERATED, indexCommitPointFiles, translogCommitPointFiles);
        try {
            byte[] commitPointData = CommitPoints.toXContent(commitPoint);
            blobContainer.writeBlob(commitPointName, new FastByteArrayInputStream(commitPointData), commitPointData.length);
        } catch (Exception e) {
            throw new IndexShardGatewaySnapshotFailedException(shardId, "Failed to write commit point", e);
        }

        // delete all files that are not referenced by any commit point
        // build a new CommitPoint, that includes this one and all the saved ones
        List<CommitPoint> newCommitPointsList = Lists.newArrayList();
        newCommitPointsList.add(commitPoint);
        for (CommitPoint point : commitPoints) {
            if (point.type() == CommitPoint.Type.SAVED) {
                newCommitPointsList.add(point);
            }
        }
        CommitPoints newCommitPoints = new CommitPoints(newCommitPointsList);
        // first, go over and delete all the commit points
        for (String blobName : blobs.keySet()) {
            if (!blobName.startsWith("commit-")) {
                continue;
            }
            long checkedVersion = Long.parseLong(blobName.substring("commit-".length()), Character.MAX_RADIX);
            if (!newCommitPoints.hasVersion(checkedVersion)) {
                try {
                    blobContainer.deleteBlob(blobName);
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        // now go over all the blobs, and if they don't exists in a commit point, delete them
        for (String blobName : blobs.keySet()) {
            String name = blobName;
            if (!name.startsWith("__")) {
                continue;
            }
            if (blobName.contains(".part")) {
                name = blobName.substring(0, blobName.indexOf(".part"));
            }
            if (newCommitPoints.findNameFile(name) == null) {
                try {
                    blobContainer.deleteBlob(blobName);
                } catch (IOException e) {
                    // ignore, will delete it laters
                }
            }
        }
    }

    @Override
    public void recover(boolean indexShouldExists, RecoveryStatus recoveryStatus) throws IndexShardGatewayRecoveryException {
        this.recoveryStatus = recoveryStatus;

        final ImmutableMap<String, BlobMetaData> blobs;
        try {
            blobs = blobContainer.listBlobs();
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to list content of gateway", e);
        }

        List<CommitPoint> commitPointsList = Lists.newArrayList();
        boolean atLeastOneCommitPointExists = false;
        for (String name : blobs.keySet()) {
            if (name.startsWith("commit-")) {
                atLeastOneCommitPointExists = true;
                try {
                    commitPointsList.add(CommitPoints.fromXContent(blobContainer.readBlobFully(name)));
                } catch (Exception e) {
                    logger.warn("failed to read commit point [{}]", e, name);
                }
            }
        }
        if (atLeastOneCommitPointExists && commitPointsList.isEmpty()) {
            // no commit point managed to load, bail so we won't corrupt the index, will require manual intervention
            throw new IndexShardGatewayRecoveryException(shardId, "Commit points exists but none could be loaded", null);
        }
        CommitPoints commitPoints = new CommitPoints(commitPointsList);

        if (commitPoints.commits().isEmpty()) {
            // no commit points, clean the store just so we won't recover wrong files
            try {
                indexShard.store().deleteContent();
            } catch (IOException e) {
                logger.warn("failed to clean store before starting shard", e);
            }
            recoveryStatus.index().startTime(System.currentTimeMillis());
            recoveryStatus.index().time(System.currentTimeMillis() - recoveryStatus.index().startTime());
            return;
        }

        for (CommitPoint commitPoint : commitPoints) {
            if (!commitPointExistsInBlobs(commitPoint, blobs)) {
                logger.warn("listed commit_point [{}]/[{}], but not all files exists, ignoring", commitPoint.name(), commitPoint.version());
                continue;
            }
            try {
                recoveryStatus.index().startTime(System.currentTimeMillis());
                recoverIndex(commitPoint, blobs);
                recoveryStatus.index().time(System.currentTimeMillis() - recoveryStatus.index().startTime());

                recoverTranslog(commitPoint, blobs);
                return;
            } catch (Exception e) {
                throw new IndexShardGatewayRecoveryException(shardId, "failed to recover commit_point [" + commitPoint.name() + "]/[" + commitPoint.version() + "]", e);
            }
        }
        throw new IndexShardGatewayRecoveryException(shardId, "No commit point data is available in gateway", null);
    }

    private void recoverTranslog(CommitPoint commitPoint, ImmutableMap<String, BlobMetaData> blobs) throws IndexShardGatewayRecoveryException {
        if (commitPoint.translogFiles().isEmpty()) {
            // no translog files, bail
            recoveryStatus.start().startTime(System.currentTimeMillis());
            recoveryStatus.updateStage(RecoveryStatus.Stage.START);
            indexShard.start("post recovery from gateway, no translog");
            recoveryStatus.start().time(System.currentTimeMillis() - recoveryStatus.start().startTime());
            recoveryStatus.start().checkIndexTime(indexShard.checkIndexTook());
            return;
        }

        try {
            recoveryStatus.start().startTime(System.currentTimeMillis());
            recoveryStatus.updateStage(RecoveryStatus.Stage.START);
            indexShard.performRecoveryPrepareForTranslog();
            recoveryStatus.start().time(System.currentTimeMillis() - recoveryStatus.start().startTime());
            recoveryStatus.start().checkIndexTime(indexShard.checkIndexTook());

            recoveryStatus.updateStage(RecoveryStatus.Stage.TRANSLOG);
            recoveryStatus.translog().startTime(System.currentTimeMillis());

            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            final CountDownLatch latch = new CountDownLatch(1);

            final Iterator<CommitPoint.FileInfo> transIt = commitPoint.translogFiles().iterator();

            blobContainer.readBlob(transIt.next().name(), new BlobContainer.ReadBlobListener() {
                FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
                boolean ignore = false;

                @Override
                public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                    if (ignore) {
                        return;
                    }
                    bos.write(data, offset, size);
                    // if we don't have enough to read the header size of the first translog, bail and wait for the next one
                    if (bos.size() < 4) {
                        return;
                    }
                    BytesStreamInput si = new BytesStreamInput(bos.bytes());
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
                        newBos.write(bos.bytes().array(), position, leftOver);
                    }

                    bos = newBos;
                }

                @Override
                public synchronized void onCompleted() {
                    if (ignore) {
                        return;
                    }
                    if (!transIt.hasNext()) {
                        latch.countDown();
                        return;
                    }
                    blobContainer.readBlob(transIt.next().name(), this);
                }

                @Override
                public void onFailure(Throwable t) {
                    failure.set(t);
                    latch.countDown();
                }
            });


            latch.await();
            if (failure.get() != null) {
                throw failure.get();
            }

            indexShard.performRecoveryFinalization(true);
            recoveryStatus.translog().time(System.currentTimeMillis() - recoveryStatus.translog().startTime());
        } catch (Throwable e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to recover translog", e);
        }
    }

    private void recoverIndex(CommitPoint commitPoint, ImmutableMap<String, BlobMetaData> blobs) throws Exception {
        recoveryStatus.updateStage(RecoveryStatus.Stage.INDEX);
        int numberOfFiles = 0;
        long totalSize = 0;
        int numberOfReusedFiles = 0;
        long reusedTotalSize = 0;

        List<CommitPoint.FileInfo> filesToRecover = Lists.newArrayList();
        for (CommitPoint.FileInfo fileInfo : commitPoint.indexFiles()) {
            String fileName = fileInfo.physicalName();
            StoreFileMetaData md = null;
            try {
                md = store.metaData(fileName);
            } catch (Exception e) {
                // no file
            }
            // we don't compute checksum for segments, so always recover them
            if (!fileName.startsWith("segments") && md != null && fileInfo.isSame(md)) {
                numberOfFiles++;
                totalSize += md.length();
                numberOfReusedFiles++;
                reusedTotalSize += md.length();
                if (logger.isTraceEnabled()) {
                    logger.trace("not_recovering [{}], exists in local store and is same", fileInfo.physicalName());
                }
            } else {
                if (logger.isTraceEnabled()) {
                    if (md == null) {
                        logger.trace("recovering [{}], does not exists in local store", fileInfo.physicalName());
                    } else {
                        logger.trace("recovering [{}], exists in local store but is different", fileInfo.physicalName());
                    }
                }
                numberOfFiles++;
                totalSize += fileInfo.length();
                filesToRecover.add(fileInfo);
            }
        }

        recoveryStatus.index().files(numberOfFiles, totalSize, numberOfReusedFiles, reusedTotalSize);
        if (filesToRecover.isEmpty()) {
            logger.trace("no files to recover, all exists within the local store");
        }

        if (logger.isTraceEnabled()) {
            logger.trace("recovering_files [{}] with total_size [{}], reusing_files [{}] with reused_size [{}]", numberOfFiles, new ByteSizeValue(totalSize), numberOfReusedFiles, new ByteSizeValue(reusedTotalSize));
        }

        final CountDownLatch latch = new CountDownLatch(filesToRecover.size());
        final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

        for (final CommitPoint.FileInfo fileToRecover : filesToRecover) {
            recoverFile(fileToRecover, blobs, latch, failures);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IndexShardGatewayRecoveryException(shardId, "Interrupted while recovering index", e);
        }

        if (!failures.isEmpty()) {
            throw new IndexShardGatewayRecoveryException(shardId, "Failed to recover index", failures.get(0));
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
                if (!commitPoint.containPhysicalIndexFile(storeFile)) {
                    try {
                        store.directory().deleteFile(storeFile);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private void recoverFile(final CommitPoint.FileInfo fileInfo, final ImmutableMap<String, BlobMetaData> blobs, final CountDownLatch latch, final List<Throwable> failures) {
        final IndexOutput indexOutput;
        try {
            // we create an output with no checksum, this is because the pure binary data of the file is not
            // the checksum (because of seek). We will create the checksum file once copying is done
            indexOutput = store.createOutputRaw(fileInfo.physicalName());
        } catch (IOException e) {
            failures.add(e);
            latch.countDown();
            return;
        }

        String firstFileToRecover = fileInfo.name();
        if (!blobs.containsKey(fileInfo.name())) {
            // chunking, append part0 to it
            firstFileToRecover = fileInfo.name() + ".part0";
        }
        if (!blobs.containsKey(firstFileToRecover)) {
            // no file, what to do, what to do?
            logger.warn("no file [{}]/[{}] to recover, ignoring it", fileInfo.name(), fileInfo.physicalName());
            latch.countDown();
            return;
        }
        final AtomicInteger partIndex = new AtomicInteger();

        blobContainer.readBlob(firstFileToRecover, new BlobContainer.ReadBlobListener() {
            @Override
            public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                recoveryStatus.index().addCurrentFilesSize(size);
                indexOutput.writeBytes(data, offset, size);
            }

            @Override
            public synchronized void onCompleted() {
                int part = partIndex.incrementAndGet();
                String partName = fileInfo.name() + ".part" + part;
                if (blobs.containsKey(partName)) {
                    // continue with the new part
                    blobContainer.readBlob(partName, this);
                    return;
                } else {
                    // we are done...
                    try {
                        indexOutput.close();
                        // write the checksum
                        if (fileInfo.checksum() != null) {
                            store.writeChecksum(fileInfo.physicalName(), fileInfo.checksum());
                        }
                        store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                    } catch (IOException e) {
                        onFailure(e);
                        return;
                    }
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                failures.add(t);
                latch.countDown();
            }
        });
    }

    private void snapshotTranslog(Translog.Snapshot snapshot, CommitPoint.FileInfo fileInfo) throws IOException {
        blobContainer.writeBlob(fileInfo.name(), snapshot.stream(), snapshot.lengthInBytes());
//
//        long chunkBytes = Long.MAX_VALUE;
//        if (chunkSize != null) {
//            chunkBytes = chunkSize.bytes();
//        }
//
//        long totalLength = fileInfo.length();
//        long numberOfChunks = totalLength / chunkBytes;
//        if (totalLength % chunkBytes > 0) {
//            numberOfChunks++;
//        }
//        if (numberOfChunks == 0) {
//            numberOfChunks++;
//        }
//
//        if (numberOfChunks == 1) {
//            blobContainer.writeBlob(fileInfo.name(), snapshot.stream(), snapshot.lengthInBytes());
//        } else {
//            InputStream translogStream = snapshot.stream();
//            long totalLengthLeftToWrite = totalLength;
//            for (int i = 0; i < numberOfChunks; i++) {
//                long lengthToWrite = chunkBytes;
//                if (totalLengthLeftToWrite < chunkBytes) {
//                    lengthToWrite = totalLengthLeftToWrite;
//                }
//                blobContainer.writeBlob(fileInfo.name() + ".part" + i, new LimitInputStream(translogStream, lengthToWrite), lengthToWrite);
//                totalLengthLeftToWrite -= lengthToWrite;
//            }
//        }
    }

    private void snapshotFile(Directory dir, final CommitPoint.FileInfo fileInfo, final CountDownLatch latch, final List<Throwable> failures) throws IOException {
        long chunkBytes = Long.MAX_VALUE;
        if (chunkSize != null) {
            chunkBytes = chunkSize.bytes();
        }

        long totalLength = fileInfo.length();
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
            final long partNumber = i;

            IndexInput indexInput = null;
            try {
                indexInput = indexShard.store().openInputRaw(fileInfo.physicalName());
                indexInput.seek(partNumber * chunkBytes);
                InputStreamIndexInput is = new ThreadSafeInputStreamIndexInput(indexInput, chunkBytes);

                String blobName = fileInfo.name();
                if (fNumberOfChunks > 1) {
                    // if we do chunks, then all of them are in the form of "[xxx].part[N]".
                    blobName += ".part" + partNumber;
                }

                final IndexInput fIndexInput = indexInput;
                blobContainer.writeBlob(blobName, is, is.actualSizeToRead(), new ImmutableBlobContainer.WriterListener() {
                    @Override
                    public void onCompleted() {
                        try {
                            fIndexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }
                        if (counter.decrementAndGet() == 0) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
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

    private boolean commitPointExistsInBlobs(CommitPoint commitPoint, ImmutableMap<String, BlobMetaData> blobs) {
        for (CommitPoint.FileInfo fileInfo : Iterables.concat(commitPoint.indexFiles(), commitPoint.translogFiles())) {
            if (!commitPointFileExistsInBlobs(fileInfo, blobs)) {
                return false;
            }
        }
        return true;
    }

    private boolean commitPointFileExistsInBlobs(CommitPoint.FileInfo fileInfo, ImmutableMap<String, BlobMetaData> blobs) {
        BlobMetaData blobMetaData = blobs.get(fileInfo.name());
        if (blobMetaData != null) {
            if (blobMetaData.length() != fileInfo.length()) {
                return false;
            }
        } else if (blobs.containsKey(fileInfo.name() + ".part0")) {
            // multi part file sum up the size and check
            int part = 0;
            long totalSize = 0;
            while (true) {
                blobMetaData = blobs.get(fileInfo.name() + ".part" + part++);
                if (blobMetaData == null) {
                    break;
                }
                totalSize += blobMetaData.length();
            }
            if (totalSize != fileInfo.length()) {
                return false;
            }
        } else {
            // no file, not exact and not multipart
            return false;
        }
        return true;
    }

    private CommitPoints buildCommitPoints(ImmutableMap<String, BlobMetaData> blobs) {
        List<CommitPoint> commitPoints = Lists.newArrayList();
        for (String name : blobs.keySet()) {
            if (name.startsWith("commit-")) {
                try {
                    commitPoints.add(CommitPoints.fromXContent(blobContainer.readBlobFully(name)));
                } catch (Exception e) {
                    logger.warn("failed to read commit point [{}]", e, name);
                }
            }
        }
        return new CommitPoints(commitPoints);
    }

    private String fileNameFromGeneration(long generation) {
        return "__" + Long.toString(generation, Character.MAX_RADIX);
    }

    private long findLatestFileNameGeneration(ImmutableMap<String, BlobMetaData> blobs) {
        long generation = -1;
        for (String name : blobs.keySet()) {
            if (!name.startsWith("__")) {
                continue;
            }
            if (name.contains(".part")) {
                name = name.substring(0, name.indexOf(".part"));
            }

            try {
                long currentGen = Long.parseLong(name.substring(2) /*__*/, Character.MAX_RADIX);
                if (currentGen > generation) {
                    generation = currentGen;
                }
            } catch (NumberFormatException e) {
                logger.warn("file [{}] does not conform to the '__' schema");
            }
        }
        return generation;
    }
}
