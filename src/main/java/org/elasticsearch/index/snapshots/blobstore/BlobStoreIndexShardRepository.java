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

package org.elasticsearch.index.snapshots.blobstore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.lucene.store.ThreadSafeInputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.*;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoryName;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Blob store based implementation of IndexShardRepository
 */
public class BlobStoreIndexShardRepository extends AbstractComponent implements IndexShardRepository {

    private BlobStore blobStore;

    private BlobPath basePath;

    private final String repositoryName;

    private ByteSizeValue chunkSize;

    private final IndicesService indicesService;

    private RateLimiter snapshotRateLimiter;

    private RateLimiter restoreRateLimiter;

    private RateLimiterListener rateLimiterListener;

    private RateLimitingInputStream.Listener snapshotThrottleListener;

    private static final String SNAPSHOT_PREFIX = "snapshot-";

    @Inject
    BlobStoreIndexShardRepository(Settings settings, RepositoryName repositoryName, IndicesService indicesService) {
        super(settings);
        this.repositoryName = repositoryName.name();
        this.indicesService = indicesService;
    }

    /**
     * Called by {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} on repository startup
     *
     * @param blobStore blob store
     * @param basePath  base path to blob store
     * @param chunkSize chunk size
     */
    public void initialize(BlobStore blobStore, BlobPath basePath, ByteSizeValue chunkSize,
                           RateLimiter snapshotRateLimiter, RateLimiter restoreRateLimiter,
                           final RateLimiterListener rateLimiterListener) {
        this.blobStore = blobStore;
        this.basePath = basePath;
        this.chunkSize = chunkSize;
        this.snapshotRateLimiter = snapshotRateLimiter;
        this.restoreRateLimiter = restoreRateLimiter;
        this.rateLimiterListener = rateLimiterListener;
        this.snapshotThrottleListener = new RateLimitingInputStream.Listener() {
            @Override
            public void onPause(long nanos) {
                rateLimiterListener.onSnapshotPause(nanos);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(SnapshotId snapshotId, ShardId shardId, SnapshotIndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
        SnapshotContext snapshotContext = new SnapshotContext(snapshotId, shardId, snapshotStatus);
        snapshotStatus.startTime(System.currentTimeMillis());

        try {
            snapshotContext.snapshot(snapshotIndexCommit);
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
            snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.DONE);
        } catch (Throwable e) {
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
            snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FAILURE);
            snapshotStatus.failure(ExceptionsHelper.detailedMessage(e));
            if (e instanceof IndexShardSnapshotFailedException) {
                throw (IndexShardSnapshotFailedException) e;
            } else {
                throw new IndexShardSnapshotFailedException(shardId, e.getMessage(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restore(SnapshotId snapshotId, ShardId shardId, ShardId snapshotShardId, RecoveryState recoveryState) {
        RestoreContext snapshotContext = new RestoreContext(snapshotId, shardId, snapshotShardId, recoveryState);

        try {
            recoveryState.getIndex().startTime(System.currentTimeMillis());
            snapshotContext.restore();
            recoveryState.getIndex().time(System.currentTimeMillis() - recoveryState.getIndex().startTime());
        } catch (Throwable e) {
            throw new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId.getSnapshot() + "]", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexShardSnapshotStatus snapshotStatus(SnapshotId snapshotId, ShardId shardId) {
        Context context = new Context(snapshotId, shardId);
        BlobStoreIndexShardSnapshot snapshot = context.loadSnapshot();
        IndexShardSnapshotStatus status = new IndexShardSnapshotStatus();
        status.updateStage(IndexShardSnapshotStatus.Stage.DONE);
        status.startTime(snapshot.startTime());
        status.files(snapshot.numberOfFiles(), snapshot.totalSize());
        // The snapshot is done which means the number of processed files is the same as total
        status.processedFiles(snapshot.numberOfFiles(), snapshot.totalSize());
        status.time(snapshot.time());
        return status;
    }

    /**
     * Delete shard snapshot
     *
     * @param snapshotId snapshot id
     * @param shardId    shard id
     */
    public void delete(SnapshotId snapshotId, ShardId shardId) {
        Context context = new Context(snapshotId, shardId, shardId);
        context.delete();
    }

    @Override
    public String toString() {
        return "BlobStoreIndexShardRepository[" +
                "[" + repositoryName +
                "], [" + blobStore + ']' +
                ']';
    }

    /**
     * Returns shard snapshot metadata file name
     *
     * @param snapshotId snapshot id
     * @return shard snapshot metadata file name
     */
    private String snapshotBlobName(SnapshotId snapshotId) {
        return SNAPSHOT_PREFIX + snapshotId.getSnapshot();
    }

    /**
     * Serializes snapshot to JSON
     *
     * @param snapshot snapshot
     * @return JSON representation of the snapshot
     * @throws IOException
     */
    public static byte[] writeSnapshot(BlobStoreIndexShardSnapshot snapshot) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        BlobStoreIndexShardSnapshot.toXContent(snapshot, builder, ToXContent.EMPTY_PARAMS);
        return builder.bytes().toBytes();
    }

    /**
     * Parses JSON representation of a snapshot
     *
     * @param data JSON
     * @return snapshot
     * @throws IOException
     */
    public static BlobStoreIndexShardSnapshot readSnapshot(byte[] data) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(data)) {
            parser.nextToken();
            return BlobStoreIndexShardSnapshot.fromXContent(parser);
        }
    }

    /**
     * Context for snapshot/restore operations
     */
    private class Context {

        protected final SnapshotId snapshotId;

        protected final ShardId shardId;

        protected final ImmutableBlobContainer blobContainer;

        public Context(SnapshotId snapshotId, ShardId shardId) {
            this(snapshotId, shardId, shardId);
        }

        public Context(SnapshotId snapshotId, ShardId shardId, ShardId snapshotShardId) {
            this.snapshotId = snapshotId;
            this.shardId = shardId;
            blobContainer = blobStore.immutableBlobContainer(basePath.add("indices").add(snapshotShardId.getIndex()).add(Integer.toString(snapshotShardId.getId())));
        }

        /**
         * Delete shard snapshot
         */
        public void delete() {
            final ImmutableMap<String, BlobMetaData> blobs;
            try {
                blobs = blobContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardSnapshotException(shardId, "Failed to list content of gateway", e);
            }

            BlobStoreIndexShardSnapshots snapshots = buildBlobStoreIndexShardSnapshots(blobs);

            String commitPointName = snapshotBlobName(snapshotId);

            try {
                blobContainer.deleteBlob(commitPointName);
            } catch (IOException e) {
                logger.debug("[{}] [{}] failed to delete shard snapshot file", shardId, snapshotId);
            }

            // delete all files that are not referenced by any commit point
            // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
            List<BlobStoreIndexShardSnapshot> newSnapshotsList = Lists.newArrayList();
            for (BlobStoreIndexShardSnapshot point : snapshots) {
                if (!point.snapshot().equals(snapshotId.getSnapshot())) {
                    newSnapshotsList.add(point);
                }
            }
            cleanup(newSnapshotsList, blobs);
        }

        /**
         * Loads information about shard snapshot
         */
        public BlobStoreIndexShardSnapshot loadSnapshot() {
            BlobStoreIndexShardSnapshot snapshot;
            try {
                snapshot = readSnapshot(blobContainer.readBlobFully(snapshotBlobName(snapshotId)));
            } catch (IOException ex) {
                throw new IndexShardRestoreFailedException(shardId, "failed to read shard snapshot file", ex);
            }
            return snapshot;
        }

        /**
         * Removes all unreferenced files from the repository
         *
         * @param snapshots list of active snapshots in the container
         * @param blobs     list of blobs in the container
         */
        protected void cleanup(List<BlobStoreIndexShardSnapshot> snapshots, ImmutableMap<String, BlobMetaData> blobs) {
            BlobStoreIndexShardSnapshots newSnapshots = new BlobStoreIndexShardSnapshots(snapshots);
            // now go over all the blobs, and if they don't exists in a snapshot, delete them
            for (String blobName : blobs.keySet()) {
                if (!blobName.startsWith("__")) {
                    continue;
                }
                if (newSnapshots.findNameFile(FileInfo.canonicalName(blobName)) == null) {
                    try {
                        blobContainer.deleteBlob(blobName);
                    } catch (IOException e) {
                        logger.debug("[{}] [{}] error deleting blob [{}] during cleanup", e, snapshotId, shardId, blobName);
                    }
                }
            }
        }

        /**
         * Generates blob name
         *
         * @param generation the blob number
         * @return the blob name
         */
        protected String fileNameFromGeneration(long generation) {
            return "__" + Long.toString(generation, Character.MAX_RADIX);
        }

        /**
         * Finds the next available blob number
         *
         * @param blobs list of blobs in the repository
         * @return next available blob number
         */
        protected long findLatestFileNameGeneration(ImmutableMap<String, BlobMetaData> blobs) {
            long generation = -1;
            for (String name : blobs.keySet()) {
                if (!name.startsWith("__")) {
                    continue;
                }
                name = FileInfo.canonicalName(name);
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

        /**
         * Loads all available snapshots in the repository
         *
         * @param blobs list of blobs in repository
         * @return BlobStoreIndexShardSnapshots
         */
        protected BlobStoreIndexShardSnapshots buildBlobStoreIndexShardSnapshots(ImmutableMap<String, BlobMetaData> blobs) {
            List<BlobStoreIndexShardSnapshot> snapshots = Lists.newArrayList();
            for (String name : blobs.keySet()) {
                if (name.startsWith(SNAPSHOT_PREFIX)) {
                    try {
                        snapshots.add(readSnapshot(blobContainer.readBlobFully(name)));
                    } catch (IOException e) {
                        logger.warn("failed to read commit point [{}]", e, name);
                    }
                }
            }
            return new BlobStoreIndexShardSnapshots(snapshots);
        }

    }

    /**
     * Context for snapshot operations
     */
    private class SnapshotContext extends Context {

        private final Store store;

        private final IndexShardSnapshotStatus snapshotStatus;

        /**
         * Constructs new context
         *
         * @param snapshotId     snapshot id
         * @param shardId        shard to be snapshotted
         * @param snapshotStatus snapshot status to report progress
         */
        public SnapshotContext(SnapshotId snapshotId, ShardId shardId, IndexShardSnapshotStatus snapshotStatus) {
            super(snapshotId, shardId);
            store = indicesService.indexServiceSafe(shardId.getIndex()).shardInjectorSafe(shardId.id()).getInstance(Store.class);
            this.snapshotStatus = snapshotStatus;
        }

        /**
         * Create snapshot from index commit point
         *
         * @param snapshotIndexCommit snapshot commit point
         */
        public void snapshot(SnapshotIndexCommit snapshotIndexCommit) {
            logger.debug("[{}] [{}] snapshot to [{}] ...", shardId, snapshotId, repositoryName);
            store.incRef();
            try {
                final ImmutableMap<String, BlobMetaData> blobs;
                try {
                    blobs = blobContainer.listBlobs();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }

                long generation = findLatestFileNameGeneration(blobs);
                BlobStoreIndexShardSnapshots snapshots = buildBlobStoreIndexShardSnapshots(blobs);

                final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
                final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles = newArrayList();

                int indexNumberOfFiles = 0;
                long indexTotalFilesSize = 0;
                ArrayList<FileInfo> filesToSnapshot = newArrayList();
                for (String fileName : snapshotIndexCommit.getFiles()) {
                    if (snapshotStatus.aborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }
                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetaData md;
                    try {
                        md = store.metaData(fileName);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                    }

                    boolean snapshotRequired = false;
                    // TODO: For now segment files are copied on each commit because segment files don't have checksum
    //            if (snapshot.indexChanged() && fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
    //                snapshotRequired = true; // we want to always snapshot the segment file if the index changed
    //            }

                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = snapshots.findPhysicalIndexFile(fileName);

                    if (fileInfo == null || !fileInfo.isSame(md) || !snapshotFileExistsInBlobs(fileInfo, blobs)) {
                        // commit point file does not exists in any commit point, or has different length, or does not fully exists in the listed blobs
                        snapshotRequired = true;
                    }

                    if (snapshotRequired) {
                        indexNumberOfFiles++;
                        indexTotalFilesSize += md.length();
                        // create a new FileInfo
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(fileNameFromGeneration(++generation), fileName, md.length(), chunkSize, md.checksum());
                        indexCommitPointFiles.add(snapshotFileInfo);
                        filesToSnapshot.add(snapshotFileInfo);
                    } else {
                        indexCommitPointFiles.add(fileInfo);
                    }
                }

                snapshotStatus.files(indexNumberOfFiles, indexTotalFilesSize);

                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.STARTED);

                final CountDownLatch indexLatch = new CountDownLatch(filesToSnapshot.size());

                for (FileInfo snapshotFileInfo : filesToSnapshot) {
                    try {
                        snapshotFile(snapshotFileInfo, indexLatch, failures);
                    } catch (IOException e) {
                        failures.add(e);
                    }
                }

                snapshotStatus.indexVersion(snapshotIndexCommit.getGeneration());

                try {
                    indexLatch.await();
                } catch (InterruptedException e) {
                    failures.add(e);
                    Thread.currentThread().interrupt();
                }
                if (!failures.isEmpty()) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to perform snapshot (index files)", failures.get(0));
                }

                // now create and write the commit point
                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FINALIZE);

                String commitPointName = snapshotBlobName(snapshotId);
                BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(snapshotId.getSnapshot(),
                        snapshotIndexCommit.getGeneration(), indexCommitPointFiles, snapshotStatus.startTime(),
                        // snapshotStatus.startTime() is assigned on the same machine, so it's safe to use with VLong
                        System.currentTimeMillis() - snapshotStatus.startTime(), indexNumberOfFiles, indexTotalFilesSize);
                //TODO: The time stored in snapshot doesn't include cleanup time.
                try {
                    byte[] snapshotData = writeSnapshot(snapshot);
                    logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                    blobContainer.writeBlob(commitPointName, new BytesStreamInput(snapshotData, false), snapshotData.length);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }

                // delete all files that are not referenced by any commit point
                // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
                List<BlobStoreIndexShardSnapshot> newSnapshotsList = Lists.newArrayList();
                newSnapshotsList.add(snapshot);
                for (BlobStoreIndexShardSnapshot point : snapshots) {
                    newSnapshotsList.add(point);
                }
                cleanup(newSnapshotsList, blobs);
                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.DONE);
            } finally {
                store.decRef();
            }
        }

        /**
         * Snapshot individual file
         * <p/>
         * This is asynchronous method. Upon completion of the operation latch is getting counted down and any failures are
         * added to the {@code failures} list
         *
         * @param fileInfo file to be snapshotted
         * @param latch    latch that should be counted down once file is snapshoted
         * @param failures thread-safe list of failures
         * @throws IOException
         */
        private void snapshotFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo, final CountDownLatch latch, final List<Throwable> failures) throws IOException {
            final AtomicLong counter = new AtomicLong(fileInfo.numberOfParts());
            for (long i = 0; i < fileInfo.numberOfParts(); i++) {
                IndexInput indexInput = null;
                try {
                    final String file = fileInfo.physicalName();
                    indexInput = store.openInputRaw(file, IOContext.READONCE);
                    indexInput.seek(i * fileInfo.partBytes());
                    InputStreamIndexInput inputStreamIndexInput = new ThreadSafeInputStreamIndexInput(indexInput, fileInfo.partBytes());

                    final IndexInput fIndexInput = indexInput;
                    long size = inputStreamIndexInput.actualSizeToRead();
                    InputStream inputStream;
                    if (snapshotRateLimiter != null) {
                        inputStream = new RateLimitingInputStream(inputStreamIndexInput, snapshotRateLimiter, snapshotThrottleListener);
                    } else {
                        inputStream = inputStreamIndexInput;
                    }
                    inputStream = new AbortableInputStream(inputStream, file);
                    blobContainer.writeBlob(fileInfo.partName(i), inputStream, size, new ImmutableBlobContainer.WriterListener() {
                        @Override
                        public void onCompleted() {
                            IOUtils.closeWhileHandlingException(fIndexInput);
                            snapshotStatus.addProcessedFile(fileInfo.length());
                            if (counter.decrementAndGet() == 0) {
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            IOUtils.closeWhileHandlingException(fIndexInput);
                            snapshotStatus.addProcessedFile(0);
                            failures.add(t);
                            if (counter.decrementAndGet() == 0) {
                                latch.countDown();
                            }
                        }
                    });
                } catch (Throwable e) {
                    IOUtils.closeWhileHandlingException(indexInput);
                    failures.add(e);
                    latch.countDown();
                }
            }
        }

        /**
         * Checks if snapshot file already exists in the list of blobs
         *
         * @param fileInfo file to check
         * @param blobs    list of blobs
         * @return true if file exists in the list of blobs
         */
        private boolean snapshotFileExistsInBlobs(BlobStoreIndexShardSnapshot.FileInfo fileInfo, ImmutableMap<String, BlobMetaData> blobs) {
            BlobMetaData blobMetaData = blobs.get(fileInfo.name());
            if (blobMetaData != null) {
                return blobMetaData.length() == fileInfo.length();
            } else if (blobs.containsKey(fileInfo.partName(0))) {
                // multi part file sum up the size and check
                int part = 0;
                long totalSize = 0;
                while (true) {
                    blobMetaData = blobs.get(fileInfo.partName(part++));
                    if (blobMetaData == null) {
                        break;
                    }
                    totalSize += blobMetaData.length();
                }
                return totalSize == fileInfo.length();
            }
            // no file, not exact and not multipart
            return false;
        }

        private class AbortableInputStream extends FilterInputStream {
            private final String fileName;

            public AbortableInputStream(InputStream delegate, String fileName) {
                super(delegate);
                this.fileName = fileName;
            }

            @Override
            public int read() throws IOException {
                checkAborted();
                return in.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                checkAborted();
                return in.read(b, off, len);
            }

            private void checkAborted() {
                if (snapshotStatus.aborted()) {
                    logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                    throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                }
            }
        }
    }

    /**
     * Context for restore operations
     */
    private class RestoreContext extends Context {

        private final Store store;

        private final RecoveryState recoveryState;

        /**
         * Constructs new restore context
         *
         * @param snapshotId      snapshot id
         * @param shardId         shard to be restored
         * @param snapshotShardId shard in the snapshot that data should be restored from
         * @param recoveryState   recovery state to report progress
         */
        public RestoreContext(SnapshotId snapshotId, ShardId shardId, ShardId snapshotShardId, RecoveryState recoveryState) {
            super(snapshotId, shardId, snapshotShardId);
            store = indicesService.indexServiceSafe(shardId.getIndex()).shardInjectorSafe(shardId.id()).getInstance(Store.class);
            this.recoveryState = recoveryState;
        }

        /**
         * Performs restore operation
         */
        public void restore() {
            store.incRef();
            try {
                logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, repositoryName, shardId);
                BlobStoreIndexShardSnapshot snapshot = loadSnapshot();

                recoveryState.setStage(RecoveryState.Stage.INDEX);
                int numberOfFiles = 0;
                long totalSize = 0;
                int numberOfReusedFiles = 0;
                long reusedTotalSize = 0;

                List<FileInfo> filesToRecover = Lists.newArrayList();
                for (FileInfo fileInfo : snapshot.indexFiles()) {
                    String fileName = fileInfo.physicalName();
                    StoreFileMetaData md = null;
                    try {
                        md = store.metaData(fileName);
                    } catch (IOException e) {
                        // no file
                    }
                    numberOfFiles++;
                    // we don't compute checksum for segments, so always recover them
                    if (!fileName.startsWith("segments") && md != null && fileInfo.isSame(md)) {
                        totalSize += md.length();
                        numberOfReusedFiles++;
                        reusedTotalSize += md.length();
                        recoveryState.getIndex().addReusedFileDetail(fileInfo.name(), fileInfo.length());
                        if (logger.isTraceEnabled()) {
                            logger.trace("not_recovering [{}], exists in local store and is same", fileInfo.physicalName());
                        }
                    } else {
                        totalSize += fileInfo.length();
                        filesToRecover.add(fileInfo);
                        recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length());
                        if (logger.isTraceEnabled()) {
                            if (md == null) {
                                logger.trace("recovering [{}], does not exists in local store", fileInfo.physicalName());
                            } else {
                                logger.trace("recovering [{}], exists in local store but is different", fileInfo.physicalName());
                            }
                        }
                    }
                }

                recoveryState.getIndex().files(numberOfFiles, totalSize, numberOfReusedFiles, reusedTotalSize);
                if (filesToRecover.isEmpty()) {
                    logger.trace("no files to recover, all exists within the local store");
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] recovering_files [{}] with total_size [{}], reusing_files [{}] with reused_size [{}]", shardId, snapshotId, numberOfFiles, new ByteSizeValue(totalSize), numberOfReusedFiles, new ByteSizeValue(reusedTotalSize));
                }

                final CountDownLatch latch = new CountDownLatch(filesToRecover.size());
                final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();

                for (final FileInfo fileToRecover : filesToRecover) {
                    logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
                    restoreFile(fileToRecover, latch, failures);
                }

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (!failures.isEmpty()) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", failures.get(0));
                }

                // read the snapshot data persisted
                long version = -1;
                try {
                    if (Lucene.indexExists(store.directory())) {
                        version = Lucene.readSegmentInfos(store.directory()).getVersion();
                    }
                } catch (IOException e) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
                }
                recoveryState.getIndex().updateVersion(version);

                /// now, go over and clean files that are in the store, but were not in the snapshot
                try {
                    for (String storeFile : store.directory().listAll()) {
                        if (!snapshot.containPhysicalIndexFile(storeFile)) {
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
            } finally {
                store.decRef();
            }
        }

        /**
         * Restores a file
         * This is asynchronous method. Upon completion of the operation latch is getting counted down and any failures are
         * added to the {@code failures} list
         *
         * @param fileInfo file to be restored
         * @param latch    latch that should be counted down once file is snapshoted
         * @param failures thread-safe list of failures
         */
        private void restoreFile(final FileInfo fileInfo, final CountDownLatch latch, final List<Throwable> failures) {
            final IndexOutput indexOutput;
            try {
                // we create an output with no checksum, this is because the pure binary data of the file is not
                // the checksum (because of seek). We will create the checksum file once copying is done
                indexOutput = store.createOutputRaw(fileInfo.physicalName());
            } catch (IOException e) {
                try {
                    failures.add(e);
                } finally {
                    latch.countDown();
                }
                return;
            }

            String firstFileToRecover = fileInfo.partName(0);
            final AtomicInteger partIndex = new AtomicInteger();
            boolean success = false;
            try {
                blobContainer.readBlob(firstFileToRecover, new BlobContainer.ReadBlobListener() {
                    @Override
                    public synchronized void onPartial(byte[] data, int offset, int size) throws IOException {
                        recoveryState.getIndex().addRecoveredByteCount(size);
                        RecoveryState.File file = recoveryState.getIndex().file(fileInfo.name());
                        if (file != null) {
                            file.updateRecovered(size);
                        }
                        indexOutput.writeBytes(data, offset, size);
                        if (restoreRateLimiter != null) {
                            rateLimiterListener.onRestorePause(restoreRateLimiter.pause(size));
                        }
                    }

                    @Override
                    public synchronized void onCompleted() {
                        int part = partIndex.incrementAndGet();
                        if (part < fileInfo.numberOfParts()) {
                            String partName = fileInfo.partName(part);
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
                                recoveryState.getIndex().addRecoveredFileCount(1);
                            } catch (IOException e) {
                                onFailure(e);
                                return;
                            }
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            IOUtils.closeWhileHandlingException(indexOutput);
                            failures.add(t);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(indexOutput);
                    latch.countDown();
                }
            }

        }

    }

    public interface RateLimiterListener {
        void onRestorePause(long nanos);

        void onSnapshotPause(long nanos);
    }

}
