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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.*;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositoryVerificationException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.testBlobPrefix;

/**
 * Blob store based implementation of IndexShardRepository
 */
public class BlobStoreIndexShardRepository extends AbstractComponent implements IndexShardRepository {

    private static final int BUFFER_SIZE = 4096;
    private BlobStore blobStore;

    private BlobPath basePath;

    private final String repositoryName;

    private ByteSizeValue chunkSize;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private RateLimiter snapshotRateLimiter;

    private RateLimiter restoreRateLimiter;

    private RateLimiterListener rateLimiterListener;

    private RateLimitingInputStream.Listener snapshotThrottleListener;

    private static final String SNAPSHOT_PREFIX = "snapshot-";

    @Inject
    public BlobStoreIndexShardRepository(Settings settings, RepositoryName repositoryName, IndicesService indicesService, ClusterService clusterService) {
        super(settings);
        this.repositoryName = repositoryName.name();
        this.indicesService = indicesService;
        this.clusterService = clusterService;
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
        final RestoreContext snapshotContext = new RestoreContext(snapshotId, shardId, snapshotShardId, recoveryState);
        try {
            snapshotContext.restore();
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

    @Override
    public void verify(String seed) {
        BlobContainer testBlobContainer = blobStore.blobContainer(basePath);;
        DiscoveryNode localNode = clusterService.localNode();
        if (testBlobContainer.blobExists(testBlobPrefix(seed) + "-master")) {
            try (OutputStream outputStream = testBlobContainer.createOutput(testBlobPrefix(seed) + "-" + localNode.getId())) {
                outputStream.write(Strings.toUTF8Bytes(seed));
            } catch (IOException exp) {
                throw new RepositoryVerificationException(repositoryName, "store location [" + blobStore + "] is not accessible on the node [" + localNode + "]", exp);
            }
        } else {
            throw new RepositoryVerificationException(repositoryName, "store location [" + blobStore + "] is not shared between node [" + localNode + "] and the master node");
        }
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
     * @param stream the stream to output the snapshot JSON represetation to
     * @throws IOException if an IOException occurs
     */
    public static void writeSnapshot(BlobStoreIndexShardSnapshot snapshot, OutputStream stream) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream).prettyPrint();
        BlobStoreIndexShardSnapshot.toXContent(snapshot, builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
    }

    /**
     * Parses JSON representation of a snapshot
     *
     * @param stream JSON
     * @return snapshot
     * @throws IOException if an IOException occurs
     * */
    public static BlobStoreIndexShardSnapshot readSnapshot(InputStream stream) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(stream)) {
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

        protected final BlobContainer blobContainer;

        public Context(SnapshotId snapshotId, ShardId shardId) {
            this(snapshotId, shardId, shardId);
        }

        public Context(SnapshotId snapshotId, ShardId shardId, ShardId snapshotShardId) {
            this.snapshotId = snapshotId;
            this.shardId = shardId;
            blobContainer = blobStore.blobContainer(basePath.add("indices").add(snapshotShardId.getIndex()).add(Integer.toString(snapshotShardId.getId())));
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
            try (InputStream stream = blobContainer.openInput(snapshotBlobName(snapshotId))) {
                    snapshot = readSnapshot(stream);
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
                    try (InputStream stream = blobContainer.openInput(name)) {
                        snapshots.add(readSnapshot(stream));
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
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            store = indexService.shardInjectorSafe(shardId.id()).getInstance(Store.class);
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
                final Store.MetadataSnapshot metadata;
                // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                try {
                    metadata = store.getMetadata(snapshotIndexCommit);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                }
                for (String fileName : snapshotIndexCommit.getFiles()) {
                    if (snapshotStatus.aborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }
                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetaData md = metadata.get(fileName);
                    boolean snapshotRequired = false;
                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = snapshots.findPhysicalIndexFile(fileName);
                    try {
                        // in 1.3.3 we added additional hashes for .si / segments_N files
                        // to ensure we don't double the space in the repo since old snapshots
                        // don't have this hash we try to read that hash from the blob store
                        // in a bwc compatible way.
                        maybeRecalculateMetadataHash(blobContainer, fileInfo, metadata);
                    }  catch (Throwable e) {
                        logger.warn("{} Can't calculate hash from blob for file [{}] [{}]", e, shardId, fileInfo.physicalName(), fileInfo.metadata());
                    }
                    if (fileInfo == null || !fileInfo.isSame(md) || !snapshotFileExistsInBlobs(fileInfo, blobs)) {
                        // commit point file does not exists in any commit point, or has different length, or does not fully exists in the listed blobs
                        snapshotRequired = true;
                    }

                    if (snapshotRequired) {
                        indexNumberOfFiles++;
                        indexTotalFilesSize += md.length();
                        // create a new FileInfo
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(fileNameFromGeneration(++generation), md, chunkSize);
                        indexCommitPointFiles.add(snapshotFileInfo);
                        filesToSnapshot.add(snapshotFileInfo);
                    } else {
                        indexCommitPointFiles.add(fileInfo);
                    }
                }

                snapshotStatus.files(indexNumberOfFiles, indexTotalFilesSize);

                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.STARTED);

                for (FileInfo snapshotFileInfo : filesToSnapshot) {
                    try {
                        snapshotFile(snapshotFileInfo);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to perform snapshot (index files)", e);
                    }
                }

                snapshotStatus.indexVersion(snapshotIndexCommit.getGeneration());
                // now create and write the commit point
                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FINALIZE);

                String snapshotBlobName = snapshotBlobName(snapshotId);
                BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(snapshotId.getSnapshot(),
                        snapshotIndexCommit.getGeneration(), indexCommitPointFiles, snapshotStatus.startTime(),
                        // snapshotStatus.startTime() is assigned on the same machine, so it's safe to use with VLong
                        System.currentTimeMillis() - snapshotStatus.startTime(), indexNumberOfFiles, indexTotalFilesSize);
                //TODO: The time stored in snapshot doesn't include cleanup time.
                logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                try (OutputStream output = blobContainer.createOutput(snapshotBlobName)) {
                    writeSnapshot(snapshot, output);
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
         * @throws IOException
         */
        private void snapshotFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
            final String file = fileInfo.physicalName();
            final byte[] buffer = new byte[BUFFER_SIZE];
            try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {
                for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                    final InputStreamIndexInput inputStreamIndexInput = new InputStreamIndexInput(indexInput, fileInfo.partBytes());
                    InputStream inputStream = snapshotRateLimiter == null ? inputStreamIndexInput :  new RateLimitingInputStream(inputStreamIndexInput, snapshotRateLimiter, snapshotThrottleListener);
                    inputStream = new AbortableInputStream(inputStream, fileInfo.physicalName());
                    try (OutputStream output = blobContainer.createOutput(fileInfo.partName(i))) {
                        int len;
                        while ((len = inputStream.read(buffer)) > 0) {
                            output.write(buffer, 0, len);
                        }
                    }
                }
                Store.verify(indexInput);
                snapshotStatus.addProcessedFile(fileInfo.length());
            } catch (Throwable t) {
                failStoreIfCorrupted(t);
                snapshotStatus.addProcessedFile(0);
                throw t;
            }
        }

        private void failStoreIfCorrupted(Throwable t) {
            if (t instanceof CorruptIndexException || t instanceof IndexFormatTooOldException || t instanceof IndexFormatTooNewException) {
                try {
                    store.markStoreCorrupted((IOException) t);
                } catch (IOException e) {
                    logger.warn("store cannot be marked as corrupted", e);
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
     * This is a BWC layer to ensure we update the snapshots metdata with the corresponding hashes before we compare them.
     * The new logic for StoreFileMetaData reads the entire <tt>.si</tt> and <tt>segments.n</tt> files to strengthen the
     * comparison of the files on a per-segment / per-commit level.
     */
    private static final void maybeRecalculateMetadataHash(final BlobContainer blobContainer, final FileInfo fileInfo, Store.MetadataSnapshot snapshot) throws Throwable {
        final StoreFileMetaData metadata;
        if (fileInfo != null && (metadata = snapshot.get(fileInfo.physicalName())) != null) {
            if (metadata.hash().length > 0 && fileInfo.metadata().hash().length == 0) {
                // we have a hash - check if our repo has a hash too otherwise we have
                // to calculate it.
                // we might have multiple parts even though the file is small... make sure we read all of it.
                try (final InputStream stream = new PartSliceStream(blobContainer, fileInfo)) {
                    BytesRefBuilder builder = new BytesRefBuilder();
                    Store.MetadataSnapshot.hashFile(builder, stream, fileInfo.length());
                    BytesRef hash = fileInfo.metadata().hash(); // reset the file infos metadata hash
                    assert hash.length == 0;
                    hash.bytes = builder.bytes();
                    hash.offset = 0;
                    hash.length = builder.length();
                }
            }
        }
    }

    private static final class PartSliceStream extends SlicedInputStream {

        private final BlobContainer container;
        private final FileInfo info;

        public PartSliceStream(BlobContainer container, FileInfo info) {
            super(info.numberOfParts());
            this.info = info;
            this.container = container;
        }

        @Override
        protected InputStream openSlice(long slice) throws IOException {
            return container.openInput(info.partName(slice));

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
        public void restore() throws IOException {
            store.incRef();
            try {
                logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, repositoryName, shardId);
                BlobStoreIndexShardSnapshot snapshot = loadSnapshot();
                final Store.MetadataSnapshot recoveryTargetMetadata;
                try {
                    recoveryTargetMetadata = store.getMetadataOrEmpty();
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
                    logger.warn("{} Can't read metadata from store", e, shardId);
                    throw new IndexShardRestoreFailedException(shardId, "Can't restore corrupted shard", e);
                }

                final List<FileInfo> filesToRecover = Lists.newArrayList();
                final Map<String, StoreFileMetaData> snapshotMetaData = new HashMap<>();
                final Map<String, FileInfo> fileInfos = new HashMap<>();
                for (final FileInfo fileInfo : snapshot.indexFiles()) {
                    try {
                        // in 1.3.3 we added additional hashes for .si / segments_N files
                        // to ensure we don't double the space in the repo since old snapshots
                        // don't have this hash we try to read that hash from the blob store
                        // in a bwc compatible way.
                        maybeRecalculateMetadataHash(blobContainer, fileInfo, recoveryTargetMetadata);
                    }  catch (Throwable e) {
                        // if the index is broken we might not be able to read it
                        logger.warn("{} Can't calculate hash from blog for file [{}] [{}]", e, shardId, fileInfo.physicalName(), fileInfo.metadata());
                    }
                    snapshotMetaData.put(fileInfo.metadata().name(), fileInfo.metadata());
                    fileInfos.put(fileInfo.metadata().name(), fileInfo);
                }
                final Store.MetadataSnapshot sourceMetaData = new Store.MetadataSnapshot(snapshotMetaData);
                final Store.RecoveryDiff diff = sourceMetaData.recoveryDiff(recoveryTargetMetadata);
                for (StoreFileMetaData md : diff.identical) {
                    FileInfo fileInfo = fileInfos.get(md.name());
                    recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length(), true);
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] [{}] not_recovering [{}] from [{}], exists in local store and is same", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                    }
                }

                for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                    FileInfo fileInfo = fileInfos.get(md.name());
                    filesToRecover.add(fileInfo);
                    recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length(), false);
                    if (logger.isTraceEnabled()) {
                        if (md == null) {
                            logger.trace("[{}] [{}] recovering [{}] from [{}], does not exists in local store", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                        } else {
                            logger.trace("[{}] [{}] recovering [{}] from [{}], exists in local store but is different", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                        }
                    }
                }
                final RecoveryState.Index index = recoveryState.getIndex();
                if (filesToRecover.isEmpty()) {
                    logger.trace("no files to recover, all exists within the local store");
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] recovering_files [{}] with total_size [{}], reusing_files [{}] with reused_size [{}]", shardId, snapshotId,
                            index.totalRecoverFiles(), new ByteSizeValue(index.totalRecoverBytes()), index.reusedFileCount(), new ByteSizeValue(index.reusedFileCount()));
                }
                try {
                    for (final FileInfo fileToRecover : filesToRecover) {
                        logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
                        restoreFile(fileToRecover);
                    }
                } catch (IOException ex) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", ex);
                }
                final StoreFileMetaData restoredSegmentsFile = sourceMetaData.getSegmentsFile();
                if (recoveryTargetMetadata == null) {
                    throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
                }
                assert restoredSegmentsFile != null;
                // read the snapshot data persisted
                final SegmentInfos segmentCommitInfos;
                try {
                    segmentCommitInfos = Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
                } catch (IOException e) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
                }
                recoveryState.getIndex().updateVersion(segmentCommitInfos.getVersion());
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
         */
        private void restoreFile(final FileInfo fileInfo) throws IOException {
            boolean success = false;
            try (InputStream stream = new PartSliceStream(blobContainer, fileInfo)) {
                try (final IndexOutput indexOutput = store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int length;
                    while((length=stream.read(buffer))>0){
                        indexOutput.writeBytes(buffer,0,length);
                        recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.name(), length);
                        if (restoreRateLimiter != null) {
                            rateLimiterListener.onRestorePause(restoreRateLimiter.pause(length));
                        }
                    }
                    Store.verify(indexOutput);
                    indexOutput.close();
                    // write the checksum
                    if (fileInfo.metadata().hasLegacyChecksum()) {
                        Store.LegacyChecksums legacyChecksums = new Store.LegacyChecksums();
                        legacyChecksums.add(fileInfo.metadata());
                        legacyChecksums.write(store);

                    }
                    store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                    success = true;
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                    try {
                        store.markStoreCorrupted(ex);
                    } catch (IOException e) {
                        logger.warn("store cannot be marked as corrupted", e);
                    }
                    throw ex;
                } finally {
                    if (success == false) {
                        store.deleteQuiet(fileInfo.physicalName());
                    }
                }
            }
        }

    }
    
    public interface RateLimiterListener {
        void onRestorePause(long nanos);

        void onSnapshotPause(long nanos);
    }

}
