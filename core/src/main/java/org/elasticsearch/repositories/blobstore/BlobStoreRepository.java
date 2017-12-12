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

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * BlobStore - based implementation of Snapshot Repository
 * <p>
 * This repository works with any {@link BlobStore} implementation. The blobStore should be initialized in the derived
 * class before {@link #doStart()} is called.
 * <p>
 * BlobStoreRepository maintains the following structure in the blob store
 * <pre>
 * {@code
 *   STORE_ROOT
 *   |- index-N           - list of all snapshot ids and the indices belonging to each snapshot, N is the generation of the file
 *   |- index.latest      - contains the numeric value of the latest generation of the index file (i.e. N from above)
 *   |- incompatible-snapshots - list of all snapshot ids that are no longer compatible with the current version of the cluster
 *   |- snap-20131010 - JSON serialized Snapshot for snapshot "20131010"
 *   |- meta-20131010.dat - JSON serialized MetaData for snapshot "20131010" (includes only global metadata)
 *   |- snap-20131011 - JSON serialized Snapshot for snapshot "20131011"
 *   |- meta-20131011.dat - JSON serialized MetaData for snapshot "20131011"
 *   .....
 *   |- indices/ - data for all indices
 *      |- Ac1342-B_x/ - data for index "foo" which was assigned the unique id of Ac1342-B_x in the repository
 *      |  |- meta-20131010.dat - JSON Serialized IndexMetaData for index "foo"
 *      |  |- 0/ - data for shard "0" of index "foo"
 *      |  |  |- __1 \
 *      |  |  |- __2 |
 *      |  |  |- __3 |- files from different segments see snapshot-* for their mappings to real segment files
 *      |  |  |- __4 |
 *      |  |  |- __5 /
 *      |  |  .....
 *      |  |  |- snap-20131010.dat - JSON serialized BlobStoreIndexShardSnapshot for snapshot "20131010"
 *      |  |  |- snap-20131011.dat - JSON serialized BlobStoreIndexShardSnapshot for snapshot "20131011"
 *      |  |  |- list-123 - JSON serialized BlobStoreIndexShardSnapshot for snapshot "20131011"
 *      |  |
 *      |  |- 1/ - data for shard "1" of index "foo"
 *      |  |  |- __1
 *      |  |  .....
 *      |  |
 *      |  |-2/
 *      |  ......
 *      |
 *      |- 1xB0D8_B3y/ - data for index "bar" which was assigned the unique id of 1xB0D8_B3y in the repository
 *      ......
 * }
 * </pre>
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository {

    private BlobContainer snapshotsBlobContainer;

    protected final RepositoryMetaData metadata;

    protected final NamedXContentRegistry namedXContentRegistry;

    private static final int BUFFER_SIZE = 4096;

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_CODEC = "snapshot";

    private static final String INDEX_FILE_PREFIX = "index-";

    private static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String INCOMPATIBLE_SNAPSHOTS_BLOB = "incompatible-snapshots";

    private static final String TESTS_FILE = "tests-";

    private static final String METADATA_NAME_FORMAT = "meta-%s.dat";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    private static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s.dat";

    private static final String SNAPSHOT_INDEX_PREFIX = "index-";

    private static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    private static final String SNAPSHOT_INDEX_CODEC = "snapshots";

    private static final String DATA_BLOB_PREFIX = "__";

    private final RateLimiter snapshotRateLimiter;

    private final RateLimiter restoreRateLimiter;

    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    private ChecksumBlobStoreFormat<MetaData> globalMetaDataFormat;

    private ChecksumBlobStoreFormat<IndexMetaData> indexMetaDataFormat;

    private ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat;

    private final boolean readOnly;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> indexShardSnapshotsFormat;

    /**
     * Constructs new BlobStoreRepository
     *
     * @param metadata       The metadata for this repository including name and settings
     * @param globalSettings Settings for the node this repository object is created on
     */
    protected BlobStoreRepository(RepositoryMetaData metadata, Settings globalSettings, NamedXContentRegistry namedXContentRegistry) {
        super(globalSettings);
        this.metadata = metadata;
        this.namedXContentRegistry = namedXContentRegistry;
        snapshotRateLimiter = getRateLimiter(metadata.settings(), "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(metadata.settings(), "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        readOnly = metadata.settings().getAsBoolean("readonly", false);

        indexShardSnapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
            BlobStoreIndexShardSnapshot::fromXContent, namedXContentRegistry, isCompress());
        indexShardSnapshotsFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_INDEX_CODEC, SNAPSHOT_INDEX_NAME_FORMAT,
            BlobStoreIndexShardSnapshots::fromXContent, namedXContentRegistry, isCompress());
        ByteSizeValue chunkSize = chunkSize();
        if (chunkSize != null && chunkSize.getBytes() <= 0) {
            throw new IllegalArgumentException("the chunk size cannot be negative: [" + chunkSize + "]");
        }
    }

    @Override
    protected void doStart() {
        this.snapshotsBlobContainer = blobStore().blobContainer(basePath());
        globalMetaDataFormat = new ChecksumBlobStoreFormat<>(METADATA_CODEC, METADATA_NAME_FORMAT,
            MetaData::fromXContent, namedXContentRegistry, isCompress());
        indexMetaDataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT,
            IndexMetaData::fromXContent, namedXContentRegistry, isCompress());
        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
            SnapshotInfo::fromXContent, namedXContentRegistry, isCompress());
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        try {
            blobStore().close();
        } catch (Exception t) {
            logger.warn("cannot close blob store", t);
        }
    }

    /**
     * Returns the BlobStore to read and write data.
     */
    protected abstract BlobStore blobStore();

    /**
     * Returns base path of the repository
     */
    protected abstract BlobPath basePath();

    /**
     * Returns true if metadata and snapshot files should be compressed
     *
     * @return true if compression is needed
     */
    protected boolean isCompress() {
        return false;
    }

    /**
     * Returns data file chunk size.
     * <p>
     * This method should return null if no chunking is needed.
     *
     * @return chunk size
     */
    protected ByteSizeValue chunkSize() {
        return null;
    }

    @Override
    public RepositoryMetaData getMetadata() {
        return metadata;
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetaData) {
        if (isReadOnly()) {
            throw new RepositoryException(metadata.name(), "cannot create snapshot in a readonly repository");
        }
        try {
            final String snapshotName = snapshotId.getName();
            // check if the snapshot name already exists in the repository
            final RepositoryData repositoryData = getRepositoryData();
            if (repositoryData.getAllSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                throw new InvalidSnapshotNameException(metadata.name(), snapshotId.getName(), "snapshot with the same name already exists");
            }
            if (snapshotFormat.exists(snapshotsBlobContainer, snapshotId.getUUID())) {
                throw new InvalidSnapshotNameException(metadata.name(), snapshotId.getName(), "snapshot with the same name already exists");
            }

            // Write Global MetaData
            globalMetaDataFormat.write(clusterMetaData, snapshotsBlobContainer, snapshotId.getUUID());

            // write the index metadata for each index in the snapshot
            for (IndexId index : indices) {
                final IndexMetaData indexMetaData = clusterMetaData.index(index.getName());
                final BlobPath indexPath = basePath().add("indices").add(index.getId());
                final BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                indexMetaDataFormat.write(indexMetaData, indexMetaDataBlobContainer, snapshotId.getUUID());
            }
        } catch (IOException ex) {
            throw new SnapshotCreationException(metadata.name(), snapshotId, ex);
        }
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        if (isReadOnly()) {
            throw new RepositoryException(metadata.name(), "cannot delete snapshot from a readonly repository");
        }
        final RepositoryData repositoryData = getRepositoryData();
        List<String> indices = Collections.emptyList();
        SnapshotInfo snapshot = null;
        try {
            snapshot = getSnapshotInfo(snapshotId);
            indices = snapshot.indices();
        } catch (SnapshotMissingException ex) {
            throw ex;
        } catch (IllegalStateException | SnapshotException | ElasticsearchParseException ex) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("cannot read snapshot file [{}]", snapshotId), ex);
        }
        MetaData metaData = null;
        try {
            if (snapshot != null) {
                metaData = readSnapshotMetaData(snapshotId, snapshot.version(), repositoryData.resolveIndices(indices), true);
            } else {
                metaData = readSnapshotMetaData(snapshotId, null, repositoryData.resolveIndices(indices), true);
            }
        } catch (IOException | SnapshotException ex) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("cannot read metadata for snapshot [{}]", snapshotId), ex);
        }
        try {
            // Delete snapshot from the index file, since it is the maintainer of truth of active snapshots
            final RepositoryData updatedRepositoryData = repositoryData.removeSnapshot(snapshotId);
            writeIndexGen(updatedRepositoryData, repositoryStateId);

            // delete the snapshot file
            deleteSnapshotBlobIgnoringErrors(snapshot, snapshotId.getUUID());
            // delete the global metadata file
            deleteGlobalMetaDataBlobIgnoringErrors(snapshot, snapshotId.getUUID());

            // Now delete all indices
            for (String index : indices) {
                final IndexId indexId = repositoryData.resolveIndexId(index);
                BlobPath indexPath = basePath().add("indices").add(indexId.getId());
                BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                try {
                    indexMetaDataFormat.delete(indexMetaDataBlobContainer, snapshotId.getUUID());
                } catch (IOException ex) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to delete metadata for index [{}]", snapshotId, index), ex);
                }
                if (metaData != null) {
                    IndexMetaData indexMetaData = metaData.index(index);
                    if (indexMetaData != null) {
                        for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                            try {
                                delete(snapshotId, snapshot.version(), indexId, new ShardId(indexMetaData.getIndex(), shardId));
                            } catch (SnapshotException ex) {
                                final int finalShardId = shardId;
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to delete shard data for shard [{}][{}]", snapshotId, index, finalShardId), ex);
                            }
                        }
                    }
                }
            }

            // cleanup indices that are no longer part of the repository
            final Collection<IndexId> indicesToCleanUp = Sets.newHashSet(repositoryData.getIndices().values());
            indicesToCleanUp.removeAll(updatedRepositoryData.getIndices().values());
            final BlobContainer indicesBlobContainer = blobStore().blobContainer(basePath().add("indices"));
            for (final IndexId indexId : indicesToCleanUp) {
                try {
                    indicesBlobContainer.deleteBlob(indexId.getId());
                } catch (DirectoryNotEmptyException dnee) {
                    // if the directory isn't empty for some reason, it will fail to clean up;
                    // we'll ignore that and accept that cleanup didn't fully succeed.
                    // since we are using UUIDs for path names, this won't be an issue for
                    // snapshotting indices of the same name
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " +
                            "its index folder due to the directory not being empty.", metadata.name(), indexId), dnee);
                } catch (IOException ioe) {
                    // a different IOException occurred while trying to delete - will just log the issue for now
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " +
                            "its index folder.", metadata.name(), indexId), ioe);
                }
            }
        } catch (IOException | ResourceNotFoundException ex) {
            throw new RepositoryException(metadata.name(), "failed to delete snapshot [" + snapshotId + "]", ex);
        }
    }

    private void deleteSnapshotBlobIgnoringErrors(final SnapshotInfo snapshotInfo, final String blobId) {
        try {
            snapshotFormat.delete(snapshotsBlobContainer, blobId);
        } catch (IOException e) {
            if (snapshotInfo != null) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] Unable to delete snapshot file [{}]",
                    snapshotInfo.snapshotId(), blobId), e);
            } else {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Unable to delete snapshot file [{}]", blobId), e);
            }
        }
    }

    private void deleteGlobalMetaDataBlobIgnoringErrors(final SnapshotInfo snapshotInfo, final String blobId) {
        try {
            globalMetaDataFormat.delete(snapshotsBlobContainer, blobId);
        } catch (IOException e) {
            if (snapshotInfo != null) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] Unable to delete global metadata file [{}]",
                    snapshotInfo.snapshotId(), blobId), e);
            } else {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Unable to delete global metadata file [{}]", blobId), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SnapshotInfo finalizeSnapshot(final SnapshotId snapshotId,
                                         final List<IndexId> indices,
                                         final long startTime,
                                         final String failure,
                                         final int totalShards,
                                         final List<SnapshotShardFailure> shardFailures,
                                         final long repositoryStateId) {

        SnapshotInfo blobStoreSnapshot = new SnapshotInfo(snapshotId,
            indices.stream().map(IndexId::getName).collect(Collectors.toList()),
            startTime, failure, System.currentTimeMillis(), totalShards, shardFailures);
        try {
            snapshotFormat.write(blobStoreSnapshot, snapshotsBlobContainer, snapshotId.getUUID());
            final RepositoryData repositoryData = getRepositoryData();
            writeIndexGen(repositoryData.addSnapshot(snapshotId, blobStoreSnapshot.state(), indices), repositoryStateId);
        } catch (FileAlreadyExistsException ex) {
            // if another master was elected and took over finalizing the snapshot, it is possible
            // that both nodes try to finalize the snapshot and write to the same blobs, so we just
            // log a warning here and carry on
            throw new RepositoryException(metadata.name(), "Blob already exists while " +
                "finalizing snapshot, assume the snapshot has already been saved", ex);
        } catch (IOException ex) {
            throw new RepositoryException(metadata.name(), "failed to update snapshot in repository", ex);
        }
        return blobStoreSnapshot;
    }

    @Override
    public MetaData getSnapshotMetaData(SnapshotInfo snapshot, List<IndexId> indices) throws IOException {
        return readSnapshotMetaData(snapshot.snapshotId(), snapshot.version(), indices, false);
    }

    @Override
    public SnapshotInfo getSnapshotInfo(final SnapshotId snapshotId) {
        try {
            return snapshotFormat.read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException | NotXContentException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex);
        }
    }

    private MetaData readSnapshotMetaData(SnapshotId snapshotId, Version snapshotVersion, List<IndexId> indices, boolean ignoreIndexErrors) throws IOException {
        MetaData metaData;
        if (snapshotVersion == null) {
            // When we delete corrupted snapshots we might not know which version we are dealing with
            // We can try detecting the version based on the metadata file format
            assert ignoreIndexErrors;
            if (globalMetaDataFormat.exists(snapshotsBlobContainer, snapshotId.getUUID()) == false) {
                throw new SnapshotMissingException(metadata.name(), snapshotId);
            }
        }
        try {
            metaData = globalMetaDataFormat.read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex);
        }
        MetaData.Builder metaDataBuilder = MetaData.builder(metaData);
        for (IndexId index : indices) {
            BlobPath indexPath = basePath().add("indices").add(index.getId());
            BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
            try {
                metaDataBuilder.put(indexMetaDataFormat.read(indexMetaDataBlobContainer, snapshotId.getUUID()), false);
            } catch (ElasticsearchParseException | IOException ex) {
                if (ignoreIndexErrors) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] failed to read metadata for index", snapshotId, index.getName()), ex);
                } else {
                    throw ex;
                }
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Configures RateLimiter based on repository and global settings
     *
     * @param repositorySettings repository settings
     * @param setting            setting to use to configure rate limiter
     * @param defaultRate        default limiting rate
     * @return rate limiter or null of no throttling is needed
     */
    private RateLimiter getRateLimiter(Settings repositorySettings, String setting, ByteSizeValue defaultRate) {
        ByteSizeValue maxSnapshotBytesPerSec = repositorySettings.getAsBytesSize(setting,
                settings.getAsBytesSize(setting, defaultRate));
        if (maxSnapshotBytesPerSec.getBytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.getMbFrac());
        }
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    @Override
    public String startVerification() {
        try {
            if (isReadOnly()) {
                // It's readonly - so there is not much we can do here to verify it
                return null;
            } else {
                String seed = UUIDs.randomBase64UUID();
                byte[] testBytes = Strings.toUTF8Bytes(seed);
                BlobContainer testContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
                String blobName = "master.dat";
                BytesArray bytes = new BytesArray(testBytes);
                try (InputStream stream = bytes.streamInput()) {
                    testContainer.writeBlob(blobName + "-temp", stream, bytes.length());
                }
                // Make sure that move is supported
                testContainer.move(blobName + "-temp", blobName);
                return seed;
            }
        } catch (IOException exp) {
            throw new RepositoryVerificationException(metadata.name(), "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        if (isReadOnly()) {
            throw new UnsupportedOperationException("shouldn't be called");
        }
        try {
            blobStore().delete(basePath().add(testBlobPrefix(seed)));
        } catch (IOException exp) {
            throw new RepositoryVerificationException(metadata.name(), "cannot delete test data at " + basePath(), exp);
        }
    }

    @Override
    public RepositoryData getRepositoryData() {
        try {
            final long indexGen = latestIndexBlobId();
            final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + Long.toString(indexGen);

            RepositoryData repositoryData;
            try (InputStream blob = snapshotsBlobContainer.readBlob(snapshotsIndexBlobName)) {
                BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(blob, out);
                // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, out.bytes(), XContentType.JSON)) {
                    repositoryData = RepositoryData.snapshotsFromXContent(parser, indexGen);
                } catch (NotXContentException e) {
                    logger.warn("[{}] index blob is not valid x-content [{} bytes]", snapshotsIndexBlobName, out.bytes().length());
                    throw e;
                }
            }

            // now load the incompatible snapshot ids, if they exist
            try (InputStream blob = snapshotsBlobContainer.readBlob(INCOMPATIBLE_SNAPSHOTS_BLOB)) {
                BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(blob, out);
                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, out.bytes(), XContentType.JSON)) {
                    repositoryData = repositoryData.incompatibleSnapshotsFromXContent(parser);
                }
            } catch (NoSuchFileException e) {
                if (isReadOnly()) {
                    logger.debug("[{}] Incompatible snapshots blob [{}] does not exist, the likely " +
                                 "reason is that there are no incompatible snapshots in the repository",
                                 metadata.name(), INCOMPATIBLE_SNAPSHOTS_BLOB);
                } else {
                    // write an empty incompatible-snapshots blob - we do this so that there
                    // is a blob present, which helps speed up some cloud-based repositories
                    // (e.g. S3), which retry if a blob is missing with exponential backoff,
                    // delaying the read of repository data and sometimes causing a timeout
                    writeIncompatibleSnapshots(RepositoryData.EMPTY);
                }
            }
            return repositoryData;
        } catch (NoSuchFileException ex) {
            // repository doesn't have an index blob, its a new blank repo
            return RepositoryData.EMPTY;
        } catch (IOException ioe) {
            throw new RepositoryException(metadata.name(), "could not read repository data from index blob", ioe);
        }
    }

    public static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    // package private, only use for testing
    BlobContainer blobContainer() {
        return snapshotsBlobContainer;
    }

    protected void writeIndexGen(final RepositoryData repositoryData, final long repositoryStateId) throws IOException {
        assert isReadOnly() == false; // can not write to a read only repository
        final long currentGen = latestIndexBlobId();
        if (repositoryStateId != SnapshotsInProgress.UNDEFINED_REPOSITORY_STATE_ID && currentGen != repositoryStateId) {
            // the index file was updated by a concurrent operation, so we were operating on stale
            // repository data
            throw new RepositoryException(metadata.name(), "concurrent modification of the index-N file, expected current generation [" +
                                              repositoryStateId + "], actual current generation [" + currentGen +
                                              "] - possibly due to simultaneous snapshot deletion requests");
        }
        final long newGen = currentGen + 1;
        final BytesReference snapshotsBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                repositoryData.snapshotsToXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
            }
            snapshotsBytes = bStream.bytes();
        }
        // write the index file
        final String indexBlob = INDEX_FILE_PREFIX + Long.toString(newGen);
        logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);
        writeAtomic(indexBlob, snapshotsBytes);
        // delete the N-2 index file if it exists, keep the previous one around as a backup
        if (isReadOnly() == false && newGen - 2 >= 0) {
            final String oldSnapshotIndexFile = INDEX_FILE_PREFIX + Long.toString(newGen - 2);
            if (snapshotsBlobContainer.blobExists(oldSnapshotIndexFile)) {
                snapshotsBlobContainer.deleteBlob(oldSnapshotIndexFile);
            }
        }

        // write the current generation to the index-latest file
        final BytesReference genBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            bStream.writeLong(newGen);
            genBytes = bStream.bytes();
        }
        if (snapshotsBlobContainer.blobExists(INDEX_LATEST_BLOB)) {
            snapshotsBlobContainer.deleteBlob(INDEX_LATEST_BLOB);
        }
        logger.debug("Repository [{}] updating index.latest with generation [{}]", metadata.name(), newGen);
        writeAtomic(INDEX_LATEST_BLOB, genBytes);
    }

    /**
     * Writes the incompatible snapshot ids list to the `incompatible-snapshots` blob in the repository.
     *
     * Package private for testing.
     */
    void writeIncompatibleSnapshots(RepositoryData repositoryData) throws IOException {
        assert isReadOnly() == false; // can not write to a read only repository
        final BytesReference bytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                repositoryData.incompatibleSnapshotsToXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
            }
            bytes = bStream.bytes();
        }
        if (snapshotsBlobContainer.blobExists(INCOMPATIBLE_SNAPSHOTS_BLOB)) {
            snapshotsBlobContainer.deleteBlob(INCOMPATIBLE_SNAPSHOTS_BLOB);
        }
        // write the incompatible snapshots blob
        writeAtomic(INCOMPATIBLE_SNAPSHOTS_BLOB, bytes);
    }

    /**
     * Get the latest snapshot index blob id.  Snapshot index blobs are named index-N, where N is
     * the next version number from when the index blob was written.  Each individual index-N blob is
     * only written once and never overwritten.  The highest numbered index-N blob is the latest one
     * that contains the current snapshots in the repository.
     *
     * Package private for testing
     */
    long latestIndexBlobId() throws IOException {
        try {
            // First, try listing all index-N blobs (there should only be two index-N blobs at any given
            // time in a repository if cleanup is happening properly) and pick the index-N blob with the
            // highest N value - this will be the latest index blob for the repository.  Note, we do this
            // instead of directly reading the index.latest blob to get the current index-N blob because
            // index.latest is not written atomically and is not immutable - on every index-N change,
            // we first delete the old index.latest and then write the new one.  If the repository is not
            // read-only, it is possible that we try deleting the index.latest blob while it is being read
            // by some other operation (such as the get snapshots operation).  In some file systems, it is
            // illegal to delete a file while it is being read elsewhere (e.g. Windows).  For read-only
            // repositories, we read for index.latest, both because listing blob prefixes is often unsupported
            // and because the index.latest blob will never be deleted and re-written.
            return listBlobsToGetLatestIndexId();
        } catch (UnsupportedOperationException e) {
            // If its a read-only repository, listing blobs by prefix may not be supported (e.g. a URL repository),
            // in this case, try reading the latest index generation from the index.latest blob
            try {
                return readSnapshotIndexLatestBlob();
            } catch (NoSuchFileException nsfe) {
                return RepositoryData.EMPTY_REPO_GEN;
            }
        }
    }

    // package private for testing
    long readSnapshotIndexLatestBlob() throws IOException {
        try (InputStream blob = snapshotsBlobContainer.readBlob(INDEX_LATEST_BLOB)) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            return Numbers.bytesToLong(out.bytes().toBytesRef());
        }
    }

    private long listBlobsToGetLatestIndexId() throws IOException {
        Map<String, BlobMetaData> blobs = snapshotsBlobContainer.listBlobsByPrefix(INDEX_FILE_PREFIX);
        long latest = RepositoryData.EMPTY_REPO_GEN;
        if (blobs.isEmpty()) {
            // no snapshot index blobs have been written yet
            return latest;
        }
        for (final BlobMetaData blobMetaData : blobs.values()) {
            final String blobName = blobMetaData.name();
            try {
                final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
                latest = Math.max(latest, curr);
            } catch (NumberFormatException nfe) {
                // the index- blob wasn't of the format index-N where N is a number,
                // no idea what this blob is but it doesn't belong in the repository!
                logger.debug("[{}] Unknown blob in the repository: {}", metadata.name(), blobName);
            }
        }
        return latest;
    }

    private void writeAtomic(final String blobName, final BytesReference bytesRef) throws IOException {
        final String tempBlobName = "pending-" + blobName + "-" + UUIDs.randomBase64UUID();
        try (InputStream stream = bytesRef.streamInput()) {
            snapshotsBlobContainer.writeBlob(tempBlobName, stream, bytesRef.length());
            snapshotsBlobContainer.move(tempBlobName, blobName);
        } catch (IOException ex) {
            // temporary blob creation or move failed - try cleaning up
            try {
                snapshotsBlobContainer.deleteBlob(tempBlobName);
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        }
    }

    @Override
    public void snapshotShard(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
        SnapshotContext snapshotContext = new SnapshotContext(shard, snapshotId, indexId, snapshotStatus);
        snapshotStatus.startTime(System.currentTimeMillis());

        try {
            snapshotContext.snapshot(snapshotIndexCommit);
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
            snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.DONE);
        } catch (Exception e) {
            snapshotStatus.time(System.currentTimeMillis() - snapshotStatus.startTime());
            snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FAILURE);
            snapshotStatus.failure(ExceptionsHelper.detailedMessage(e));
            if (e instanceof IndexShardSnapshotFailedException) {
                throw (IndexShardSnapshotFailedException) e;
            } else {
                throw new IndexShardSnapshotFailedException(shard.shardId(), e);
            }
        }
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
        final RestoreContext snapshotContext = new RestoreContext(shard, snapshotId, version, indexId, snapshotShardId, recoveryState);
        try {
            snapshotContext.restore();
        } catch (Exception e) {
            throw new IndexShardRestoreFailedException(shard.shardId(), "failed to restore snapshot [" + snapshotId + "]", e);
        }
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        Context context = new Context(snapshotId, version, indexId, shardId);
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
    public void verify(String seed, DiscoveryNode localNode) {
        BlobContainer testBlobContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
        if (testBlobContainer.blobExists("master.dat")) {
            try  {
                BytesArray bytes = new BytesArray(seed);
                try (InputStream stream = bytes.streamInput()) {
                    testBlobContainer.writeBlob("data-" + localNode.getId() + ".dat", stream, bytes.length());
                }
            } catch (IOException exp) {
                throw new RepositoryVerificationException(metadata.name(), "store location [" + blobStore() + "] is not accessible on the node [" + localNode + "]", exp);
            }
        } else {
            throw new RepositoryVerificationException(metadata.name(), "a file written by master to the store [" + blobStore() + "] cannot be accessed on the node [" + localNode + "]. "
                + "This might indicate that the store [" + blobStore() + "] is not shared between this node and the master node or "
                + "that permissions on the store don't allow reading files written by the master node");
        }
    }

    /**
     * Delete shard snapshot
     *
     * @param snapshotId snapshot id
     * @param shardId    shard id
     */
    private void delete(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        Context context = new Context(snapshotId, version, indexId, shardId, shardId);
        context.delete();
    }

    @Override
    public String toString() {
        return "BlobStoreRepository[" +
            "[" + metadata.name() +
            "], [" + blobStore() + ']' +
            ']';
    }

    BlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat(Version version) {
        return indexShardSnapshotFormat;
    }

    /**
     * Context for snapshot/restore operations
     */
    private class Context {

        protected final SnapshotId snapshotId;

        protected final ShardId shardId;

        protected final BlobContainer blobContainer;

        protected final Version version;

        Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            this(snapshotId, version, indexId, shardId, shardId);
        }

        Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId, ShardId snapshotShardId) {
            this.snapshotId = snapshotId;
            this.version = version;
            this.shardId = shardId;
            blobContainer = blobStore().blobContainer(basePath().add("indices").add(indexId.getId()).add(Integer.toString(snapshotShardId.getId())));
        }

        /**
         * Delete shard snapshot
         */
        public void delete() {
            final Map<String, BlobMetaData> blobs;
            try {
                blobs = blobContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardSnapshotException(shardId, "Failed to list content of gateway", e);
            }

            Tuple<BlobStoreIndexShardSnapshots, Integer> tuple = buildBlobStoreIndexShardSnapshots(blobs);
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            int fileListGeneration = tuple.v2();

            try {
                indexShardSnapshotFormat(version).delete(blobContainer, snapshotId.getUUID());
            } catch (IOException e) {
                logger.debug("[{}] [{}] failed to delete shard snapshot file", shardId, snapshotId);
            }

            // Build a list of snapshots that should be preserved
            List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
            for (SnapshotFiles point : snapshots) {
                if (!point.snapshot().equals(snapshotId.getName())) {
                    newSnapshotsList.add(point);
                }
            }
            // finalize the snapshot and rewrite the snapshot index with the next sequential snapshot index
            finalize(newSnapshotsList, fileListGeneration + 1, blobs);
        }

        /**
         * Loads information about shard snapshot
         */
        public BlobStoreIndexShardSnapshot loadSnapshot() {
            try {
                return indexShardSnapshotFormat(version).read(blobContainer, snapshotId.getUUID());
            } catch (IOException ex) {
                throw new SnapshotException(metadata.name(), snapshotId, "failed to read shard snapshot file for " + shardId, ex);
            }
        }

        /**
         * Removes all unreferenced files from the repository and writes new index file
         *
         * We need to be really careful in handling index files in case of failures to make sure we have index file that
         * points to files that were deleted.
         *
         *
         * @param snapshots list of active snapshots in the container
         * @param fileListGeneration the generation number of the snapshot index file
         * @param blobs     list of blobs in the container
         */
        protected void finalize(List<SnapshotFiles> snapshots, int fileListGeneration, Map<String, BlobMetaData> blobs) {
            BlobStoreIndexShardSnapshots newSnapshots = new BlobStoreIndexShardSnapshots(snapshots);
            // delete old index files first
            for (String blobName : blobs.keySet()) {
                if (indexShardSnapshotsFormat.isTempBlobName(blobName) || blobName.startsWith(SNAPSHOT_INDEX_PREFIX)) {
                    try {
                        blobContainer.deleteBlob(blobName);
                    } catch (IOException e) {
                        // We cannot delete index file - this is fatal, we cannot continue, otherwise we might end up
                        // with references to non-existing files
                        throw new IndexShardSnapshotFailedException(shardId, "error deleting index file ["
                            + blobName + "] during cleanup", e);
                    }
                }
            }

            // now go over all the blobs, and if they don't exist in a snapshot, delete them
            for (String blobName : blobs.keySet()) {
                // delete unused files
                if (blobName.startsWith(DATA_BLOB_PREFIX)) {
                    if (newSnapshots.findNameFile(BlobStoreIndexShardSnapshot.FileInfo.canonicalName(blobName)) == null) {
                        try {
                            blobContainer.deleteBlob(blobName);
                        } catch (IOException e) {
                            // TODO: don't catch and let the user handle it?
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] error deleting blob [{}] during cleanup", snapshotId, shardId, blobName), e);
                        }
                    }
                }
            }

            // If we deleted all snapshots - we don't need to create the index file
            if (snapshots.size() > 0) {
                try {
                    indexShardSnapshotsFormat.writeAtomic(newSnapshots, blobContainer, Integer.toString(fileListGeneration));
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write file list", e);
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
            return DATA_BLOB_PREFIX + Long.toString(generation, Character.MAX_RADIX);
        }

        /**
         * Finds the next available blob number
         *
         * @param blobs list of blobs in the repository
         * @return next available blob number
         */
        protected long findLatestFileNameGeneration(Map<String, BlobMetaData> blobs) {
            long generation = -1;
            for (String name : blobs.keySet()) {
                if (!name.startsWith(DATA_BLOB_PREFIX)) {
                    continue;
                }
                name = BlobStoreIndexShardSnapshot.FileInfo.canonicalName(name);
                try {
                    long currentGen = Long.parseLong(name.substring(DATA_BLOB_PREFIX.length()), Character.MAX_RADIX);
                    if (currentGen > generation) {
                        generation = currentGen;
                    }
                } catch (NumberFormatException e) {
                    logger.warn("file [{}] does not conform to the '{}' schema", name, DATA_BLOB_PREFIX);
                }
            }
            return generation;
        }

        /**
         * Loads all available snapshots in the repository
         *
         * @param blobs list of blobs in repository
         * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
         */
        protected Tuple<BlobStoreIndexShardSnapshots, Integer> buildBlobStoreIndexShardSnapshots(Map<String, BlobMetaData> blobs) {
            int latest = -1;
            Set<String> blobKeys = blobs.keySet();
            for (String name : blobKeys) {
                if (name.startsWith(SNAPSHOT_INDEX_PREFIX)) {
                    try {
                        int gen = Integer.parseInt(name.substring(SNAPSHOT_INDEX_PREFIX.length()));
                        if (gen > latest) {
                            latest = gen;
                        }
                    } catch (NumberFormatException ex) {
                        logger.warn("failed to parse index file name [{}]", name);
                    }
                }
            }
            if (latest >= 0) {
                try {
                    final BlobStoreIndexShardSnapshots shardSnapshots =
                        indexShardSnapshotsFormat.read(blobContainer, Integer.toString(latest));
                    return new Tuple<>(shardSnapshots, latest);
                } catch (IOException e) {
                    final String file = SNAPSHOT_INDEX_PREFIX + latest;
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to read index file [{}]", file), e);
                }
            } else if (blobKeys.isEmpty() == false) {
                logger.debug("Could not find a readable index-N file in a non-empty shard snapshot directory [{}]", blobContainer.path());
            }

            // We couldn't load the index file - falling back to loading individual snapshots
            List<SnapshotFiles> snapshots = new ArrayList<>();
            for (String name : blobKeys) {
                try {
                    BlobStoreIndexShardSnapshot snapshot = null;
                    if (name.startsWith(SNAPSHOT_PREFIX)) {
                        snapshot = indexShardSnapshotFormat.readBlob(blobContainer, name);
                    }
                    if (snapshot != null) {
                        snapshots.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles()));
                    }
                } catch (IOException e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to read commit point [{}]", name), e);
                }
            }
            return new Tuple<>(new BlobStoreIndexShardSnapshots(snapshots), -1);
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
         * @param shard          shard to be snapshotted
         * @param snapshotId     snapshot id
         * @param indexId        the id of the index being snapshotted
         * @param snapshotStatus snapshot status to report progress
         */
        SnapshotContext(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexShardSnapshotStatus snapshotStatus) {
            super(snapshotId, Version.CURRENT, indexId, shard.shardId());
            this.snapshotStatus = snapshotStatus;
            this.store = shard.store();
        }

        /**
         * Create snapshot from index commit point
         *
         * @param snapshotIndexCommit snapshot commit point
         */
        public void snapshot(IndexCommit snapshotIndexCommit) {
            logger.debug("[{}] [{}] snapshot to [{}] ...", shardId, snapshotId, metadata.name());
            store.incRef();
            try {
                final Map<String, BlobMetaData> blobs;
                try {
                    blobs = blobContainer.listBlobs();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }

                long generation = findLatestFileNameGeneration(blobs);
                Tuple<BlobStoreIndexShardSnapshots, Integer> tuple = buildBlobStoreIndexShardSnapshots(blobs);
                BlobStoreIndexShardSnapshots snapshots = tuple.v1();
                int fileListGeneration = tuple.v2();

                final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles = new ArrayList<>();

                int indexNumberOfFiles = 0;
                long indexTotalFilesSize = 0;
                ArrayList<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot = new ArrayList<>();
                final Store.MetadataSnapshot metadata;
                // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                final Collection<String> fileNames;
                try {
                    metadata = store.getMetadata(snapshotIndexCommit);
                    fileNames = snapshotIndexCommit.getFileNames();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                }
                for (String fileName : fileNames) {
                    if (snapshotStatus.aborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }
                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetaData md = metadata.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = null;
                    List<BlobStoreIndexShardSnapshot.FileInfo> filesInfo = snapshots.findPhysicalIndexFiles(fileName);
                    if (filesInfo != null) {
                        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : filesInfo) {
                            try {
                                // in 1.3.3 we added additional hashes for .si / segments_N files
                                // to ensure we don't double the space in the repo since old snapshots
                                // don't have this hash we try to read that hash from the blob store
                                // in a bwc compatible way.
                                maybeRecalculateMetadataHash(blobContainer, fileInfo, metadata);
                            } catch (Exception e) {
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} Can't calculate hash from blob for file [{}] [{}]", shardId, fileInfo.physicalName(), fileInfo.metadata()), e);
                            }
                            if (fileInfo.isSame(md) && snapshotFileExistsInBlobs(fileInfo, blobs)) {
                                // a commit point file with the same name, size and checksum was already copied to repository
                                // we will reuse it for this snapshot
                                existingFileInfo = fileInfo;
                                break;
                            }
                        }
                    }
                    if (existingFileInfo == null) {
                        indexNumberOfFiles++;
                        indexTotalFilesSize += md.length();
                        // create a new FileInfo
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(fileNameFromGeneration(++generation), md, chunkSize());
                        indexCommitPointFiles.add(snapshotFileInfo);
                        filesToSnapshot.add(snapshotFileInfo);
                    } else {
                        indexCommitPointFiles.add(existingFileInfo);
                    }
                }

                snapshotStatus.files(indexNumberOfFiles, indexTotalFilesSize);

                if (snapshotStatus.aborted()) {
                    logger.debug("[{}] [{}] Aborted during initialization", shardId, snapshotId);
                    throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                }

                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.STARTED);

                for (BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo : filesToSnapshot) {
                    try {
                        snapshotFile(snapshotFileInfo);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to perform snapshot (index files)", e);
                    }
                }

                snapshotStatus.indexVersion(snapshotIndexCommit.getGeneration());
                // now create and write the commit point
                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FINALIZE);

                BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(snapshotId.getName(),
                    snapshotIndexCommit.getGeneration(), indexCommitPointFiles, snapshotStatus.startTime(),
                    // snapshotStatus.startTime() is assigned on the same machine, so it's safe to use with VLong
                    System.currentTimeMillis() - snapshotStatus.startTime(), indexNumberOfFiles, indexTotalFilesSize);
                //TODO: The time stored in snapshot doesn't include cleanup time.
                logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                try {
                    indexShardSnapshotFormat.write(snapshot, blobContainer, snapshotId.getUUID());
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }

                // delete all files that are not referenced by any commit point
                // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
                List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
                newSnapshotsList.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles()));
                for (SnapshotFiles point : snapshots) {
                    newSnapshotsList.add(point);
                }
                // finalize the snapshot and rewrite the snapshot index with the next sequential snapshot index
                finalize(newSnapshotsList, fileListGeneration + 1, blobs);
                snapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.DONE);
            } finally {
                store.decRef();
            }
        }

        /**
         * Snapshot individual file
         * <p>
         * This is asynchronous method. Upon completion of the operation latch is getting counted down and any failures are
         * added to the {@code failures} list
         *
         * @param fileInfo file to be snapshotted
         */
        private void snapshotFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
            final String file = fileInfo.physicalName();
            try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {
                for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                    final long partBytes = fileInfo.partBytes(i);

                    final InputStreamIndexInput inputStreamIndexInput = new InputStreamIndexInput(indexInput, partBytes);
                    InputStream inputStream = inputStreamIndexInput;
                    if (snapshotRateLimiter != null) {
                        inputStream = new RateLimitingInputStream(inputStreamIndexInput, snapshotRateLimiter,
                                                                  snapshotRateLimitingTimeInNanos::inc);
                    }
                    inputStream = new AbortableInputStream(inputStream, fileInfo.physicalName());
                    blobContainer.writeBlob(fileInfo.partName(i), inputStream, partBytes);
                }
                Store.verify(indexInput);
                snapshotStatus.addProcessedFile(fileInfo.length());
            } catch (Exception t) {
                failStoreIfCorrupted(t);
                snapshotStatus.addProcessedFile(0);
                throw t;
            }
        }

        private void failStoreIfCorrupted(Exception e) {
            if (e instanceof CorruptIndexException || e instanceof IndexFormatTooOldException || e instanceof IndexFormatTooNewException) {
                try {
                    store.markStoreCorrupted((IOException) e);
                } catch (IOException inner) {
                    inner.addSuppressed(e);
                    logger.warn("store cannot be marked as corrupted", inner);
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
        private boolean snapshotFileExistsInBlobs(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Map<String, BlobMetaData> blobs) {
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

            AbortableInputStream(InputStream delegate, String fileName) {
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
     * This is a BWC layer to ensure we update the snapshots metadata with the corresponding hashes before we compare them.
     * The new logic for StoreFileMetaData reads the entire <tt>.si</tt> and <tt>segments.n</tt> files to strengthen the
     * comparison of the files on a per-segment / per-commit level.
     */
    private static void maybeRecalculateMetadataHash(final BlobContainer blobContainer, final BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store.MetadataSnapshot snapshot) throws Exception {
        final StoreFileMetaData metadata;
        if (fileInfo != null && (metadata = snapshot.get(fileInfo.physicalName())) != null) {
            if (metadata.hash().length > 0 && fileInfo.metadata().hash().length == 0) {
                // we have a hash - check if our repo has a hash too otherwise we have
                // to calculate it.
                // we might have multiple parts even though the file is small... make sure we read all of it.
                try (InputStream stream = new PartSliceStream(blobContainer, fileInfo)) {
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
        private final BlobStoreIndexShardSnapshot.FileInfo info;

        PartSliceStream(BlobContainer container, BlobStoreIndexShardSnapshot.FileInfo info) {
            super(info.numberOfParts());
            this.info = info;
            this.container = container;
        }

        @Override
        protected InputStream openSlice(long slice) throws IOException {
            return container.readBlob(info.partName(slice));
        }
    }

    /**
     * Context for restore operations
     */
    private class RestoreContext extends Context {

        private final IndexShard targetShard;

        private final RecoveryState recoveryState;

        /**
         * Constructs new restore context
         *
         * @param shard           shard to restore into
         * @param snapshotId      snapshot id
         * @param indexId         id of the index being restored
         * @param snapshotShardId shard in the snapshot that data should be restored from
         * @param recoveryState   recovery state to report progress
         */
        RestoreContext(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
            super(snapshotId, version, indexId, shard.shardId(), snapshotShardId);
            this.recoveryState = recoveryState;
            this.targetShard = shard;
        }

        /**
         * Performs restore operation
         */
        public void restore() throws IOException {
            final Store store = targetShard.store();
            store.incRef();
            try {
                logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, metadata.name(), shardId);
                BlobStoreIndexShardSnapshot snapshot = loadSnapshot();

                if (snapshot.indexFiles().size() == 1
                    && snapshot.indexFiles().get(0).physicalName().startsWith("segments_")
                    && snapshot.indexFiles().get(0).hasUnknownChecksum()) {
                    // If the shard has no documents, it will only contain a single segments_N file for the
                    // shard's snapshot.  If we are restoring a snapshot created by a previous supported version,
                    // it is still possible that in that version, an empty shard has a segments_N file with an unsupported
                    // version (and no checksum), because we don't know the Lucene version to assign segments_N until we
                    // have written some data.  Since the segments_N for an empty shard could have an incompatible Lucene
                    // version number and no checksum, even though the index itself is perfectly fine to restore, this
                    // empty shard would cause exceptions to be thrown.  Since there is no data to restore from an empty
                    // shard anyway, we just create the empty shard here and then exit.
                    IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig(null)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                        .setCommitOnClose(true));
                    writer.close();
                    return;
                }

                SnapshotFiles snapshotFiles = new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles());
                Store.MetadataSnapshot recoveryTargetMetadata;
                try {
                    // this will throw an IOException if the store has no segments infos file. The
                    // store can still have existing files but they will be deleted just before being
                    // restored.
                    recoveryTargetMetadata = targetShard.snapshotStoreMetadata();
                } catch (IndexNotFoundException e) {
                    // happens when restore to an empty shard, not a big deal
                    logger.trace("[{}] [{}] restoring from to an empty shard", shardId, snapshotId);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                } catch (IOException e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} Can't read metadata from store, will not reuse any local file while restoring", shardId), e);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                }

                final List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover = new ArrayList<>();
                final Map<String, StoreFileMetaData> snapshotMetaData = new HashMap<>();
                final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfos = new HashMap<>();
                for (final BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshot.indexFiles()) {
                    try {
                        // in 1.3.3 we added additional hashes for .si / segments_N files
                        // to ensure we don't double the space in the repo since old snapshots
                        // don't have this hash we try to read that hash from the blob store
                        // in a bwc compatible way.
                        maybeRecalculateMetadataHash(blobContainer, fileInfo, recoveryTargetMetadata);
                    } catch (Exception e) {
                        // if the index is broken we might not be able to read it
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} Can't calculate hash from blog for file [{}] [{}]", shardId, fileInfo.physicalName(), fileInfo.metadata()), e);
                    }
                    snapshotMetaData.put(fileInfo.metadata().name(), fileInfo.metadata());
                    fileInfos.put(fileInfo.metadata().name(), fileInfo);
                }

                final Store.MetadataSnapshot sourceMetaData = new Store.MetadataSnapshot(unmodifiableMap(snapshotMetaData), emptyMap(), 0);

                final StoreFileMetaData restoredSegmentsFile = sourceMetaData.getSegmentsFile();
                if (restoredSegmentsFile == null) {
                    throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
                }

                final Store.RecoveryDiff diff = sourceMetaData.recoveryDiff(recoveryTargetMetadata);
                for (StoreFileMetaData md : diff.identical) {
                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                    recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length(), true);
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] [{}] not_recovering [{}] from [{}], exists in local store and is same", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                    }
                }

                for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
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

                try {
                    // list of all existing store files
                    final List<String> deleteIfExistFiles = Arrays.asList(store.directory().listAll());

                    // restore the files from the snapshot to the Lucene store
                    for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
                        // if a file with a same physical name already exist in the store we need to delete it
                        // before restoring it from the snapshot. We could be lenient and try to reuse the existing
                        // store files (and compare their names/length/checksum again with the snapshot files) but to
                        // avoid extra complexity we simply delete them and restore them again like StoreRecovery
                        // does with dangling indices. Any existing store file that is not restored from the snapshot
                        // will be clean up by RecoveryTarget.cleanFiles().
                        final String physicalName = fileToRecover.physicalName();
                        if (deleteIfExistFiles.contains(physicalName)) {
                            logger.trace("[{}] [{}] deleting pre-existing file [{}]", shardId, snapshotId, physicalName);
                            store.directory().deleteFile(physicalName);
                        }

                        logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
                        restoreFile(fileToRecover, store);
                    }
                } catch (IOException ex) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", ex);
                }

                // read the snapshot data persisted
                final SegmentInfos segmentCommitInfos;
                try {
                    segmentCommitInfos = Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
                } catch (IOException e) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
                }
                recoveryState.getIndex().updateVersion(segmentCommitInfos.getVersion());

                /// now, go over and clean files that are in the store, but were not in the snapshot
                try {
                    for (String storeFile : store.directory().listAll()) {
                        if (Store.isAutogenerated(storeFile) || snapshotFiles.containPhysicalIndexFile(storeFile)) {
                            continue; //skip write.lock, checksum files and files that exist in the snapshot
                        }
                        try {
                            store.deleteQuiet("restore", storeFile);
                            store.directory().deleteFile(storeFile);
                        } catch (IOException e) {
                            logger.warn("[{}] failed to delete file [{}] during snapshot cleanup", snapshotId, storeFile);
                        }
                    }
                } catch (IOException e) {
                    logger.warn("[{}] failed to list directory - some of files might not be deleted", snapshotId);
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
         */
        private void restoreFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo, final Store store) throws IOException {
            boolean success = false;

            try (InputStream partSliceStream = new PartSliceStream(blobContainer, fileInfo)) {
                final InputStream stream;
                if (restoreRateLimiter == null) {
                    stream = partSliceStream;
                } else {
                    stream = new RateLimitingInputStream(partSliceStream, restoreRateLimiter, restoreRateLimitingTimeInNanos::inc);
                }

                try (IndexOutput indexOutput = store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int length;
                    while ((length = stream.read(buffer)) > 0) {
                        indexOutput.writeBytes(buffer, 0, length);
                        recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.name(), length);
                    }
                    Store.verify(indexOutput);
                    indexOutput.close();
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
}
