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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.ParseFieldMatcher;
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
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
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
 *   |- index-N           - list of all snapshot name as JSON array, N is the generation of the file
 *   |- index.latest      - contains the numeric value of the latest generation of the index file (i.e. N from above)
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

    private static final int BUFFER_SIZE = 4096;

    private static final String LEGACY_SNAPSHOT_PREFIX = "snapshot-";

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_CODEC = "snapshot";

    static final String SNAPSHOTS_FILE = "index"; // package private for unit testing

    private static final String INDEX_FILE_PREFIX = "index-";

    private static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String TESTS_FILE = "tests-";

    private static final String METADATA_NAME_FORMAT = "meta-%s.dat";

    private static final String LEGACY_METADATA_NAME_FORMAT = "metadata-%s";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    protected static final String LEGACY_SNAPSHOT_NAME_FORMAT = LEGACY_SNAPSHOT_PREFIX + "%s";

    protected static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s.dat";

    protected static final String SNAPSHOT_INDEX_PREFIX = "index-";

    protected static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    protected static final String SNAPSHOT_INDEX_CODEC = "snapshots";

    protected static final String DATA_BLOB_PREFIX = "__";

    private final RateLimiter snapshotRateLimiter;

    private final RateLimiter restoreRateLimiter;

    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    private ChecksumBlobStoreFormat<MetaData> globalMetaDataFormat;

    private LegacyBlobStoreFormat<MetaData> globalMetaDataLegacyFormat;

    private ChecksumBlobStoreFormat<IndexMetaData> indexMetaDataFormat;

    private LegacyBlobStoreFormat<IndexMetaData> indexMetaDataLegacyFormat;

    private ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat;

    private LegacyBlobStoreFormat<SnapshotInfo> snapshotLegacyFormat;

    private final boolean readOnly;

    private final ParseFieldMatcher parseFieldMatcher;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat;

    private final LegacyBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotLegacyFormat;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> indexShardSnapshotsFormat;

    /**
     * Constructs new BlobStoreRepository
     *
     * @param metadata       The metadata for this repository including name and settings
     * @param globalSettings Settings for the node this repository object is created on
     */
    protected BlobStoreRepository(RepositoryMetaData metadata, Settings globalSettings) {
        super(globalSettings);
        this.metadata = metadata;
        parseFieldMatcher = new ParseFieldMatcher(settings);
        snapshotRateLimiter = getRateLimiter(metadata.settings(), "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(metadata.settings(), "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        readOnly = metadata.settings().getAsBoolean("readonly", false);
        indexShardSnapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, BlobStoreIndexShardSnapshot.PROTO, parseFieldMatcher, isCompress());
        indexShardSnapshotLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, BlobStoreIndexShardSnapshot.PROTO, parseFieldMatcher);
        indexShardSnapshotsFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_INDEX_CODEC, SNAPSHOT_INDEX_NAME_FORMAT, BlobStoreIndexShardSnapshots.PROTO, parseFieldMatcher, isCompress());

    }

    @Override
    protected void doStart() {
        this.snapshotsBlobContainer = blobStore().blobContainer(basePath());

        ParseFieldMatcher parseFieldMatcher = new ParseFieldMatcher(settings);
        globalMetaDataFormat = new ChecksumBlobStoreFormat<>(METADATA_CODEC, METADATA_NAME_FORMAT, MetaData.PROTO, parseFieldMatcher, isCompress());
        globalMetaDataLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_METADATA_NAME_FORMAT, MetaData.PROTO, parseFieldMatcher);

        indexMetaDataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT, IndexMetaData.PROTO, parseFieldMatcher, isCompress());
        indexMetaDataLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, IndexMetaData.PROTO, parseFieldMatcher);

        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, SnapshotInfo.PROTO, parseFieldMatcher, isCompress());
        snapshotLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, SnapshotInfo.PROTO, parseFieldMatcher);
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
            if (getSnapshots().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                throw new SnapshotCreationException(metadata.name(), snapshotId, "snapshot with the same name already exists");
            }
            if (snapshotFormat.exists(snapshotsBlobContainer, snapshotId.getUUID()) ||
                    snapshotLegacyFormat.exists(snapshotsBlobContainer, snapshotName)) {
                throw new SnapshotCreationException(metadata.name(), snapshotId, "snapshot with such name already exists");
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

    // Older repository index files (index-N) only contain snapshot info, not indices info,
    // so if the repository data is of the older format, populate it with the indices entries
    // so we know which indices of snapshots have blob ids in the older format.
    private RepositoryData upgradeRepositoryData(final RepositoryData repositoryData) throws IOException {
        final Map<IndexId, Set<SnapshotId>> indexToSnapshots = new HashMap<>();
        for (final SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
            final SnapshotInfo snapshotInfo;
            try {
                snapshotInfo = getSnapshotInfo(snapshotId);
            } catch (SnapshotException e) {
                logger.warn("[{}] repository is on a pre-5.0 format with an index file that contains snapshot [{}] but " +
                            "the corresponding snap-{}.dat file cannot be read. The snapshot will no longer be included in " +
                            "the repository but its data directories will remain.", e, getMetadata().name(),
                            snapshotId, snapshotId.getUUID());
                continue;
            }
            for (final String indexName : snapshotInfo.indices()) {
                final IndexId indexId = new IndexId(indexName, indexName);
                if (indexToSnapshots.containsKey(indexId)) {
                    indexToSnapshots.get(indexId).add(snapshotId);
                } else {
                    indexToSnapshots.put(indexId, Sets.newHashSet(snapshotId));
                }
            }
        }
        try {
            final RepositoryData updatedRepoData = repositoryData.initIndices(indexToSnapshots);
            if (isReadOnly() == false) {
                // write the new index gen file with the indices included
                writeIndexGen(updatedRepoData);
            }
            return updatedRepoData;
        } catch (IOException e) {
            throw new RepositoryException(metadata.name(), "failed to update the repository index blob with indices data on startup", e);
        }
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId) {
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
            logger.warn("cannot read snapshot file [{}]", ex, snapshotId);
        }
        MetaData metaData = null;
        try {
            if (snapshot != null) {
                metaData = readSnapshotMetaData(snapshotId, snapshot.version(), repositoryData.resolveIndices(indices), true);
            } else {
                metaData = readSnapshotMetaData(snapshotId, null, repositoryData.resolveIndices(indices), true);
            }
        } catch (IOException | SnapshotException ex) {
            logger.warn("cannot read metadata for snapshot [{}]", ex, snapshotId);
        }
        try {
            // Delete snapshot from the index file, since it is the maintainer of truth of active snapshots
            final RepositoryData updatedRepositoryData = repositoryData.removeSnapshot(snapshotId);
            writeIndexGen(updatedRepositoryData);

            // delete the snapshot file
            safeSnapshotBlobDelete(snapshot, snapshotId.getUUID());
            // delete the global metadata file
            safeGlobalMetaDataBlobDelete(snapshot, snapshotId.getUUID());

            // Now delete all indices
            for (String index : indices) {
                final IndexId indexId = repositoryData.resolveIndexId(index);
                BlobPath indexPath = basePath().add("indices").add(indexId.getId());
                BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                try {
                    indexMetaDataFormat(snapshot.version()).delete(indexMetaDataBlobContainer, snapshotId.getUUID());
                } catch (IOException ex) {
                    logger.warn("[{}] failed to delete metadata for index [{}]", ex, snapshotId, index);
                }
                if (metaData != null) {
                    IndexMetaData indexMetaData = metaData.index(index);
                    if (indexMetaData != null) {
                        for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                            try {
                                delete(snapshotId, snapshot.version(), indexId, new ShardId(indexMetaData.getIndex(), shardId));
                            } catch (SnapshotException ex) {
                                logger.warn("[{}] failed to delete shard data for shard [{}][{}]", ex, snapshotId, index, shardId);
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
                    logger.debug("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " +
                                 "its index folder due to the directory not being empty.", dnee, metadata.name(), indexId);
                } catch (IOException ioe) {
                    // a different IOException occurred while trying to delete - will just log the issue for now
                    logger.debug("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " +
                                 "its index folder.", ioe, metadata.name(), indexId);
                }
            }
        } catch (IOException ex) {
            throw new RepositoryException(metadata.name(), "failed to update snapshot in repository", ex);
        }
    }

    private void safeSnapshotBlobDelete(final SnapshotInfo snapshotInfo, final String blobId) {
        if (snapshotInfo != null) {
            // we know the version the snapshot was created with
            try {
                snapshotFormat(snapshotInfo.version()).delete(snapshotsBlobContainer, blobId);
            } catch (IOException e) {
                logger.warn("[{}] Unable to delete snapshot file [{}]", e, snapshotInfo.snapshotId(), blobId);
            }
        } else {
            // we don't know the version, first try the current format, then the legacy format
            try {
                snapshotFormat.delete(snapshotsBlobContainer, blobId);
            } catch (IOException e) {
                // now try legacy format
                try {
                    snapshotLegacyFormat.delete(snapshotsBlobContainer, blobId);
                } catch (IOException e2) {
                    // neither snapshot file could be deleted, log the error
                    logger.warn("Unable to delete snapshot file [{}]", e, blobId);
                }
            }
        }
    }

    private void safeGlobalMetaDataBlobDelete(final SnapshotInfo snapshotInfo, final String blobId) {
        if (snapshotInfo != null) {
            // we know the version the snapshot was created with
            try {
                globalMetaDataFormat(snapshotInfo.version()).delete(snapshotsBlobContainer, blobId);
            } catch (IOException e) {
                logger.warn("[{}] Unable to delete global metadata file [{}]", e, snapshotInfo.snapshotId(), blobId);
            }
        } else {
            // we don't know the version, first try the current format, then the legacy format
            try {
                globalMetaDataFormat.delete(snapshotsBlobContainer, blobId);
            } catch (IOException e) {
                // now try legacy format
                try {
                    globalMetaDataLegacyFormat.delete(snapshotsBlobContainer, blobId);
                } catch (IOException e2) {
                    // neither global metadata file could be deleted, log the error
                    logger.warn("Unable to delete global metadata file [{}]", e, blobId);
                }
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
                                         final List<SnapshotShardFailure> shardFailures) {
        try {
            SnapshotInfo blobStoreSnapshot = new SnapshotInfo(snapshotId,
                                                              indices.stream().map(IndexId::getName).collect(Collectors.toList()),
                                                              startTime,
                                                              failure,
                                                              System.currentTimeMillis(),
                                                              totalShards,
                                                              shardFailures);
            snapshotFormat.write(blobStoreSnapshot, snapshotsBlobContainer, snapshotId.getUUID());
            final RepositoryData repositoryData = getRepositoryData();
            List<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
            if (!snapshotIds.contains(snapshotId)) {
                writeIndexGen(repositoryData.addSnapshot(snapshotId, indices));
            }
            return blobStoreSnapshot;
        } catch (IOException ex) {
            throw new RepositoryException(metadata.name(), "failed to update snapshot in repository", ex);
        }
    }

    public List<SnapshotId> getSnapshots() {
        return getRepositoryData().getSnapshotIds();
    }

    @Override
    public MetaData getSnapshotMetaData(SnapshotInfo snapshot, List<IndexId> indices) throws IOException {
        return readSnapshotMetaData(snapshot.snapshotId(), snapshot.version(), indices, false);
    }

    @Override
    public SnapshotInfo getSnapshotInfo(final SnapshotId snapshotId) {
        try {
            return snapshotFormat.read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (FileNotFoundException | NoSuchFileException ex) {
            // File is missing - let's try legacy format instead
            try {
                return snapshotLegacyFormat.read(snapshotsBlobContainer, snapshotId.getName());
            } catch (FileNotFoundException | NoSuchFileException ex1) {
                throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
            } catch (IOException | NotXContentException ex1) {
                throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex1);
            }
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
            if (globalMetaDataFormat.exists(snapshotsBlobContainer, snapshotId.getUUID())) {
                snapshotVersion = Version.CURRENT;
            } else if (globalMetaDataLegacyFormat.exists(snapshotsBlobContainer, snapshotId.getName())) {
                throw new SnapshotException(metadata.name(), snapshotId, "snapshot is too old");
            } else {
                throw new SnapshotMissingException(metadata.name(), snapshotId);
            }
        }
        try {
            metaData = globalMetaDataFormat(snapshotVersion).read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (FileNotFoundException | NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex);
        }
        MetaData.Builder metaDataBuilder = MetaData.builder(metaData);
        for (IndexId index : indices) {
            BlobPath indexPath = basePath().add("indices").add(index.getId());
            BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
            try {
                metaDataBuilder.put(indexMetaDataFormat(snapshotVersion).read(indexMetaDataBlobContainer, snapshotId.getUUID()), false);
            } catch (ElasticsearchParseException | IOException ex) {
                if (ignoreIndexErrors) {
                    logger.warn("[{}] [{}] failed to read metadata for index", ex, snapshotId, index.getName());
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
        if (maxSnapshotBytesPerSec.bytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.mbFrac());
        }
    }

    /**
     * Returns appropriate global metadata format based on the provided version of the snapshot
     */
    private BlobStoreFormat<MetaData> globalMetaDataFormat(Version version) {
        if(legacyMetaData(version)) {
            return globalMetaDataLegacyFormat;
        } else {
            return globalMetaDataFormat;
        }
    }

    /**
     * Returns appropriate snapshot format based on the provided version of the snapshot
     */
    private BlobStoreFormat<SnapshotInfo> snapshotFormat(Version version) {
        if(legacyMetaData(version)) {
            return snapshotLegacyFormat;
        } else {
            return snapshotFormat;
        }
    }

    /**
     * In v2.0.0 we changed the metadata file format
     * @return true if legacy version should be used false otherwise
     */
    public static boolean legacyMetaData(Version version) {
        return version.before(Version.V_2_0_0_beta1);
    }

    /**
     * Returns appropriate index metadata format based on the provided version of the snapshot
     */
    private BlobStoreFormat<IndexMetaData> indexMetaDataFormat(Version version) {
        if(legacyMetaData(version)) {
            return indexMetaDataLegacyFormat;
        } else {
            return indexMetaDataFormat;
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
            final String snapshotsIndexBlobName;
            final boolean legacyFormat;
            if (indexGen == -1) {
                // index-N file doesn't exist, either its a fresh repository, or its in the
                // old format, so look for the older index file before returning an empty list
                snapshotsIndexBlobName = SNAPSHOTS_FILE;
                legacyFormat = true;
            } else {
                snapshotsIndexBlobName = INDEX_FILE_PREFIX + Long.toString(indexGen);
                legacyFormat = false;
            }

            RepositoryData repositoryData;
            try (InputStream blob = snapshotsBlobContainer.readBlob(snapshotsIndexBlobName)) {
                BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(blob, out);
                try (XContentParser parser = XContentHelper.createParser(out.bytes())) {
                    repositoryData = RepositoryData.fromXContent(parser);
                }
            }
            if (legacyFormat) {
                // pre 5.0 repository data needs to be updated to include the indices
                repositoryData = upgradeRepositoryData(repositoryData);
            }
            return repositoryData;
        } catch (NoSuchFileException nsfe) {
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

    protected void writeIndexGen(final RepositoryData repositoryData) throws IOException {
        assert isReadOnly() == false; // can not write to a read only repository
        final BytesReference snapshotsBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                repositoryData.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
            }
            snapshotsBytes = bStream.bytes();
        }
        final long gen = latestIndexBlobId() + 1;
        // write the index file
        writeAtomic(INDEX_FILE_PREFIX + Long.toString(gen), snapshotsBytes);
        // delete the N-2 index file if it exists, keep the previous one around as a backup
        if (isReadOnly() == false && gen - 2 >= 0) {
            final String oldSnapshotIndexFile = INDEX_FILE_PREFIX + Long.toString(gen - 2);
            if (snapshotsBlobContainer.blobExists(oldSnapshotIndexFile)) {
                snapshotsBlobContainer.deleteBlob(oldSnapshotIndexFile);
            }
            // delete the old index file (non-generational) if it exists
            if (snapshotsBlobContainer.blobExists(SNAPSHOTS_FILE)) {
                snapshotsBlobContainer.deleteBlob(SNAPSHOTS_FILE);
            }
        }

        // write the current generation to the index-latest file
        final BytesReference genBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            bStream.writeLong(gen);
            genBytes = bStream.bytes();
        }
        if (snapshotsBlobContainer.blobExists(INDEX_LATEST_BLOB)) {
            snapshotsBlobContainer.deleteBlob(INDEX_LATEST_BLOB);
        }
        writeAtomic(INDEX_LATEST_BLOB, genBytes);
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
            // first, try listing the blobs and determining which index blob is the latest
            return listBlobsToGetLatestIndexId();
        } catch (UnsupportedOperationException e) {
            // could not list the blobs because the repository does not support the operation,
            // try reading from the index-latest file
            try {
                return readSnapshotIndexLatestBlob();
            } catch (IOException ioe) {
                // we likely could not find the blob, this can happen in two scenarios:
                //  (1) its an empty repository
                //  (2) when writing the index-latest blob, if the blob already exists,
                //      we first delete it, then atomically write the new blob.  there is
                //      a small window in time when the blob is deleted and the new one
                //      written - if the node crashes during that time, we won't have an
                //      index-latest blob
                // in a read-only repository, we can't know which of the two scenarios it is,
                // but we will assume (1) because we can't do anything about (2) anyway
                return -1;
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
        long latest = -1;
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
        final String tempBlobName = "pending-" + blobName;
        try (InputStream stream = bytesRef.streamInput()) {
            snapshotsBlobContainer.writeBlob(tempBlobName, stream, bytesRef.length());
        }
        try {
            snapshotsBlobContainer.move(tempBlobName, blobName);
        } catch (IOException ex) {
            // Move failed - try cleaning up
            snapshotsBlobContainer.deleteBlob(tempBlobName);
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
        if (BlobStoreRepository.legacyMetaData(version)) {
            return indexShardSnapshotLegacyFormat;
        } else {
            return indexShardSnapshotFormat;
        }
    }

    /**
     * Context for snapshot/restore operations
     */
    private class Context {

        protected final SnapshotId snapshotId;

        protected final ShardId shardId;

        protected final BlobContainer blobContainer;

        protected final Version version;

        public Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            this(snapshotId, version, indexId, shardId, shardId);
        }

        public Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId, ShardId snapshotShardId) {
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
                throw new IndexShardRestoreFailedException(shardId, "failed to read shard snapshot file", ex);
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
                            logger.debug("[{}] [{}] error deleting blob [{}] during cleanup", e, snapshotId, shardId, blobName);
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
            for (String name : blobs.keySet()) {
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
                    logger.warn("failed to read index file  [{}]", e, SNAPSHOT_INDEX_PREFIX + latest);
                }
            }

            // We couldn't load the index file - falling back to loading individual snapshots
            List<SnapshotFiles> snapshots = new ArrayList<>();
            for (String name : blobs.keySet()) {
                try {
                    BlobStoreIndexShardSnapshot snapshot = null;
                    if (name.startsWith(SNAPSHOT_PREFIX)) {
                        snapshot = indexShardSnapshotFormat.readBlob(blobContainer, snapshotId.getUUID());
                    } else if (name.startsWith(LEGACY_SNAPSHOT_PREFIX)) {
                        snapshot = indexShardSnapshotLegacyFormat.readBlob(blobContainer, name);
                    }
                    if (snapshot != null) {
                        snapshots.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles()));
                    }
                } catch (IOException e) {
                    logger.warn("failed to read commit point [{}]", e, name);
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
        public SnapshotContext(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexShardSnapshotStatus snapshotStatus) {
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
                                logger.warn("{} Can't calculate hash from blob for file [{}] [{}]", e, shardId, fileInfo.physicalName(), fileInfo.metadata());
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
        private final BlobStoreIndexShardSnapshot.FileInfo info;

        public PartSliceStream(BlobContainer container, BlobStoreIndexShardSnapshot.FileInfo info) {
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
        public RestoreContext(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
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
                    recoveryTargetMetadata = targetShard.snapshotStoreMetadata();
                } catch (IndexNotFoundException e) {
                    // happens when restore to an empty shard, not a big deal
                    logger.trace("[{}] [{}] restoring from to an empty shard", shardId, snapshotId);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                } catch (IOException e) {
                    logger.warn("{} Can't read metadata from store, will not reuse any local file while restoring", e, shardId);
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
                        logger.warn("{} Can't calculate hash from blog for file [{}] [{}]", e, shardId, fileInfo.physicalName(), fileInfo.metadata());
                    }
                    snapshotMetaData.put(fileInfo.metadata().name(), fileInfo.metadata());
                    fileInfos.put(fileInfo.metadata().name(), fileInfo);
                }
                final Store.MetadataSnapshot sourceMetaData = new Store.MetadataSnapshot(unmodifiableMap(snapshotMetaData), emptyMap(), 0);
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

                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] recovering_files [{}] with total_size [{}], reusing_files [{}] with reused_size [{}]", shardId, snapshotId,
                        index.totalRecoverFiles(), new ByteSizeValue(index.totalRecoverBytes()), index.reusedFileCount(), new ByteSizeValue(index.reusedFileCount()));
                }
                try {
                    for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
                        logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
                        restoreFile(fileToRecover, store);
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
                try (final IndexOutput indexOutput = store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
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
