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

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.snapshots.SnapshotId;
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
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository.RateLimiterListener;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
 *   |- index             - list of all snapshot name as JSON array
 *   |- snapshot-20131010 - JSON serialized Snapshot for snapshot "20131010"
 *   |- meta-20131010.dat - JSON serialized MetaData for snapshot "20131010" (includes only global metadata)
 *   |- snapshot-20131011 - JSON serialized Snapshot for snapshot "20131011"
 *   |- meta-20131011.dat - JSON serialized MetaData for snapshot "20131011"
 *   .....
 *   |- indices/ - data for all indices
 *      |- foo/ - data for index "foo"
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
 *      |- bar/ - data for index bar
 *      ......
 * }
 * </pre>
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent<Repository> implements Repository, RateLimiterListener {

    private BlobContainer snapshotsBlobContainer;

    protected final String repositoryName;

    private static final String LEGACY_SNAPSHOT_PREFIX = "snapshot-";

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_SUFFIX = ".dat";

    private static final String COMMON_SNAPSHOT_PREFIX = "snap";

    private static final String SNAPSHOT_CODEC = "snapshot";

    static final String SNAPSHOTS_FILE = "index"; // package private for unit testing

    private static final String TESTS_FILE = "tests-";

    private static final String METADATA_NAME_FORMAT = "meta-%s.dat";

    private static final String LEGACY_METADATA_NAME_FORMAT = "metadata-%s";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    private static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s" + SNAPSHOT_SUFFIX;

    private static final String LEGACY_SNAPSHOT_NAME_FORMAT = LEGACY_SNAPSHOT_PREFIX + "%s";


    private final BlobStoreIndexShardRepository indexShardRepository;

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

    /**
     * Constructs new BlobStoreRepository
     *
     * @param repositoryName       repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository an instance of IndexShardRepository
     */
    protected BlobStoreRepository(String repositoryName, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) {
        super(repositorySettings.globalSettings());
        this.repositoryName = repositoryName;
        this.indexShardRepository = (BlobStoreIndexShardRepository) indexShardRepository;
        snapshotRateLimiter = getRateLimiter(repositorySettings, "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(repositorySettings, "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        readOnly = repositorySettings.settings().getAsBoolean("readonly", false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() {
        this.snapshotsBlobContainer = blobStore().blobContainer(basePath());
        indexShardRepository.initialize(blobStore(), basePath(), chunkSize(), snapshotRateLimiter, restoreRateLimiter, this, isCompress());

        ParseFieldMatcher parseFieldMatcher = new ParseFieldMatcher(settings);
        globalMetaDataFormat = new ChecksumBlobStoreFormat<>(METADATA_CODEC, METADATA_NAME_FORMAT, MetaData.PROTO, parseFieldMatcher, isCompress());
        globalMetaDataLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_METADATA_NAME_FORMAT, MetaData.PROTO, parseFieldMatcher);

        indexMetaDataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT, IndexMetaData.PROTO, parseFieldMatcher, isCompress());
        indexMetaDataLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, IndexMetaData.PROTO, parseFieldMatcher);

        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, SnapshotInfo.PROTO, parseFieldMatcher, isCompress());
        snapshotLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, SnapshotInfo.PROTO, parseFieldMatcher);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStop() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doClose() {
        try {
            blobStore().close();
        } catch (Throwable t) {
            logger.warn("cannot close blob store", t);
        }
    }

    /**
     * Returns initialized and ready to use BlobStore
     * <p>
     * This method is first called in the {@link #doStart()} method.
     *
     * @return blob store
     */
    abstract protected BlobStore blobStore();

    /**
     * Returns base path of the repository
     */
    abstract protected BlobPath basePath();

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<String> indices, MetaData metaData) {
        if (readOnly()) {
            throw new RepositoryException(this.repositoryName, "cannot create snapshot in a readonly repository");
        }
        try {
            final String snapshotName = snapshotId.getName();
            // check if the snapshot name already exists in the repository
            if (snapshots().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                throw new SnapshotCreationException(repositoryName, snapshotId, "snapshot with the same name already exists");
            }
            if (snapshotFormat.exists(snapshotsBlobContainer, blobId(snapshotId)) ||
                    snapshotLegacyFormat.exists(snapshotsBlobContainer, snapshotName)) {
                throw new SnapshotCreationException(repositoryName, snapshotId, "snapshot with such name already exists");
            }
            // Write Global MetaData
            globalMetaDataFormat.write(metaData, snapshotsBlobContainer, snapshotName);
            for (String index : indices) {
                final IndexMetaData indexMetaData = metaData.index(index);
                final BlobPath indexPath = basePath().add("indices").add(index);
                final BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                indexMetaDataFormat.write(indexMetaData, indexMetaDataBlobContainer, snapshotName);
            }
        } catch (IOException ex) {
            throw new SnapshotCreationException(repositoryName, snapshotId, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSnapshot(SnapshotId snapshotId) {
        if (readOnly()) {
            throw new RepositoryException(this.repositoryName, "cannot delete snapshot from a readonly repository");
        }
        List<String> indices = Collections.emptyList();
        SnapshotInfo snapshot = null;
        try {
            snapshot = readSnapshot(snapshotId);
            indices = snapshot.indices();
        } catch (SnapshotMissingException ex) {
            throw ex;
        } catch (IllegalStateException | SnapshotException | ElasticsearchParseException ex) {
            logger.warn("cannot read snapshot file [{}]", ex, snapshotId);
        }
        MetaData metaData = null;
        try {
            if (snapshot != null) {
                metaData = readSnapshotMetaData(snapshotId, snapshot.version(), indices, true);
            } else {
                metaData = readSnapshotMetaData(snapshotId, null, indices, true);
            }
        } catch (IOException | SnapshotException ex) {
            logger.warn("cannot read metadata for snapshot [{}]", ex, snapshotId);
        }
        try {
            final String snapshotName = snapshotId.getName();
            // Delete snapshot file first so we wouldn't end up with partially deleted snapshot that looks OK
            if (snapshot != null) {
                snapshotFormat(snapshot.version()).delete(snapshotsBlobContainer, blobId(snapshotId));
                globalMetaDataFormat(snapshot.version()).delete(snapshotsBlobContainer, snapshotName);
            } else {
                // We don't know which version was the snapshot created with - try deleting both current and legacy formats
                snapshotFormat.delete(snapshotsBlobContainer, blobId(snapshotId));
                snapshotLegacyFormat.delete(snapshotsBlobContainer, snapshotName);
                globalMetaDataLegacyFormat.delete(snapshotsBlobContainer, snapshotName);
                globalMetaDataFormat.delete(snapshotsBlobContainer, snapshotName);
            }
            // Delete snapshot from the snapshot list
            List<SnapshotId> snapshotIds = snapshots().stream().filter(id -> snapshotId.equals(id) == false).collect(Collectors.toList());
            writeSnapshotList(snapshotIds);
            // Now delete all indices
            for (String index : indices) {
                BlobPath indexPath = basePath().add("indices").add(index);
                BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                try {
                    indexMetaDataFormat(snapshot.version()).delete(indexMetaDataBlobContainer, snapshotId.getName());
                } catch (IOException ex) {
                    logger.warn("[{}] failed to delete metadata for index [{}]", ex, snapshotId, index);
                }
                if (metaData != null) {
                    IndexMetaData indexMetaData = metaData.index(index);
                    if (indexMetaData != null) {
                        for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                            try {
                                indexShardRepository.delete(snapshotId, snapshot.version(), new ShardId(indexMetaData.getIndex(), shardId));
                            } catch (SnapshotException ex) {
                                logger.warn("[{}] failed to delete shard data for shard [{}][{}]", ex, snapshotId, index, shardId);
                            }
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RepositoryException(this.repositoryName, "failed to update snapshot in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SnapshotInfo finalizeSnapshot(final SnapshotId snapshotId,
                                         final List<String> indices,
                                         final long startTime,
                                         final String failure,
                                         final int totalShards,
                                         final List<SnapshotShardFailure> shardFailures) {
        try {
            SnapshotInfo blobStoreSnapshot = new SnapshotInfo(snapshotId,
                                                              indices,
                                                              startTime,
                                                              failure,
                                                              System.currentTimeMillis(),
                                                              totalShards,
                                                              shardFailures);
            snapshotFormat.write(blobStoreSnapshot, snapshotsBlobContainer, blobId(snapshotId));
            List<SnapshotId> snapshotIds = snapshots();
            if (!snapshotIds.contains(snapshotId)) {
                snapshotIds = new ArrayList<>(snapshotIds);
                snapshotIds.add(snapshotId);
                snapshotIds = Collections.unmodifiableList(snapshotIds);
            }
            writeSnapshotList(snapshotIds);
            return blobStoreSnapshot;
        } catch (IOException ex) {
            throw new RepositoryException(this.repositoryName, "failed to update snapshot in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SnapshotId> snapshots() {
        try {
            List<SnapshotId> snapshots = new ArrayList<>();
            Map<String, BlobMetaData> blobs;
            try {
                blobs = snapshotsBlobContainer.listBlobsByPrefix(COMMON_SNAPSHOT_PREFIX);
            } catch (UnsupportedOperationException ex) {
                // Fall back in case listBlobsByPrefix isn't supported by the blob store
                return readSnapshotList();
            }
            int prefixLength = SNAPSHOT_PREFIX.length();
            int suffixLength = SNAPSHOT_SUFFIX.length();
            int legacyPrefixLength = LEGACY_SNAPSHOT_PREFIX.length();
            for (BlobMetaData md : blobs.values()) {
                String blobName = md.name();
                final String name;
                String uuid;
                if (blobName.startsWith(SNAPSHOT_PREFIX) && blobName.length() > legacyPrefixLength) {
                    final String str = blobName.substring(prefixLength, blobName.length() - suffixLength);
                    // TODO: this will go away once we make the snapshot file writes atomic and
                    // use it as the source of truth for the snapshots list instead of listing blobs
                    Tuple<String, String> pair = parseNameUUIDFromBlobName(str);
                    name = pair.v1();
                    uuid = pair.v2();
                } else if (blobName.startsWith(LEGACY_SNAPSHOT_PREFIX) && blobName.length() > suffixLength + prefixLength) {
                    name = blobName.substring(legacyPrefixLength);
                    uuid = SnapshotId.UNASSIGNED_UUID;
                } else {
                    // not sure what it was - ignore
                    continue;
                }
                snapshots.add(new SnapshotId(name, uuid));
            }
            return Collections.unmodifiableList(snapshots);
        } catch (IOException ex) {
            throw new RepositoryException(repositoryName, "failed to list snapshots in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaData readSnapshotMetaData(SnapshotInfo snapshot, List<String> indices) throws IOException {
        return readSnapshotMetaData(snapshot.snapshotId(), snapshot.version(), indices, false);
    }

    @Override
    public SnapshotInfo readSnapshot(final SnapshotId snapshotId) {
        try {
            return snapshotFormat.read(snapshotsBlobContainer, blobId(snapshotId));
        } catch (FileNotFoundException | NoSuchFileException ex) {
            // File is missing - let's try legacy format instead
            try {
                return snapshotLegacyFormat.read(snapshotsBlobContainer, snapshotId.getName());
            } catch (FileNotFoundException | NoSuchFileException ex1) {
                throw new SnapshotMissingException(repositoryName, snapshotId, ex);
            } catch (IOException | NotXContentException ex1) {
                throw new SnapshotException(repositoryName, snapshotId, "failed to get snapshots", ex1);
            }
        } catch (IOException | NotXContentException ex) {
            throw new SnapshotException(repositoryName, snapshotId, "failed to get snapshots", ex);
        }
    }

    private MetaData readSnapshotMetaData(SnapshotId snapshotId, Version snapshotVersion, List<String> indices, boolean ignoreIndexErrors) throws IOException {
        MetaData metaData;
        if (snapshotVersion == null) {
            // When we delete corrupted snapshots we might not know which version we are dealing with
            // We can try detecting the version based on the metadata file format
            assert ignoreIndexErrors;
            if (globalMetaDataFormat.exists(snapshotsBlobContainer, snapshotId.getName())) {
                snapshotVersion = Version.CURRENT;
            } else if (globalMetaDataLegacyFormat.exists(snapshotsBlobContainer, snapshotId.getName())) {
                throw new SnapshotException(repositoryName, snapshotId, "snapshot is too old");
            } else {
                throw new SnapshotMissingException(repositoryName, snapshotId);
            }
        }
        try {
            metaData = globalMetaDataFormat(snapshotVersion).read(snapshotsBlobContainer, snapshotId.getName());
        } catch (FileNotFoundException | NoSuchFileException ex) {
            throw new SnapshotMissingException(repositoryName, snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(repositoryName, snapshotId, "failed to get snapshots", ex);
        }
        MetaData.Builder metaDataBuilder = MetaData.builder(metaData);
        for (String index : indices) {
            BlobPath indexPath = basePath().add("indices").add(index);
            BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
            try {
                metaDataBuilder.put(indexMetaDataFormat(snapshotVersion).read(indexMetaDataBlobContainer, snapshotId.getName()), false);
            } catch (ElasticsearchParseException | IOException ex) {
                if (ignoreIndexErrors) {
                    logger.warn("[{}] [{}] failed to read metadata for index", ex, snapshotId, index);
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
    private RateLimiter getRateLimiter(RepositorySettings repositorySettings, String setting, ByteSizeValue defaultRate) {
        ByteSizeValue maxSnapshotBytesPerSec = repositorySettings.settings().getAsBytesSize(setting,
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

    private static final String SNAPSHOTS = "snapshots";
    private static final String NAME = "name";
    private static final String UUID = "uuid";

    /**
     * Writes snapshot index file
     * <p>
     * This file can be used by read-only repositories that are unable to list files in the repository
     *
     * @param snapshots list of snapshot ids
     * @throws IOException I/O errors
     */
    protected void writeSnapshotList(List<SnapshotId> snapshots) throws IOException {
        final BytesReference bRef;
        try(BytesStreamOutput bStream = new BytesStreamOutput()) {
            try(StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                builder.startObject();
                builder.startArray(SNAPSHOTS);
                for (SnapshotId snapshot : snapshots) {
                    builder.startObject();
                    builder.field(NAME, snapshot.getName());
                    builder.field(UUID, snapshot.getUUID());
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                builder.close();
            }
            bRef = bStream.bytes();
        }
        if (snapshotsBlobContainer.blobExists(SNAPSHOTS_FILE)) {
            snapshotsBlobContainer.deleteBlob(SNAPSHOTS_FILE);
        }
        snapshotsBlobContainer.writeBlob(SNAPSHOTS_FILE, bRef);
    }

    /**
     * Reads snapshot index file
     * <p>
     * This file can be used by read-only repositories that are unable to list files in the repository
     *
     * @return list of snapshots in the repository
     * @throws IOException I/O errors
     */
    protected List<SnapshotId> readSnapshotList() throws IOException {
        try (InputStream blob = snapshotsBlobContainer.readBlob(SNAPSHOTS_FILE)) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            ArrayList<SnapshotId> snapshots = new ArrayList<>();
            try (XContentParser parser = XContentHelper.createParser(out.bytes())) {
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if (SNAPSHOTS.equals(currentFieldName)) {
                            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    // the new format from 5.0 which contains the snapshot name and uuid
                                    String name = null;
                                    String uuid = null;
                                    if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                            currentFieldName = parser.currentName();
                                            parser.nextToken();
                                            if (NAME.equals(currentFieldName)) {
                                                name = parser.text();
                                            } else if (UUID.equals(currentFieldName)) {
                                                uuid = parser.text();
                                            }
                                        }
                                        snapshots.add(new SnapshotId(name, uuid));
                                    }
                                    // the old format pre 5.0 that only contains the snapshot name, use the name as the uuid too
                                    else {
                                        name = parser.text();
                                        snapshots.add(new SnapshotId(name, SnapshotId.UNASSIGNED_UUID));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Collections.unmodifiableList(snapshots);
        }
    }

    @Override
    public void onRestorePause(long nanos) {
        restoreRateLimitingTimeInNanos.inc(nanos);
    }

    @Override
    public void onSnapshotPause(long nanos) {
        snapshotRateLimitingTimeInNanos.inc(nanos);
    }

    @Override
    public long snapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long restoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    @Override
    public String startVerification() {
        try {
            if (readOnly()) {
                // It's readonly - so there is not much we can do here to verify it
                return null;
            } else {
                String seed = UUIDs.randomBase64UUID();
                byte[] testBytes = Strings.toUTF8Bytes(seed);
                BlobContainer testContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
                String blobName = "master.dat";
                testContainer.writeBlob(blobName + "-temp", new BytesArray(testBytes));
                // Make sure that move is supported
                testContainer.move(blobName + "-temp", blobName);
                return seed;
            }
        } catch (IOException exp) {
            throw new RepositoryVerificationException(repositoryName, "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        if (readOnly()) {
            throw new UnsupportedOperationException("shouldn't be called");
        }
        try {
            blobStore().delete(basePath().add(testBlobPrefix(seed)));
        } catch (IOException exp) {
            throw new RepositoryVerificationException(repositoryName, "cannot delete test data at " + basePath(), exp);
        }
    }

    public static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }

    @Override
    public boolean readOnly() {
        return readOnly;
    }

    // package private, only use for testing
    BlobContainer blobContainer() {
        return snapshotsBlobContainer;
    }

    // TODO: this will go away once readSnapshotsList uses the index file instead of listing blobs
    // to know which snapshots are part of a repository.  See #18156
    // Package private for testing.
    static Tuple<String, String> parseNameUUIDFromBlobName(final String str) {
        final String name;
        final String uuid;
        final int sizeOfUUID = 22; // uuid is 22 chars in length
        // unreliable, but highly unlikely to have a snapshot name with a dash followed by 22 characters,
        // and this will go away before a release (see #18156).
        //norelease
        if (str.length() > sizeOfUUID + 1 && str.charAt(str.length() - sizeOfUUID - 1) == '-') {
            // new naming convention, snapshot blob id has name and uuid
            final int idx = str.length() - sizeOfUUID - 1;
            name = str.substring(0, idx);
            uuid = str.substring(idx + 1);
        } else {
            // old naming convention, before snapshots had UUIDs
            name = str;
            uuid = SnapshotId.UNASSIGNED_UUID;
        }
        return Tuple.tuple(name, uuid);
    }

    // Package private for testing
    static String blobId(final SnapshotId snapshotId) {
        final String uuid = snapshotId.getUUID();
        if (uuid.equals(SnapshotId.UNASSIGNED_UUID)) {
            // the old snapshot blob naming
            return snapshotId.getName();
        }
        return snapshotId.getName() + "-" + uuid;
    }
}
