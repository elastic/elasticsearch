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
import org.elasticsearch.common.Numbers;
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
 *   |- index-N           - list of all snapshot name as JSON array, N is the generation of the file
 *   |- index-latest      - contains the numeric value of the latest generation of the index file (i.e. N from above)
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
public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository, RateLimiterListener {

    private BlobContainer snapshotsBlobContainer;

    protected final String repositoryName;

    private static final String LEGACY_SNAPSHOT_PREFIX = "snapshot-";

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_SUFFIX = ".dat";

    private static final String SNAPSHOT_CODEC = "snapshot";

    static final String SNAPSHOTS_FILE = "index"; // package private for unit testing

    private static final String SNAPSHOTS_FILE_PREFIX = "index-";

    private static final String SNAPSHOTS_INDEX_LATEST_BLOB = "index.latest";

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
    protected abstract  BlobStore blobStore();

    /**
     * Returns base path of the repository
     */
    protected abstract  BlobPath basePath();

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
            writeSnapshotsToIndexGen(snapshotIds);

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
                writeSnapshotsToIndexGen(snapshotIds);
            }
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
            return Collections.unmodifiableList(readSnapshotsFromIndex());
        } catch (NoSuchFileException | FileNotFoundException e) {
            // its a fresh repository, no index file exists, so return an empty list
            return Collections.emptyList();
        } catch (IOException ioe) {
            throw new RepositoryException(repositoryName, "failed to list snapshots in repository", ioe);
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

    protected void writeSnapshotsToIndexGen(final List<SnapshotId> snapshots) throws IOException {
        assert readOnly() == false; // can not write to a read only repository
        final BytesReference snapshotsBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
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
            snapshotsBytes = bStream.bytes();
        }
        final long gen = latestIndexBlobId() + 1;
        // write the index file
        writeAtomic(SNAPSHOTS_FILE_PREFIX + Long.toString(gen), snapshotsBytes);
        // delete the N-2 index file if it exists, keep the previous one around as a backup
        if (readOnly() == false && gen - 2 >= 0) {
            final String oldSnapshotIndexFile = SNAPSHOTS_FILE_PREFIX + Long.toString(gen - 2);
            if (snapshotsBlobContainer.blobExists(oldSnapshotIndexFile)) {
                snapshotsBlobContainer.deleteBlob(oldSnapshotIndexFile);
            }
        }

        // write the current generation to the index-latest file
        final BytesReference genBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            bStream.writeLong(gen);
            genBytes = bStream.bytes();
        }
        if (snapshotsBlobContainer.blobExists(SNAPSHOTS_INDEX_LATEST_BLOB)) {
            snapshotsBlobContainer.deleteBlob(SNAPSHOTS_INDEX_LATEST_BLOB);
        }
        writeAtomic(SNAPSHOTS_INDEX_LATEST_BLOB, genBytes);
    }

    protected List<SnapshotId> readSnapshotsFromIndex() throws IOException {
        final long indexGen = latestIndexBlobId();
        final String snapshotsIndexBlobName;
        if (indexGen == -1) {
            // index-N file doesn't exist, either its a fresh repository, or its in the
            // old format, so look for the older index file before returning an empty list
            snapshotsIndexBlobName = SNAPSHOTS_FILE;
        } else {
            snapshotsIndexBlobName = SNAPSHOTS_FILE_PREFIX + Long.toString(indexGen);
        }

        try (InputStream blob = snapshotsBlobContainer.readBlob(snapshotsIndexBlobName)) {
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

    // Package private for testing
    static String blobId(final SnapshotId snapshotId) {
        final String uuid = snapshotId.getUUID();
        if (uuid.equals(SnapshotId.UNASSIGNED_UUID)) {
            // the old snapshot blob naming
            return snapshotId.getName();
        }
        return snapshotId.getName() + "-" + uuid;
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
        try (InputStream blob = snapshotsBlobContainer.readBlob(SNAPSHOTS_INDEX_LATEST_BLOB)) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            return Numbers.bytesToLong(out.bytes().toBytesRef());
        }
    }

    private long listBlobsToGetLatestIndexId() throws IOException {
        Map<String, BlobMetaData> blobs = snapshotsBlobContainer.listBlobsByPrefix(SNAPSHOTS_FILE_PREFIX);
        long latest = -1;
        if (blobs.isEmpty()) {
            // no snapshot index blobs have been written yet
            return latest;
        }
        for (final BlobMetaData blobMetaData : blobs.values()) {
            final String blobName = blobMetaData.name();
            try {
                final long curr = Long.parseLong(blobName.substring(SNAPSHOTS_FILE_PREFIX.length()));
                latest = Math.max(latest, curr);
            } catch (NumberFormatException nfe) {
                // the index- blob wasn't of the format index-N where N is a number,
                // no idea what this blob is but it doesn't belong in the repository!
                logger.debug("[{}] Unknown blob in the repository: {}", repositoryName, blobName);
            }
        }
        return latest;
    }

    private void writeAtomic(final String blobName, final BytesReference bytesRef) throws IOException {
        final String tempBlobName = "pending-" + blobName;
        snapshotsBlobContainer.writeBlob(tempBlobName, bytesRef);
        try {
            snapshotsBlobContainer.move(tempBlobName, blobName);
        } catch (IOException ex) {
            // Move failed - try cleaning up
            snapshotsBlobContainer.deleteBlob(tempBlobName);
            throw ex;
        }
    }

}
