/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.oldcodecs.Lucene5Plugin.LUCENE5_STORE_TYPE;

public class Lucene5SnapshotRepository extends FilterRepository {
    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity(), Setting.Property
        .NodeScope);

    private static final Logger logger = LogManager.getLogger(Lucene5SnapshotRepository.class);

    private final ThreadPool threadPool;
    private final ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat;
    private final LegacyBlobStoreFormat<SnapshotInfo> snapshotLegacyFormat;
    private final ChecksumBlobStoreFormat<IndexMetadata> indexMetadataFormat;
    private final LegacyBlobStoreFormat<IndexMetadata> indexMetaDataLegacyFormat;
    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat;
    private final LegacyBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotLegacyFormat;



    public Lucene5SnapshotRepository(Repository in, ThreadPool threadPool) {
        super(in);
        this.threadPool = threadPool;
        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, SnapshotInfo::fromXContentInternal);
        snapshotLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT, Lucene5SnapshotRepository::fromLegacyXContent);
        indexMetadataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT,
            (repoName, parser) -> basicIndexMetadataFromXContent(parser));
        indexMetaDataLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT,
            (repoName, parser) -> basicIndexMetadataFromXContent(parser));
        indexShardSnapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
            (repoName, parser) -> BlobStoreIndexShardSnapshot.fromXContent(parser));
        indexShardSnapshotLegacyFormat = new LegacyBlobStoreFormat<>(LEGACY_SNAPSHOT_NAME_FORMAT,
            (repoName, parser) -> BlobStoreIndexShardSnapshot.fromXContent(parser));
    }

    static final String KEY_STATE = "state";
    static final String KEY_VERSION = "version";
    static final String KEY_SETTINGS = "settings";
    static final String KEY_MAPPINGS = "mappings";

    public static IndexMetadata basicIndexMetadataFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        IndexMetadata.Builder builder = new IndexMetadata.Builder(parser.currentName());

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_SETTINGS.equals(currentFieldName)) {
                    builder.settings(Settings.fromXContent(parser));
                } else if (KEY_MAPPINGS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            String mappingType = currentFieldName;
                            Map<String, Object> mappingSource =
                                MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                            builder.putMapping(new MappingMetadata(mappingType, mappingSource));
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else {
                    // assume it's custom index metadata
                    parser.skipChildren();
                    //parser.mapStrings();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_MAPPINGS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                            builder.putMapping(new MappingMetadata(new CompressedXContent(parser.binaryValue())));
                        } else {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                builder.putMapping(new MappingMetadata(mappingType, mapping));
                            }
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                if (KEY_STATE.equals(currentFieldName)) {
                    builder.state(IndexMetadata.State.fromString(parser.text()));
                } else if (KEY_VERSION.equals(currentFieldName)) {
                    builder.version(parser.longValue());
                } else {
                    parser.nextToken();
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);

        return builder.build();
    }



    public static Repository.Factory newRepositoryFactory(ThreadPool threadPool) {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetadata metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetadata metadata, Function<String, Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metadata.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                Settings settings = metadata.settings();
                if (settings.hasValue(BlobStoreRepository.READONLY_SETTING_KEY)) {
                    if (metadata.settings().getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, false) == false) {
                        throw new IllegalArgumentException("repository must be readonly");
                    }
                } else {
                    settings = Settings.builder().put(settings).put(BlobStoreRepository.READONLY_SETTING_KEY, "true").build();
                }
                return new Lucene5SnapshotRepository(factory.create(new RepositoryMetadata(metadata.name(),
                    delegateType, settings), typeLookup), threadPool);
            }
        };
    }

    private static final String SNAPSHOTS_FILE = "index";

    private static final String LEGACY_SNAPSHOT_PREFIX = "snapshot-";

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_SUFFIX = ".dat";

    private static final String COMMON_SNAPSHOT_PREFIX = "snap";

    private static final String SNAPSHOT_CODEC = "snapshot";

    private static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s" + SNAPSHOT_SUFFIX;

    private static final String LEGACY_SNAPSHOT_NAME_FORMAT = LEGACY_SNAPSHOT_PREFIX + "%s";

    private static final String METADATA_NAME_FORMAT = "meta-%s.dat";

    private static final String LEGACY_METADATA_NAME_FORMAT = "metadata-%s";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    public static SnapshotInfo fromLegacyXContent(final String repoName, XContentParser parser) throws IOException {
        String name = null;
        Version version = Version.CURRENT;
        SnapshotState state = SnapshotState.IN_PROGRESS;
        String reason = null;
        List<String> indices = Collections.emptyList();
        long startTime = 0;
        long endTime = 0;
        int totalShard = 0;
        int successfulShards = 0;
        List<SnapshotShardFailure> shardFailures = Collections.emptyList();
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParser.Token token;
        if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
            String currentFieldName = parser.currentName();
            if ("snapshot".equals(currentFieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            if ("name".equals(currentFieldName)) {
                                name = parser.text();
                            } else if ("state".equals(currentFieldName)) {
                                state = SnapshotState.valueOf(parser.text());
                            } else if ("reason".equals(currentFieldName)) {
                                reason = parser.text();
                            } else if ("start_time".equals(currentFieldName)) {
                                startTime = parser.longValue();
                            } else if ("end_time".equals(currentFieldName)) {
                                endTime = parser.longValue();
                            } else if ("total_shards".equals(currentFieldName)) {
                                totalShard = parser.intValue();
                            } else if ("successful_shards".equals(currentFieldName)) {
                                successfulShards = parser.intValue();
                            } else if ("version_id".equals(currentFieldName)) {
                                version = Version.fromId(parser.intValue());
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if ("indices".equals(currentFieldName)) {
                                ArrayList<String> indicesArray = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    indicesArray.add(parser.text());
                                }
                                indices = Collections.unmodifiableList(indicesArray);
                            } else if ("failures".equals(currentFieldName)) {
                                ArrayList<SnapshotShardFailure> shardFailureArrayList = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    shardFailureArrayList.add(SnapshotShardFailure.fromXContent(parser));
                                }
                                shardFailures = Collections.unmodifiableList(shardFailureArrayList);
                            } else {
                                // It was probably created by newer version - ignoring
                                parser.skipChildren();
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            // It was probably created by newer version - ignoring
                            parser.skipChildren();
                        }
                    }
                }
            }
        } else {
            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
        }
        return new SnapshotInfo(new Snapshot(repoName, new SnapshotId(name, name)), indices, Collections.emptyList(),
            Collections.emptyList(), reason, version, startTime, endTime,
            totalShard, successfulShards, shardFailures, false, Collections.emptyMap(), state, Collections.emptyMap());
    }


    protected List<SnapshotId> readSnapshotList() throws IOException {
        final String repositoryName = repositoryName();
        try (InputStream blob = blobContainer().readBlob(SNAPSHOTS_FILE)) {
            final byte[] data = Streams.readFully(blob).array();
            ArrayList<SnapshotId> snapshots = new ArrayList<>();
            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
                new BytesArray(data))) {
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("snapshots".equals(currentFieldName)) {
                            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    snapshots.add(new SnapshotId(repositoryName, parser.text()));
                                }
                            }
                        }
                    }
                }
            }
            return Collections.unmodifiableList(snapshots);
        }
    }

    private String repositoryName() {
        return getMetadata().name();
    }

    public List<SnapshotId> snapshots() {
        final String repositoryName = repositoryName();
        try {
            List<SnapshotId> snapshots = new ArrayList<>();
            Map<String, BlobMetadata> blobs;
            try {
                blobs = blobContainer().listBlobsByPrefix(COMMON_SNAPSHOT_PREFIX);
            } catch (UnsupportedOperationException ex) {
                // Fall back in case listBlobsByPrefix isn't supported by the blob store
                return readSnapshotList();
            }
            int prefixLength = SNAPSHOT_PREFIX.length();
            int suffixLength = SNAPSHOT_SUFFIX.length();
            int legacyPrefixLength = LEGACY_SNAPSHOT_PREFIX.length();
            for (BlobMetadata md : blobs.values()) {
                String blobName = md.name();
                final String name;
                if (blobName.startsWith(SNAPSHOT_PREFIX) && blobName.length() > legacyPrefixLength) {
                    name = blobName.substring(prefixLength, blobName.length() - suffixLength);
                } else if (blobName.startsWith(LEGACY_SNAPSHOT_PREFIX) && blobName.length() > suffixLength + prefixLength) {
                    name = blobName.substring(legacyPrefixLength);
                } else {
                    // not sure what it was - ignore
                    continue;
                }
                snapshots.add(new SnapshotId(name, repositoryName + ":" + name));
            }
            return Collections.unmodifiableList(snapshots);
        } catch (IOException ex) {
            throw new RepositoryException(repositoryName, "failed to list snapshots in repository", ex);
        }
    }

    private BlobContainer blobContainer() {
        return ((BlobStoreRepository) getDelegate()).blobContainer();
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
        executor.execute(ActionRunnable.wrap(listener, this::doGetRepositoryData));
    }

    private void doGetRepositoryData(ActionListener<RepositoryData> listener) {
        ActionListener.completeWith(listener, () -> {
            final List<SnapshotId> snapshotIds = snapshots();
            return new RepositoryData(RepositoryData.MISSING_UUID, 1,
                snapshotIds.stream().collect(Collectors.toMap(s -> s.getUUID(), s -> s)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                ShardGenerations.EMPTY,
                IndexMetaDataGenerations.EMPTY,
                RepositoryData.MISSING_UUID) {

                @Override
                public IndexId resolveIndexId(final String indexName) {
                    return new IndexId(indexName, indexName);
                }
            };
        });
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        // put snapshot info downloads into a task queue instead of pushing them all into the queue to not completely monopolize the
        // snapshot meta pool for a single request
        final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT_META).getMax(), context.snapshotIds().size());
        final BlockingQueue<SnapshotId> queue = new LinkedBlockingQueue<>(context.snapshotIds());
        for (int i = 0; i < workers; i++) {
            getOneSnapshotInfo(queue, context);
        }
    }

    private void getOneSnapshotInfo(BlockingQueue<SnapshotId> queue, GetSnapshotInfoContext context) {
        final SnapshotId snapshotId = queue.poll();
        if (snapshotId == null) {
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT_META).execute(() -> {
            if (context.done()) {
                return;
            }
            if (context.isCancelled()) {
                queue.clear();
                context.onFailure(new TaskCancelledException("task cancelled"));
                return;
            }
            Exception failure = null;
            SnapshotInfo snapshotInfo = null;
            try {
                try {
                    snapshotInfo = snapshotFormat.read(repositoryName(), blobContainer(), snapshotId.getName(),
                        NamedXContentRegistry.EMPTY);
                } catch (NoSuchFileException ex) {
                    try {
                        snapshotInfo = snapshotLegacyFormat.read(repositoryName(), blobContainer(), snapshotId.getName(),
                            NamedXContentRegistry.EMPTY);
                    } catch (NoSuchFileException nsfe) {
                        failure = new SnapshotMissingException(repositoryName(), snapshotId, nsfe);
                    }
                }
            } catch (IOException | NotXContentException ex) {
                failure = new SnapshotException(repositoryName(), snapshotId, "failed to get snapshot info" + snapshotId, ex);
            } catch (Exception e) {
                failure = e instanceof SnapshotException
                    ? e
                    : new SnapshotException(repositoryName(), snapshotId, "Snapshot could not be read", e);
            }
            if (failure != null) {
                if (context.abortOnFailure()) {
                    queue.clear();
                }
                context.onFailure(failure);
            } else {
                assert snapshotInfo != null;
                // Fake version ID so that the snapshot can be restored
                snapshotInfo = new SnapshotInfo(snapshotInfo.snapshot(), snapshotInfo.indices(), snapshotInfo.dataStreams(),
                    snapshotInfo.featureStates(), snapshotInfo.reason(), Version.CURRENT, snapshotInfo.startTime(), snapshotInfo.endTime(),
                    snapshotInfo.totalShards(), snapshotInfo.successfulShards(), snapshotInfo.shardFailures(),
                    snapshotInfo.includeGlobalState(), snapshotInfo.userMetadata(), snapshotInfo.state(),
                    snapshotInfo.indexSnapshotDetails());
                context.onResponse(snapshotInfo);
            }
            getOneSnapshotInfo(queue, context);
        });
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return Metadata.EMPTY_METADATA;
    }

    private BlobPath basePath() {
        return ((BlobStoreRepository) getDelegate()).basePath();
    }

    private BlobStore blobStore() {
        return ((BlobStoreRepository) getDelegate()).blobStore();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        BlobPath indexPath = basePath().add("indices").add(index.getName());
        BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
        IndexMetadata indexMetadata = null;
        try {
            indexMetadata = indexMetadataFormat.read(repositoryName(), indexMetaDataBlobContainer, snapshotId.getName(),
            NamedXContentRegistry.EMPTY);
        } catch (NoSuchFileException ex) {
            indexMetadata = indexMetaDataLegacyFormat.read(repositoryName(), indexMetaDataBlobContainer, snapshotId.getName(),
                NamedXContentRegistry.EMPTY);
        }

        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata).settings(Settings.builder()
                // TODO: reset index settings / only use those that the current ES version understands
                // .put(indexMetadata.getSettings())
                // Fake index created version so that it bypasses checks
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, indexMetadata.getNumberOfShards())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, indexMetadata.getNumberOfReplicas())
                .put(INDEX_STORE_TYPE_SETTING.getKey(), LUCENE5_STORE_TYPE)
            .build())
            // Reset mapping to avoid incompatibilities
            // TODO: perhaps put existing mapping into _meta section so that it is available for introspection
            // alternatively upgrade field definitions to runtime fields
            .putMapping("{\"properties\": {}}");
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            builder.putInSyncAllocationIds(i, Collections.singleton("UUID"));
        }
        return builder.build();
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(shardContainer(indexId, shardId), snapshotId);
        return IndexShardSnapshotStatus.newDone(
            snapshot.startTime(),
            snapshot.time(),
            snapshot.incrementalFileCount(),
            snapshot.totalFileCount(),
            snapshot.incrementalSize(),
            snapshot.totalSize(),
            null
        ); // Not adding a real generation here as it doesn't matter to callers
    }

    private BlobPath indicesPath() {
        return basePath().add("indices");
    }

    private BlobContainer indexContainer(IndexId indexId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()));
    }

    private BlobContainer shardContainer(IndexId indexId, ShardId shardId) {
        return shardContainer(indexId, shardId.getId());
    }

    public BlobContainer shardContainer(IndexId indexId, int shardId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()).add(Integer.toString(shardId)));
    }

    public BlobStoreIndexShardSnapshot loadShardSnapshot(BlobContainer shardContainer, SnapshotId snapshotId) {
        try {
            try {
                return indexShardSnapshotFormat.read(repositoryName(), shardContainer, snapshotId.getUUID(), NamedXContentRegistry.EMPTY);
            } catch (NoSuchFileException ex) {
                try {
                    return indexShardSnapshotLegacyFormat.read(repositoryName(), shardContainer, snapshotId.getUUID(),
                        NamedXContentRegistry.EMPTY);
                } catch (NoSuchFileException ex2) {
                    throw new SnapshotMissingException(repositoryName(), snapshotId, ex);
                }
            }
        } catch (IOException ex) {
            throw new SnapshotException(
                repositoryName(),
                snapshotId,
                "failed to read shard snapshot file for [" + shardContainer.path() + ']',
                ex
            );
        }
    }

    @Override
    public void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    ) {
        ((BlobStoreRepository) getDelegate()).restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener,
            this::loadShardSnapshot);
    }


    public static class LegacyBlobStoreFormat<T extends ToXContent> {

        private final String blobNameFormat;

        private final CheckedBiFunction<String, XContentParser, T, IOException> reader;

        public LegacyBlobStoreFormat(String blobNameFormat, CheckedBiFunction<String, XContentParser, T, IOException> reader) {
            this.blobNameFormat = blobNameFormat;
            this.reader = reader;
        }

        public T read(String repoName, BlobContainer blobContainer, String blobName, NamedXContentRegistry namedXContentRegistry)
            throws IOException {
            try (InputStream inputStream = blobContainer.readBlob(blobName(blobName))) {
                final byte[] data = Streams.readFully(inputStream).array();
                try (
                    XContentParser parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                        new BytesArray(data))
                ) {
                    return reader.apply(repoName, parser);
                }
            }
        }

        protected String blobName(String name) {
            return String.format(Locale.ROOT, blobNameFormat, name);
        }
    }

}
