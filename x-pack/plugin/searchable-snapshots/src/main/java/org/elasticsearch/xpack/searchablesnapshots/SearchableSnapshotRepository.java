/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

/**
 * A repository that wraps a {@link BlobStoreRepository} to add settings to the index metadata during a restore to identify the source
 * snapshot and index in order to create a {@link SearchableSnapshotDirectory} (and corresponding empty translog) to search these shards
 * without needing to fully restore them.
 */
public class SearchableSnapshotRepository extends FilterRepository {

    public static final String TYPE = "searchable";

    public static final Setting<String> SNAPSHOT_REPOSITORY_SETTING =
        Setting.simpleString("index.store.snapshot.repository_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_SNAPSHOT_NAME_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_SNAPSHOT_ID_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_INDEX_ID_SETTING =
        Setting.simpleString("index.store.snapshot.index_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<Boolean> SNAPSHOT_CACHE_ENABLED_SETTING =
        Setting.boolSetting("index.store.snapshot.cache.enabled", true, Setting.Property.IndexScope);

    public static final String SNAPSHOT_DIRECTORY_FACTORY_KEY = "snapshot";

    private static final Setting<String> DELEGATE_TYPE
        = new Setting<>("delegate_type", "", Function.identity(), Setting.Property.NodeScope);

    private final BlobStoreRepository blobStoreRepository;

    public SearchableSnapshotRepository(Repository in) {
        super(in);
        if (in instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + in + "] does not support searchable snapshots");
        }
        blobStoreRepository = (BlobStoreRepository) in;
    }

    private Directory makeDirectory(IndexSettings indexSettings, ShardPath shardPath, CacheService cacheService) throws IOException {

        IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
        BlobContainer blobContainer = blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id());

        SnapshotId snapshotId = new SnapshotId(SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()));
        BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);

        Directory directory = new SearchableSnapshotDirectory(snapshot, blobContainer);
        if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings.getSettings())) {
            final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
            directory = new CacheDirectory(directory, cacheService, cacheDir, snapshotId, indexId, shardPath.getShardId());
        }
        directory = new InMemoryNoOpCommitDirectory(directory);

        try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
            final Map<String, String> userData = new HashMap<>();
            indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

            final String translogUUID = Translog.createEmptyTranslog(shardPath.resolveTranslog(),
                Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                shardPath.getShardId(), 0L);

            userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            indexWriter.setLiveCommitData(userData.entrySet());
            indexWriter.commit();
        }

        return directory;
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        final IndexMetaData indexMetaData = super.getSnapshotIndexMetaData(snapshotId, index);
        final IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
        builder.settings(Settings.builder().put(indexMetaData.getSettings()).put(getIndexSettings(blobStoreRepository, snapshotId, index)));
        return builder.build();
    }

    private static Settings getIndexSettings(Repository repository, SnapshotId snapshotId, IndexId indexId) {
        return Settings.builder()
            .put(SNAPSHOT_REPOSITORY_SETTING.getKey(), repository.getMetadata().name())
            .put(SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey(), snapshotId.getName())
            .put(SNAPSHOT_SNAPSHOT_ID_SETTING.getKey(), snapshotId.getUUID())
            .put(SNAPSHOT_INDEX_ID_SETTING.getKey(), indexId.getId())
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SNAPSHOT_DIRECTORY_FACTORY_KEY)
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), SearchableSnapshotAllocator.ALLOCATOR_NAME)
            .build();
    }

    static Factory getRepositoryFactory() {
        return new Repository.Factory() {
            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                return new SearchableSnapshotRepository(factory.create(new RepositoryMetaData(metaData.name(),
                    delegateType, metaData.settings()), typeLookup));
            }
        };
    }

    public static IndexStorePlugin.DirectoryFactory newDirectoryFactory(final Supplier<RepositoriesService> repositoriesService,
                                                                        final Supplier<CacheService> cacheService) {
        return (indexSettings, shardPath) -> {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;

            final Repository repository = repositories.repository(SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
            if (repository instanceof SearchableSnapshotRepository == false) {
                throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
            }

            final CacheService cache = cacheService.get();
            assert cache != null;

            return ((SearchableSnapshotRepository) repository).makeDirectory(indexSettings, shardPath, cache);
        };
    }
}
