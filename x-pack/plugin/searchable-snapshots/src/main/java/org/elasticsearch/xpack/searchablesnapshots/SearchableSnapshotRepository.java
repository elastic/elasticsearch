/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.IndexStorePlugin;
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
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A repository that wraps a {@link BlobStoreRepository} to add settings to the index metadata during a restore to identify the source
 * snapshot and index in order to create a {@link SearchableSnapshotDirectory} (and corresponding empty translog) to search these shards
 * without needing to fully restore them.
 */
public class SearchableSnapshotRepository {

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

    private final BlobStoreRepository blobStoreRepository;

    public SearchableSnapshotRepository(Repository in) {
        // TODO: this is probably terrible, revisit this unwrapping
        if (in instanceof SearchableSnapshotRepository) {
            blobStoreRepository = ((SearchableSnapshotRepository) in).blobStoreRepository;
        } else if (in instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + in + "] does not support searchable snapshots");
        } else {
            blobStoreRepository = (BlobStoreRepository) in;
        }
    }

    private Directory makeDirectory(IndexSettings indexSettings, ShardPath shardPath, CacheService cacheService,
                                    LongSupplier currentTimeNanosSupplier) throws IOException {

        IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
        BlobContainer blobContainer = blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id());

        SnapshotId snapshotId = new SnapshotId(SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()));
        BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);

        Directory directory = new SearchableSnapshotDirectory(snapshot, blobContainer);
        if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings.getSettings())) {
            final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
            directory = new CacheDirectory(directory, cacheService, cacheDir, snapshotId, indexId, shardPath.getShardId(),
                currentTimeNanosSupplier);
        }
        directory = new InMemoryNoOpCommitDirectory(directory);

        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE);

        try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
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

    static IndexStorePlugin.DirectoryFactory newDirectoryFactory(final Supplier<RepositoriesService> repositoriesService,
                                                                 final Supplier<CacheService> cacheService,
                                                                 final LongSupplier currentTimeNanosSupplier) {
        return (indexSettings, shardPath) -> {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;
            final CacheService cache = cacheService.get();
            assert cache != null;

            final Repository repository = repositories.repository(SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
            // TODO: is this okay?
//            if (repository instanceof SearchableSnapshotRepository == false) {
//                throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
//            }

            SearchableSnapshotRepository searchableRepo = new SearchableSnapshotRepository(repository);
            return searchableRepo.makeDirectory(indexSettings, shardPath, cache, currentTimeNanosSupplier);
        };
    }
}
