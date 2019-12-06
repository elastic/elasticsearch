/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreSearchableSnapshotShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.CachedSearchableSnapshotShard;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SearchableSnapshots extends Plugin implements IndexStorePlugin, RepositoryPlugin {

    /** Setting for enabling or disabling searchable snapshots. Defaults to true. */
    private static final Setting<Boolean> SEARCHABLE_SNAPSHOTS_ENABLED =
        Setting.boolSetting("xpack.searchablesnapshots.enabled", true, Setting.Property.NodeScope);

    public static final Setting<String> EPHEMERAL_INDEX_REPOSITORY_SETTING =
        Setting.simpleString("index.ephemeral.repository", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<String> EPHEMERAL_INDEX_SNAPSHOT_SETTING =
        Setting.simpleString("index.ephemeral.snapshot", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<ByteSizeValue> EPHEMERAL_INDEX_BUFFER_SIZE_SETTING =
        Setting.byteSizeSetting("index.ephemeral.buffer_size", new ByteSizeValue(1, ByteSizeUnit.KB), new ByteSizeValue(1, ByteSizeUnit.KB),
            new ByteSizeValue(1, ByteSizeUnit.GB), Setting.Property.IndexScope);

    private final SetOnce<RepositoriesService> repositoriesService;
    private final SetOnce<CacheService> cacheService;
    private final SetOnce<ThreadPool> threadPool;
    private final Settings settings;
    private final boolean enabled;

    public SearchableSnapshots(Settings settings) {
        this.settings = settings;
        this.enabled = SEARCHABLE_SNAPSHOTS_ENABLED.get(settings);
        this.repositoriesService = new SetOnce<>();
        this.cacheService = new SetOnce<>();
        this.threadPool = new SetOnce<>();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SEARCHABLE_SNAPSHOTS_ENABLED,
            CacheService.CACHE_ENABLED_SETTING,
            CacheService.CACHE_SIZE_SETTING,
            CacheService.CACHE_SEGMENT_SIZE_SETTING);
    }

    private boolean isCacheEnabled() {
        return enabled && CacheService.CACHE_ENABLED_SETTING.get(settings);
    }

    @Override
    public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry) {

        this.threadPool.set(threadPool);
        if (enabled == false || isCacheEnabled() == false) {
            return List.of();
        }

        final CacheService cacheService = new CacheService(settings);
        this.cacheService.set(cacheService);
        return List.of(cacheService);
    }

    @Override
    public void onRepositoriesModule(RepositoriesModule repositoriesModule) {  // should we use some SPI mechanism?
        if (enabled) {
            repositoriesService.set(repositoriesModule.getRepositoryService());
        }
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        if (enabled) {
            RepositoriesService repositories = this.repositoriesService.get();
            assert repositories != null : "repositories service should be set";

            ThreadPool threadPool = this.threadPool.get();
            assert threadPool != null : "thread pool service should be set";

            CacheService cacheService = this.cacheService.get();
            assert cacheService != null || isCacheEnabled() == false : "cache service should be set";

            return Map.of("ephemeral", newDirectoryFactory(repositories, threadPool, cacheService));
        }
        return Map.of();
    }

    public static DirectoryFactory newDirectoryFactory(final RepositoriesService repositories,
                                                       final ThreadPool threadPool,
                                                       @Nullable final CacheService cache) {
        return (indexSettings, shardPath) -> {
            if (repositories == null) {
                throw new IllegalArgumentException("An instance of RepositoriesService is required to build a SearchableSnapshotDirectory");
            }
            if (threadPool == null) {
                throw new IllegalArgumentException("An instance of ThreadPool is required to build a SearchableSnapshotDirectory");
            }

            SearchableSnapshotContext context = new SearchableSnapshotContext(
                EPHEMERAL_INDEX_REPOSITORY_SETTING.get(indexSettings.getSettings()),
                EPHEMERAL_INDEX_SNAPSHOT_SETTING.get(indexSettings.getSettings()),
                shardPath.getShardId().getIndex().getUUID(), // assuming it is initialized as the snapshot's index id
                shardPath.getShardId().getId());

            SearchableSnapshotShard searchableSnapshotShard = new BlobStoreSearchableSnapshotShard(context, repositories, threadPool);
            if (cache != null) {
                searchableSnapshotShard = new CachedSearchableSnapshotShard(searchableSnapshotShard, cache);
            }

            int bufferSize = Math.toIntExact(EPHEMERAL_INDEX_BUFFER_SIZE_SETTING.get(indexSettings.getSettings()).getBytes());
            return new SearchableSnapshotDirectory(searchableSnapshotShard, bufferSize);
        };
    }
}
