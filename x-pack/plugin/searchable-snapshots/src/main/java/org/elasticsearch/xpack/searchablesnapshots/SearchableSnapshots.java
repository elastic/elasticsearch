/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, RepositoryPlugin {

    public static final Setting<String> SNAPSHOT_REPOSITORY_SETTING =
        Setting.simpleString("index.store.snapshot.repository_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<String> SNAPSHOT_SNAPSHOT_NAME_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<String> SNAPSHOT_SNAPSHOT_ID_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<String> SNAPSHOT_INDEX_ID_SETTING =
        Setting.simpleString("index.store.snapshot.index_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    private final SetOnce<RepositoriesService> repositoriesService;
    private final SetOnce<ThreadPool> threadPool;

    public SearchableSnapshots() {
        this.repositoriesService = new SetOnce<>();
        this.threadPool = new SetOnce<>();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SNAPSHOT_REPOSITORY_SETTING,
            SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SNAPSHOT_SNAPSHOT_ID_SETTING,
            SNAPSHOT_INDEX_ID_SETTING);
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
        return List.of();
    }

    @Override
    public void onRepositoriesModule(RepositoriesModule repositoriesModule) {
        repositoriesService.set(repositoriesModule.getRepositoryService()); // should we use some SPI mechanism?
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of("snapshot", newDirectoryFactory(repositoriesService::get));
    }

    public static DirectoryFactory newDirectoryFactory(final Supplier<RepositoriesService> repositoriesService) {
        return (indexSettings, shardPath) -> {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;

            final Repository repository = repositories.repository(SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
            if (repository instanceof BlobStoreRepository == false) {
                throw new IllegalArgumentException("Repository [" + repository + "] does not support searchable snapshots" );
            }

            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
            BlobContainer blobContainer = blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id());

            SnapshotId snapshotId = new SnapshotId(SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()));
            BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);

            return new SearchableSnapshotDirectory(snapshot, blobContainer);
        };
    }
}
