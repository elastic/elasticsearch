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

    public static final Setting<String> EPHEMERAL_INDEX_REPOSITORY_SETTING =
        Setting.simpleString("index.ephemeral.repository", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    public static final Setting<String> EPHEMERAL_INDEX_SNAPSHOT_SETTING =
        Setting.simpleString("index.ephemeral.snapshot", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    private final SetOnce<RepositoriesService> repositoriesService;
    private final SetOnce<ThreadPool> threadPool;

    public SearchableSnapshots() {
        this.repositoriesService = new SetOnce<>();
        this.threadPool = new SetOnce<>();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(EPHEMERAL_INDEX_REPOSITORY_SETTING, EPHEMERAL_INDEX_SNAPSHOT_SETTING);
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
        return Map.of("ephemeral", newDirectoryFactory(repositoriesService::get));
    }

    public static DirectoryFactory newDirectoryFactory(final Supplier<RepositoriesService> repositoriesService) {
        return (indexSettings, shardPath) -> {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;

            final Repository repository = repositories.repository(EPHEMERAL_INDEX_REPOSITORY_SETTING.get(indexSettings.getSettings()));
            if (repository instanceof BlobStoreRepository == false) {
                throw new IllegalArgumentException("Repository [" + repository + "] does not support searchable snapshots" );
            }

            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            BlobContainer blobContainer = blobStoreRepository.shardContainer(new IndexId("_ephemeral",
                shardPath.getShardId().getIndex().getUUID()), shardPath.getShardId().id());
            BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer,
                new SnapshotId("_ephemeral", EPHEMERAL_INDEX_SNAPSHOT_SETTING.get(indexSettings.getSettings())));

            return new SearchableSnapshotDirectory(snapshot, blobContainer);
        };
    }
}
