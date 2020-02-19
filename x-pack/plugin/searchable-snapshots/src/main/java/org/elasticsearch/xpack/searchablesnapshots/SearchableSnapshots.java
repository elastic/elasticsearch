/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportSearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsStatsAction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, RepositoryPlugin, EnginePlugin, ActionPlugin, ClusterPlugin {

    private final SetOnce<RepositoriesService> repositoriesService;
    private final SetOnce<CacheService> cacheService;
    private final Settings settings;

    public SearchableSnapshots(final Settings settings) {
        this.repositoriesService = new SetOnce<>();
        this.cacheService = new SetOnce<>();
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_ID_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_INDEX_ID_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_CACHE_ENABLED_SETTING,
            CacheService.SNAPSHOT_CACHE_SIZE_SETTING,
            CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING
        );
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

        final CacheService cacheService = new CacheService(settings);
        this.cacheService.set(cacheService);
        return List.of(cacheService);
    }

    @Override
    public void onRepositoriesModule(RepositoriesModule repositoriesModule) {
        repositoriesService.set(repositoriesModule.getRepositoryService()); // should we use some SPI mechanism?
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY,
            SearchableSnapshotRepository.newDirectoryFactory(repositoriesService::get, cacheService::get));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings.getSettings()))) {
            return Optional.of(engineConfig -> new ReadOnlyEngine(engineConfig, null, new TranslogStats(), false, Function.identity()));
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService) {
        return Collections.singletonMap(SearchableSnapshotRepository.TYPE, SearchableSnapshotRepository.getRepositoryFactory());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(SearchableSnapshotsStatsAction.INSTANCE, TransportSearchableSnapshotsStatsAction.class));
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return List.of(new RestSearchableSnapshotsStatsAction());
    }

    @Override
    public Collection<ExistingShardsAllocator> getExistingShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new SearchableSnapshotAllocator());
    }
}

