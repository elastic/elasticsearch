/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndicesServiceBuilder {
    Settings settings;
    PluginsService pluginsService;
    NodeEnvironment nodeEnv;
    NamedXContentRegistry xContentRegistry;
    AnalysisRegistry analysisRegistry;
    IndexNameExpressionResolver indexNameExpressionResolver;
    MapperRegistry mapperRegistry;
    NamedWriteableRegistry namedWriteableRegistry;
    ThreadPool threadPool;
    IndexScopedSettings indexScopedSettings;
    CircuitBreakerService circuitBreakerService;
    BigArrays bigArrays;
    ScriptService scriptService;
    ClusterService clusterService;
    Client client;
    FeatureService featureService;
    MetaStateService metaStateService;
    Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders = List.of();
    Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories = Map.of();
    @Nullable
    ValuesSourceRegistry valuesSourceRegistry;
    Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = Map.of();
    List<IndexStorePlugin.IndexFoldersDeletionListener> indexFoldersDeletionListeners = List.of();
    Map<String, IndexStorePlugin.SnapshotCommitSupplier> snapshotCommitSuppliers = Map.of();
    @Nullable
    CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator;

    public IndicesServiceBuilder settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public IndicesServiceBuilder pluginsService(PluginsService pluginsService) {
        this.pluginsService = pluginsService;
        return this;
    }

    public IndicesServiceBuilder nodeEnvironment(NodeEnvironment nodeEnv) {
        this.nodeEnv = nodeEnv;
        return this;
    }

    public IndicesServiceBuilder xContentRegistry(NamedXContentRegistry xContentRegistry) {
        this.xContentRegistry = xContentRegistry;
        return this;
    }

    public IndicesServiceBuilder analysisRegistry(AnalysisRegistry analysisRegistry) {
        this.analysisRegistry = analysisRegistry;
        return this;
    }

    public IndicesServiceBuilder indexNameExpressionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        return this;
    }

    public IndicesServiceBuilder mapperRegistry(MapperRegistry mapperRegistry) {
        this.mapperRegistry = mapperRegistry;
        return this;
    }

    public IndicesServiceBuilder namedWriteableRegistry(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
        return this;
    }

    public IndicesServiceBuilder threadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    public IndicesServiceBuilder indexScopedSettings(IndexScopedSettings indexScopedSettings) {
        this.indexScopedSettings = indexScopedSettings;
        return this;
    }

    public IndicesServiceBuilder circuitBreakerService(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
        return this;
    }

    public IndicesServiceBuilder bigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        return this;
    }

    public IndicesServiceBuilder scriptService(ScriptService scriptService) {
        this.scriptService = scriptService;
        return this;
    }

    public IndicesServiceBuilder clusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        return this;
    }

    public IndicesServiceBuilder client(Client client) {
        this.client = client;
        return this;
    }

    public IndicesServiceBuilder featureService(FeatureService featureService) {
        this.featureService = featureService;
        return this;
    }

    public IndicesServiceBuilder metaStateService(MetaStateService metaStateService) {
        this.metaStateService = metaStateService;
        return this;
    }

    public IndicesServiceBuilder valuesSourceRegistry(ValuesSourceRegistry valuesSourceRegistry) {
        this.valuesSourceRegistry = valuesSourceRegistry;
        return this;
    }

    public IndicesServiceBuilder requestCacheKeyDifferentiator(
        CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator
    ) {
        this.requestCacheKeyDifferentiator = requestCacheKeyDifferentiator;
        return this;
    }

    public IndicesService build() {
        Objects.requireNonNull(settings);
        Objects.requireNonNull(pluginsService);
        Objects.requireNonNull(nodeEnv);
        Objects.requireNonNull(xContentRegistry);
        Objects.requireNonNull(analysisRegistry);
        Objects.requireNonNull(indexNameExpressionResolver);
        Objects.requireNonNull(mapperRegistry);
        Objects.requireNonNull(namedWriteableRegistry);
        Objects.requireNonNull(threadPool);
        Objects.requireNonNull(indexScopedSettings);
        Objects.requireNonNull(circuitBreakerService);
        Objects.requireNonNull(bigArrays);
        Objects.requireNonNull(scriptService);
        Objects.requireNonNull(clusterService);
        Objects.requireNonNull(client);
        Objects.requireNonNull(featureService);
        Objects.requireNonNull(metaStateService);
        Objects.requireNonNull(engineFactoryProviders);
        Objects.requireNonNull(directoryFactories);
        Objects.requireNonNull(recoveryStateFactories);
        Objects.requireNonNull(indexFoldersDeletionListeners);
        Objects.requireNonNull(snapshotCommitSuppliers);

        // collect engine factory providers from plugins
        engineFactoryProviders = pluginsService.filterPlugins(EnginePlugin.class)
            .<Function<IndexSettings, Optional<EngineFactory>>>map(plugin -> plugin::getEngineFactory)
            .toList();

        directoryFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
            .map(IndexStorePlugin::getDirectoryFactories)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        recoveryStateFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
            .map(IndexStorePlugin::getRecoveryStateFactories)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        indexFoldersDeletionListeners = pluginsService.filterPlugins(IndexStorePlugin.class)
            .map(IndexStorePlugin::getIndexFoldersDeletionListeners)
            .flatMap(List::stream)
            .toList();

        snapshotCommitSuppliers = pluginsService.filterPlugins(IndexStorePlugin.class)
            .map(IndexStorePlugin::getSnapshotCommitSuppliers)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new IndicesService(this);
    }
}
