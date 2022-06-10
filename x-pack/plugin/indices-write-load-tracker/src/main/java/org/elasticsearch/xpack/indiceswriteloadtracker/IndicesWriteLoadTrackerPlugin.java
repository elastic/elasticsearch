/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class IndicesWriteLoadTrackerPlugin extends Plugin implements SystemIndexPlugin {
    public static final String WRITE_LOAD_COLLECTOR_THREAD_POOL_NAME = "write_load_collector";
    public static final String INDICES_WRITE_LOAD_TRACKING_THREAD_POOL_PREFIX = "xpack.indices_write_load_tracking_thread_pool";

    private final SetOnce<IndicesWriteLoadStatsCollector> indicesWriteLoadsStatsCollectorRef = new SetOnce<>();
    private final SetOnce<IndicesWriteLoadStatsService> indicesWriteLoadStatsServiceRef = new SetOnce<>();

    public IndicesWriteLoadTrackerPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final var registry = new IndicesWriteLoadTemplateRegistry(
            environment.settings(),
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        registry.initialize();
        final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, System::nanoTime);
        indicesWriteLoadsStatsCollectorRef.set(indicesWriteLoadStatsCollector);
        final var indicesWriteLoadStatsService = IndicesWriteLoadStatsService.create(
            indicesWriteLoadStatsCollector,
            clusterService,
            threadPool,
            client,
            clusterService.getSettings()
        );
        indicesWriteLoadStatsServiceRef.set(indicesWriteLoadStatsService);

        return Collections.singletonList(indicesWriteLoadStatsService);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            IndicesWriteLoadStatsService.ENABLED_SETTING,
            IndicesWriteLoadStatsService.SAMPLING_FREQUENCY_SETTING,
            IndicesWriteLoadStatsService.STORE_FREQUENCY_SETTING,
            IndicesWriteLoadStore.ENABLED_SETTING,
            IndicesWriteLoadStore.MAX_RETRIES_SETTING,
            IndicesWriteLoadStore.MAX_BULK_SIZE_SETTING,
            IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING,
            IndicesWriteLoadStore.MAX_CONCURRENT_REQUESTS_SETTING
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        assert indicesWriteLoadsStatsCollectorRef.get() != null;
        indexModule.addIndexEventListener(indicesWriteLoadsStatsCollectorRef.get());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(
            new FixedExecutorBuilder(
                settings,
                WRITE_LOAD_COLLECTOR_THREAD_POOL_NAME,
                1,
                100,
                INDICES_WRITE_LOAD_TRACKING_THREAD_POOL_PREFIX,
                false
            )
        );
    }

    @Override
    public void close() {
        indicesWriteLoadStatsServiceRef.get().close();
    }

    @Override
    public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
        return List.of(IndicesWriteLoadStore.INDICES_WRITE_LOAD_DATA_STREAM_DESCRIPTOR);
    }

    @Override
    public String getFeatureName() {
        return IndicesWriteLoadStore.INDICES_WRITE_LOAD_FEATURE_NAME;
    }

    @Override
    public String getFeatureDescription() {
        return "Stores indices write load information";
    }

    static boolean currentThreadIsWriterLoadCollectorThreadOrTestThread() {
        return Thread.currentThread().getName().contains('[' + IndicesWriteLoadTrackerPlugin.WRITE_LOAD_COLLECTOR_THREAD_POOL_NAME + ']')
            || Thread.currentThread().getName().startsWith("TEST-");
    }
}
