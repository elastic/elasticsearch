/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.util.serializer.JacksonAdapter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_azure";
    public static final String NETTY_EVENT_LOOP_THREAD_POOL_NAME = "azure_event_loop";

    static {
        // Trigger static initialization with the plugin class loader
        // so we have access to the proper xml parser
        JacksonAdapter.createDefaultSerializerAdapter();
    }

    // protected for testing
    final SetOnce<AzureStorageService> azureStoreService = new SetOnce<>();
    private final Settings settings;
    private final Map<String, AzureStorageSettings> initialClientSettings;

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        this.initialClientSettings = AzureStorageSettings.load(settings);
        this.settings = settings;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(AzureRepository.TYPE, metadata -> {
            AzureStorageService storageService = azureStoreService.get();
            assert storageService != null;
            return new AzureRepository(metadata, namedXContentRegistry, storageService, clusterService, bigArrays, recoverySettings);
        });
    }

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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationDeciders allocationDeciders
    ) {
        AzureClientProvider azureClientProvider = AzureClientProvider.create(threadPool, settings);
        azureStoreService.set(createAzureStorageService(settings, azureClientProvider));
        return List.of(azureClientProvider);
    }

    AzureStorageService createAzureStorageService(Settings settingsToUse, AzureClientProvider azureClientProvider) {
        return new AzureStorageService(settingsToUse, azureClientProvider);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.SAS_TOKEN_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.MAX_RETRIES_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settingsToUse) {
        return List.of(executorBuilder(), nettyEventLoopExecutorBuilder(settingsToUse));
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 5, TimeValue.timeValueSeconds(30L), false);
    }

    public static ExecutorBuilder<?> nettyEventLoopExecutorBuilder(Settings settings) {
        int eventLoopThreads = AzureClientProvider.eventLoopThreadsFromSettings(settings);
        return new ScalingExecutorBuilder(NETTY_EVENT_LOOP_THREAD_POOL_NAME, 0, eventLoopThreads, TimeValue.timeValueSeconds(30L), false);
    }

    @Override
    public void reload(Settings settingsToLoad) {
        // secure settings should be readable
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settingsToLoad);
        AzureStorageService storageService = azureStoreService.get();
        assert storageService != null;
        storageService.refreshSettings(clientsSettings);
    }
}
