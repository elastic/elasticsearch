/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_azure";
    public static final String NETTY_EVENT_LOOP_THREAD_POOL_NAME = "azure_event_loop";

    // protected for testing
    final SetOnce<AzureStorageService> azureStoreService = new SetOnce<>();
    private final Settings settings;

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        AzureStorageSettings.load(settings);
        this.settings = settings;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics
    ) {
        return Collections.singletonMap(AzureRepository.TYPE, (projectId, metadata) -> {
            AzureStorageService storageService = azureStoreService.get();
            assert storageService != null;
            return new AzureRepository(
                metadata,
                namedXContentRegistry,
                storageService,
                clusterService,
                bigArrays,
                recoverySettings,
                repositoriesMetrics
            );
        });
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        AzureClientProvider azureClientProvider = AzureClientProvider.create(services.threadPool(), settings);
        azureStoreService.set(createAzureStorageService(settings, azureClientProvider));
        assert assertRepositoryAzureMaxThreads(settings, services.threadPool());
        return List.of(azureClientProvider);
    }

    AzureStorageService createAzureStorageService(Settings settingsToUse, AzureClientProvider azureClientProvider) {
        return new AzureStorageService(settingsToUse, azureClientProvider);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureClientProvider.EVENT_LOOP_THREAD_COUNT,
            AzureClientProvider.MAX_OPEN_CONNECTIONS,
            AzureClientProvider.OPEN_CONNECTION_TIMEOUT,
            AzureClientProvider.MAX_IDLE_TIME,
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.SAS_TOKEN_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.MAX_RETRIES_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING,
            AzureStorageSettings.ENDPOINT_SETTING,
            AzureStorageSettings.SECONDARY_ENDPOINT_SETTING
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(executorBuilder(settings), nettyEventLoopExecutorBuilder(settings));
    }

    public static ExecutorBuilder<?> executorBuilder(Settings settings) {
        int repositoryAzureMax = 5;
        if (DiscoveryNode.isStateless(settings)) {
            // REPOSITORY_THREAD_POOL_NAME is shared between snapshot and translogs/segments upload logic in serverless. In order to avoid
            // snapshots to slow down other uploads due to rate limiting, we allow more threads in serverless.
            repositoryAzureMax += ThreadPool.getMaxSnapshotThreadPoolSize(EsExecutors.allocatedProcessors(settings));
        }
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, repositoryAzureMax, TimeValue.timeValueSeconds(30L), false);
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

    private static boolean assertRepositoryAzureMaxThreads(Settings settings, ThreadPool threadPool) {
        if (DiscoveryNode.isStateless(settings)) {
            var repositoryAzureMax = threadPool.info(REPOSITORY_THREAD_POOL_NAME).getMax();
            var snapshotMax = ThreadPool.getMaxSnapshotThreadPoolSize(EsExecutors.allocatedProcessors(settings));
            assert snapshotMax < repositoryAzureMax
                : "thread pool ["
                    + REPOSITORY_THREAD_POOL_NAME
                    + "] should be large enough to allow all "
                    + snapshotMax
                    + " snapshot threads to run at once, but got: "
                    + repositoryAzureMax;
        }
        return true;
    }
}
