/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.http.netty.implementation.ReactorNettyClientProvider;
import com.azure.core.implementation.http.HttpClientProviders;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import org.elasticsearch.watcher.ResourceWatcherService;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
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
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService, BigArrays bigArrays,
                                                           RecoverySettings recoverySettings) {
        return Collections.singletonMap(AzureRepository.TYPE, metadata -> {
            AzureStorageService storageService = azureStoreService.get();
            assert storageService != null;
            return new AzureRepository(metadata, namedXContentRegistry, storageService, clusterService, bigArrays, recoverySettings);
        });
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        setUpDefaultHttpClientProvider();
        AzureClientProvider azureClientProvider = createClientProvider(threadPool, settings);
        azureStoreService.set(createAzureStorageService(settings, azureClientProvider));
        return List.of(azureClientProvider);
    }

    // Visible for testing
    AzureClientProvider createClientProvider(ThreadPool threadPool, Settings settings) {
        return AzureClientProvider.create(threadPool, settings);
    }

    AzureStorageService createAzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
        return new AzureStorageService(settings, azureClientProvider);
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
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(executorBuilder(), nettyEventLoopExecutorBuilder(settings));
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 5, TimeValue.timeValueSeconds(30L));
    }

    public static ExecutorBuilder<?> nettyEventLoopExecutorBuilder(Settings settings) {
        int eventLoopThreads = AzureClientProvider.eventLoopThreadsFromSettings(settings);
        return new ScalingExecutorBuilder(NETTY_EVENT_LOOP_THREAD_POOL_NAME, 0, eventLoopThreads, TimeValue.timeValueSeconds(30L));
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        if (clientsSettings.isEmpty()) {
            throw new SettingsException("If you want to use an azure repository, you need to define a client configuration.");
        }
        AzureStorageService storageService = azureStoreService.get();
        assert storageService != null;
        storageService.refreshSettings(clientsSettings);
    }

    @SuppressForbidden(reason = "needs to overcome the eager class loading problem")
    private void setUpDefaultHttpClientProvider() {
        // This is a huge hack to overcome a problem in the BlobBatchClient.
        // It should reuse the provided http client but instead it fallbacks and
        // tries to create a new one using the default provider.
        // The problem is that the provider is loaded during class creation and since the jars
        // are loaded in lexicographical order, the netty provider is not present at that time.
        // See https://github.com/Azure/azure-sdk-for-java/pull/17627 for a PR with a fix.
        try {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    final Field storageField = HttpClientProviders.class.getDeclaredField("defaultProvider");
                    storageField.setAccessible(true);
                    if (storageField.get(HttpClientProviders.class) == null) {
                        storageField.set(HttpClientProviders.class, new ReactorNettyClientProvider());
                    }
                    return null;
                } catch (Exception e) {
                    throw new IllegalStateException("HttpClient default provider could not be set up", e);
                }
            });
        } catch (Exception ignored) { }
    }
}
