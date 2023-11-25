/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.action.DeleteInferenceModelAction;
import org.elasticsearch.xpack.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportDeleteInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportPutInferenceModelAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.rest.RestDeleteInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestPutInferenceModelAction;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeService;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferencePlugin extends Plugin implements ActionPlugin, InferenceServicePlugin, SystemIndexPlugin {

    public static final String NAME = "inference";
    public static final String UTILITY_THREAD_POOL_NAME = "inference_utility";
    private final Settings settings;
    // We'll keep a reference to the http manager just in case the inference services don't get closed individually
    private final SetOnce<HttpClientManager> httpManager = new SetOnce<>();
    private final SetOnce<HttpRequestSenderFactory> httpFactory = new SetOnce<>();
    private final SetOnce<ServiceComponents> serviceComponents = new SetOnce<>();

    public InferencePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(InferenceAction.INSTANCE, TransportInferenceAction.class),
            new ActionHandler<>(GetInferenceModelAction.INSTANCE, TransportGetInferenceModelAction.class),
            new ActionHandler<>(PutInferenceModelAction.INSTANCE, TransportPutInferenceModelAction.class),
            new ActionHandler<>(DeleteInferenceModelAction.INSTANCE, TransportDeleteInferenceModelAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestInferenceAction(),
            new RestGetInferenceModelAction(),
            new RestPutInferenceModelAction(),
            new RestDeleteInferenceModelAction()
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        var throttlerManager = new ThrottlerManager(settings, services.threadPool(), services.clusterService());
        serviceComponents.set(new ServiceComponents(services.threadPool(), throttlerManager, settings));

        httpManager.set(HttpClientManager.create(settings, services.threadPool(), services.clusterService(), throttlerManager));

        var httpRequestSenderFactory = new HttpRequestSenderFactory(
            services.threadPool(),
            httpManager.get(),
            services.clusterService(),
            settings
        );
        httpFactory.set(httpRequestSenderFactory);

        ModelRegistry modelRegistry = new ModelRegistry(services.client());
        return List.of(modelRegistry);
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                .setIndexPattern(InferenceIndex.INDEX_PATTERN)
                .setPrimaryIndex(InferenceIndex.INDEX_NAME)
                .setDescription("Contains inference service and model configuration")
                .setMappings(InferenceIndex.mappings())
                .setSettings(InferenceIndex.settings())
                .setVersionMetaKey("version")
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .build(),
            SystemIndexDescriptor.builder()
                .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                .setIndexPattern(InferenceSecretsIndex.INDEX_PATTERN)
                .setPrimaryIndex(InferenceSecretsIndex.INDEX_NAME)
                .setDescription("Contains inference service secrets")
                .setMappings(InferenceSecretsIndex.mappings())
                .setSettings(InferenceSecretsIndex.settings())
                .setVersionMetaKey("version")
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setNetNew()
                .build()
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settingsToUse) {
        return List.of(
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                0,
                10,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.utility_thread_pool"
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Stream.of(
            HttpSettings.getSettings(),
            HttpClientManager.getSettings(),
            HttpRequestSenderFactory.HttpRequestSender.getSettings(),
            ThrottlerManager.getSettings(),
            RetrySettings.getSettingsDefinitions()
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public String getFeatureName() {
        return "inference_plugin";
    }

    @Override
    public String getFeatureDescription() {
        return "Inference plugin for managing inference services and inference";
    }

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(
            ElserMlNodeService::new,
            context -> new HuggingFaceElserService(httpFactory, serviceComponents),
            context -> new OpenAiService(httpFactory, serviceComponents)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getInferenceServiceNamedWriteables() {
        return InferenceNamedWriteablesProvider.getNamedWriteables();
    }

    @Override
    public void close() {
        var serviceComponentsRef = serviceComponents.get();
        var throttlerToClose = serviceComponentsRef != null ? serviceComponentsRef.throttlerManager() : null;

        IOUtils.closeWhileHandlingException(httpManager.get(), throttlerToClose);
    }
}
