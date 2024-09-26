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
import org.elasticsearch.action.support.MappedActionFilter;
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
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceUsageAction;
import org.elasticsearch.xpack.inference.action.TransportPutInferenceModelAction;
import org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.rank.random.RandomRankBuilder;
import org.elasticsearch.xpack.inference.rank.random.RandomRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.rest.RestDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestPutInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestStreamInferenceAction;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockService;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicService;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioService;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceFeature;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalService;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioService;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserService;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.mistral.MistralService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.telemetry.ApmInferenceStats;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

public class InferencePlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, SystemIndexPlugin, MapperPlugin, SearchPlugin {

    /**
     * When this setting is true the verification check that
     * connects to the external service will not be made at
     * model creation and ml node models will not be deployed.
     *
     * This setting exists for testing service configurations in
     * rolling upgrade test without connecting to those services,
     * it should not be enabled in production.
     */
    public static final Setting<Boolean> SKIP_VALIDATE_AND_START = Setting.boolSetting(
        "xpack.inference.skip_validate_and_start",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String NAME = "inference";
    public static final String UTILITY_THREAD_POOL_NAME = "inference_utility";

    private final Settings settings;
    private final SetOnce<HttpRequestSender.Factory> httpFactory = new SetOnce<>();
    private final SetOnce<AmazonBedrockRequestSender.Factory> amazonBedrockFactory = new SetOnce<>();
    private final SetOnce<ServiceComponents> serviceComponents = new SetOnce<>();
    private final SetOnce<ElasticInferenceServiceComponents> eisComponents = new SetOnce<>();
    private final SetOnce<InferenceServiceRegistry> inferenceServiceRegistry = new SetOnce<>();
    private final SetOnce<ShardBulkInferenceActionFilter> shardBulkInferenceActionFilter = new SetOnce<>();
    private List<InferenceServiceExtension> inferenceServiceExtensions;

    public InferencePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(InferenceAction.INSTANCE, TransportInferenceAction.class),
            new ActionHandler<>(GetInferenceModelAction.INSTANCE, TransportGetInferenceModelAction.class),
            new ActionHandler<>(PutInferenceModelAction.INSTANCE, TransportPutInferenceModelAction.class),
            new ActionHandler<>(DeleteInferenceEndpointAction.INSTANCE, TransportDeleteInferenceEndpointAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.INFERENCE, TransportInferenceUsageAction.class),
            new ActionHandler<>(GetInferenceDiagnosticsAction.INSTANCE, TransportGetInferenceDiagnosticsAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new RestInferenceAction(),
            new RestStreamInferenceAction(),
            new RestGetInferenceModelAction(),
            new RestPutInferenceModelAction(),
            new RestDeleteInferenceEndpointAction(),
            new RestGetInferenceDiagnosticsAction()
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        var throttlerManager = new ThrottlerManager(settings, services.threadPool(), services.clusterService());
        var truncator = new Truncator(settings, services.clusterService());
        serviceComponents.set(new ServiceComponents(services.threadPool(), throttlerManager, settings, truncator));

        var httpClientManager = HttpClientManager.create(settings, services.threadPool(), services.clusterService(), throttlerManager);
        var httpRequestSenderFactory = new HttpRequestSender.Factory(serviceComponents.get(), httpClientManager, services.clusterService());
        httpFactory.set(httpRequestSenderFactory);

        var amazonBedrockRequestSenderFactory = new AmazonBedrockRequestSender.Factory(serviceComponents.get(), services.clusterService());
        amazonBedrockFactory.set(amazonBedrockRequestSenderFactory);

        ModelRegistry modelRegistry = new ModelRegistry(services.client());

        if (inferenceServiceExtensions == null) {
            inferenceServiceExtensions = new ArrayList<>();
        }
        var inferenceServices = new ArrayList<>(inferenceServiceExtensions);
        inferenceServices.add(this::getInferenceServiceFactories);

        if (ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()) {
            ElasticInferenceServiceSettings eisSettings = new ElasticInferenceServiceSettings(settings);
            eisComponents.set(new ElasticInferenceServiceComponents(eisSettings.getEisGatewayUrl()));

            inferenceServices.add(
                () -> List.of(context -> new ElasticInferenceService(httpFactory.get(), serviceComponents.get(), eisComponents.get()))
            );
        }

        var factoryContext = new InferenceServiceExtension.InferenceServiceFactoryContext(services.client());
        // This must be done after the HttpRequestSenderFactory is created so that the services can get the
        // reference correctly
        var registry = new InferenceServiceRegistry(inferenceServices, factoryContext);
        registry.init(services.client());
        inferenceServiceRegistry.set(registry);

        var actionFilter = new ShardBulkInferenceActionFilter(registry, modelRegistry);
        shardBulkInferenceActionFilter.set(actionFilter);

        var meterRegistry = services.telemetryProvider().getMeterRegistry();
        var stats = new PluginComponentBinding<>(InferenceStats.class, ApmInferenceStats.create(meterRegistry));

        return List.of(modelRegistry, registry, httpClientManager, stats);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        inferenceServiceExtensions = loader.loadExtensions(InferenceServiceExtension.class);
    }

    public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
        return List.of(
            ElserInternalService::new,
            context -> new HuggingFaceElserService(httpFactory.get(), serviceComponents.get()),
            context -> new HuggingFaceService(httpFactory.get(), serviceComponents.get()),
            context -> new OpenAiService(httpFactory.get(), serviceComponents.get()),
            context -> new CohereService(httpFactory.get(), serviceComponents.get()),
            context -> new AzureOpenAiService(httpFactory.get(), serviceComponents.get()),
            context -> new AzureAiStudioService(httpFactory.get(), serviceComponents.get()),
            context -> new GoogleAiStudioService(httpFactory.get(), serviceComponents.get()),
            context -> new GoogleVertexAiService(httpFactory.get(), serviceComponents.get()),
            context -> new MistralService(httpFactory.get(), serviceComponents.get()),
            context -> new AnthropicService(httpFactory.get(), serviceComponents.get()),
            context -> new AmazonBedrockService(httpFactory.get(), amazonBedrockFactory.get(), serviceComponents.get()),
            context -> new AlibabaCloudSearchService(httpFactory.get(), serviceComponents.get()),
            context -> new IbmWatsonxService(httpFactory.get(), serviceComponents.get()),
            ElasticsearchInternalService::new
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        var entries = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(RankBuilder.class, TextSimilarityRankBuilder.NAME, TextSimilarityRankBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(RankBuilder.class, RandomRankBuilder.NAME, RandomRankBuilder::new));
        return entries;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {

        var inferenceIndexV1Descriptor = SystemIndexDescriptor.builder()
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .setIndexPattern(InferenceIndex.INDEX_PATTERN)
            .setAliasName(InferenceIndex.INDEX_ALIAS)
            .setPrimaryIndex(InferenceIndex.INDEX_NAME)
            .setDescription("Contains inference service and model configuration")
            .setMappings(InferenceIndex.mappingsV1())
            .setSettings(InferenceIndex.settings())
            .setVersionMetaKey("version")
            .setOrigin(ClientHelper.INFERENCE_ORIGIN)
            .build();

        return List.of(
            SystemIndexDescriptor.builder()
                .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                .setIndexPattern(InferenceIndex.INDEX_PATTERN)
                .setAliasName(InferenceIndex.INDEX_ALIAS)
                .setPrimaryIndex(InferenceIndex.INDEX_NAME)
                .setDescription("Contains inference service and model configuration")
                .setMappings(InferenceIndex.mappings())
                .setSettings(InferenceIndex.settings())
                .setVersionMetaKey("version")
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setPriorSystemIndexDescriptors(List.of(inferenceIndexV1Descriptor))
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
            HttpSettings.getSettingsDefinitions(),
            HttpClientManager.getSettingsDefinitions(),
            ThrottlerManager.getSettingsDefinitions(),
            RetrySettings.getSettingsDefinitions(),
            ElasticInferenceServiceSettings.getSettingsDefinitions(),
            Truncator.getSettingsDefinitions(),
            RequestExecutorServiceSettings.getSettingsDefinitions(),
            List.of(SKIP_VALIDATE_AND_START)
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
    public void close() {
        var serviceComponentsRef = serviceComponents.get();
        var throttlerToClose = serviceComponentsRef != null ? serviceComponentsRef.throttlerManager() : null;

        IOUtils.closeWhileHandlingException(inferenceServiceRegistry.get(), throttlerToClose);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(SemanticTextFieldMapper.CONTENT_TYPE, SemanticTextFieldMapper.PARSER);
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return singletonList(shardBulkInferenceActionFilter.get());
    }

    public List<QuerySpec<?>> getQueries() {
        return List.of(new QuerySpec<>(SemanticQueryBuilder.NAME, SemanticQueryBuilder::new, SemanticQueryBuilder::fromXContent));
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return List.of(
            new RetrieverSpec<>(new ParseField(TextSimilarityRankBuilder.NAME), TextSimilarityRankRetrieverBuilder::fromXContent),
            new RetrieverSpec<>(new ParseField(RandomRankBuilder.NAME), RandomRankRetrieverBuilder::fromXContent)
        );
    }
}
