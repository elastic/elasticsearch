/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.plugins.internal.InternalSearchPlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceServicesAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.action.TransportDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceServicesAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceActionProxy;
import org.elasticsearch.xpack.inference.action.TransportInferenceUsageAction;
import org.elasticsearch.xpack.inference.action.TransportPutInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportUnifiedCompletionInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportUpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter;
import org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator;
import org.elasticsearch.xpack.inference.common.InferenceServiceRateLimitCalculator;
import org.elasticsearch.xpack.inference.common.NoopNodeLocalRateLimitCalculator;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.queries.SemanticKnnVectorQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticMatchQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticSparseVectorQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.rank.random.RandomRankBuilder;
import org.elasticsearch.xpack.inference.rank.random.RandomRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankDoc;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ModelRegistryMetadata;
import org.elasticsearch.xpack.inference.rest.RestDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceServicesAction;
import org.elasticsearch.xpack.inference.rest.RestInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestPutInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestStreamInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestUpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicService;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioService;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.custom.CustomService;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioService;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserService;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.mistral.MistralService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerClient;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerService;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerConfiguration;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.common.InferenceAPIClusterAwareRateLimitingFeature.INFERENCE_API_CLUSTER_AWARE_RATE_LIMITING_FEATURE_FLAG;

public class InferencePlugin extends Plugin
    implements
        ActionPlugin,
        ExtensiblePlugin,
        SystemIndexPlugin,
        MapperPlugin,
        SearchPlugin,
        InternalSearchPlugin,
        ClusterPlugin {

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

    public static final LicensedFeature.Momentary INFERENCE_API_FEATURE = LicensedFeature.momentary(
        "inference",
        "api",
        License.OperationMode.ENTERPRISE
    );

    public static final String X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER = "X-elastic-product-use-case";

    public static final String NAME = "inference";
    public static final String UTILITY_THREAD_POOL_NAME = "inference_utility";

    private static final Logger log = LogManager.getLogger(InferencePlugin.class);

    private final Settings settings;
    private final SetOnce<HttpRequestSender.Factory> httpFactory = new SetOnce<>();
    private final SetOnce<AmazonBedrockRequestSender.Factory> amazonBedrockFactory = new SetOnce<>();
    private final SetOnce<HttpRequestSender.Factory> elasicInferenceServiceFactory = new SetOnce<>();
    private final SetOnce<ServiceComponents> serviceComponents = new SetOnce<>();
    // This is mainly so that the rest handlers can access the ThreadPool in a way that avoids potential null pointers from it
    // not being initialized yet
    private final SetOnce<ThreadPool> threadPoolSetOnce = new SetOnce<>();
    private final SetOnce<ElasticInferenceServiceComponents> elasticInferenceServiceComponents = new SetOnce<>();
    private final SetOnce<InferenceServiceRegistry> inferenceServiceRegistry = new SetOnce<>();
    private final SetOnce<ShardBulkInferenceActionFilter> shardBulkInferenceActionFilter = new SetOnce<>();
    private final SetOnce<ModelRegistry> modelRegistry = new SetOnce<>();
    private List<InferenceServiceExtension> inferenceServiceExtensions;

    public InferencePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(InferenceAction.INSTANCE, TransportInferenceAction.class),
            new ActionHandler(InferenceActionProxy.INSTANCE, TransportInferenceActionProxy.class),
            new ActionHandler(GetInferenceModelAction.INSTANCE, TransportGetInferenceModelAction.class),
            new ActionHandler(PutInferenceModelAction.INSTANCE, TransportPutInferenceModelAction.class),
            new ActionHandler(UpdateInferenceModelAction.INSTANCE, TransportUpdateInferenceModelAction.class),
            new ActionHandler(DeleteInferenceEndpointAction.INSTANCE, TransportDeleteInferenceEndpointAction.class),
            new ActionHandler(XPackUsageFeatureAction.INFERENCE, TransportInferenceUsageAction.class),
            new ActionHandler(GetInferenceDiagnosticsAction.INSTANCE, TransportGetInferenceDiagnosticsAction.class),
            new ActionHandler(GetInferenceServicesAction.INSTANCE, TransportGetInferenceServicesAction.class),
            new ActionHandler(UnifiedCompletionAction.INSTANCE, TransportUnifiedCompletionInferenceAction.class)
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
            new RestStreamInferenceAction(threadPoolSetOnce),
            new RestGetInferenceModelAction(),
            new RestPutInferenceModelAction(),
            new RestUpdateInferenceModelAction(),
            new RestDeleteInferenceEndpointAction(),
            new RestGetInferenceDiagnosticsAction(),
            new RestGetInferenceServicesAction()
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        var components = new ArrayList<>();
        var throttlerManager = new ThrottlerManager(settings, services.threadPool());
        throttlerManager.init(services.clusterService());

        var truncator = new Truncator(settings, services.clusterService());
        serviceComponents.set(new ServiceComponents(services.threadPool(), throttlerManager, settings, truncator));
        threadPoolSetOnce.set(services.threadPool());

        var httpClientManager = HttpClientManager.create(settings, services.threadPool(), services.clusterService(), throttlerManager);
        var httpRequestSenderFactory = new HttpRequestSender.Factory(serviceComponents.get(), httpClientManager, services.clusterService());
        httpFactory.set(httpRequestSenderFactory);

        var amazonBedrockRequestSenderFactory = new AmazonBedrockRequestSender.Factory(serviceComponents.get(), services.clusterService());
        amazonBedrockFactory.set(amazonBedrockRequestSenderFactory);

        modelRegistry.set(new ModelRegistry(services.clusterService(), services.client()));
        services.clusterService().addListener(modelRegistry.get());

        if (inferenceServiceExtensions == null) {
            inferenceServiceExtensions = new ArrayList<>();
        }
        var inferenceServices = new ArrayList<>(inferenceServiceExtensions);
        inferenceServices.add(this::getInferenceServiceFactories);

        var inferenceServiceSettings = new ElasticInferenceServiceSettings(settings);
        inferenceServiceSettings.init(services.clusterService());

        // Create a separate instance of HTTPClientManager with its own SSL configuration (`xpack.inference.elastic.http.ssl.*`).
        var elasticInferenceServiceHttpClientManager = HttpClientManager.create(
            settings,
            services.threadPool(),
            services.clusterService(),
            throttlerManager,
            getSslService(),
            inferenceServiceSettings.getConnectionTtl()
        );

        var elasticInferenceServiceRequestSenderFactory = new HttpRequestSender.Factory(
            serviceComponents.get(),
            elasticInferenceServiceHttpClientManager,
            services.clusterService()
        );
        elasicInferenceServiceFactory.set(elasticInferenceServiceRequestSenderFactory);

        var authorizationHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            inferenceServiceSettings.getElasticInferenceServiceUrl(),
            services.threadPool()
        );

        var sageMakerSchemas = new SageMakerSchemas();
        var sageMakerConfigurations = new LazyInitializable<>(new SageMakerConfiguration(sageMakerSchemas));
        inferenceServices.add(
            () -> List.of(
                context -> new ElasticInferenceService(
                    elasicInferenceServiceFactory.get(),
                    serviceComponents.get(),
                    inferenceServiceSettings,
                    modelRegistry.get(),
                    authorizationHandler,
                    context
                ),
                context -> new SageMakerService(
                    new SageMakerModelBuilder(sageMakerSchemas),
                    new SageMakerClient(
                        new SageMakerClient.Factory(new HttpSettings(settings, services.clusterService())),
                        services.threadPool()
                    ),
                    sageMakerSchemas,
                    services.threadPool(),
                    sageMakerConfigurations::getOrCompute,
                    context
                )
            )
        );

        var factoryContext = new InferenceServiceExtension.InferenceServiceFactoryContext(
            services.client(),
            services.threadPool(),
            services.clusterService(),
            settings
        );

        // This must be done after the HttpRequestSenderFactory is created so that the services can get the
        // reference correctly
        var serviceRegistry = new InferenceServiceRegistry(inferenceServices, factoryContext);
        serviceRegistry.init(services.client());
        for (var service : serviceRegistry.getServices().values()) {
            service.defaultConfigIds().forEach(modelRegistry.get()::addDefaultIds);
        }
        inferenceServiceRegistry.set(serviceRegistry);

        var meterRegistry = services.telemetryProvider().getMeterRegistry();
        var inferenceStats = InferenceStats.create(meterRegistry);
        var inferenceStatsBinding = new PluginComponentBinding<>(InferenceStats.class, inferenceStats);

        var actionFilter = new ShardBulkInferenceActionFilter(
            services.clusterService(),
            serviceRegistry,
            modelRegistry.get(),
            getLicenseState(),
            services.indexingPressure(),
            inferenceStats
        );
        shardBulkInferenceActionFilter.set(actionFilter);

        components.add(serviceRegistry);
        components.add(modelRegistry.get());
        components.add(httpClientManager);
        components.add(inferenceStatsBinding);

        // Only add InferenceServiceNodeLocalRateLimitCalculator (which is a ClusterStateListener) for cluster aware rate limiting,
        // if the rate limiting feature flags are enabled, otherwise provide noop implementation
        InferenceServiceRateLimitCalculator calculator;
        if (INFERENCE_API_CLUSTER_AWARE_RATE_LIMITING_FEATURE_FLAG) {
            calculator = new InferenceServiceNodeLocalRateLimitCalculator(services.clusterService(), serviceRegistry);
        } else {
            calculator = new NoopNodeLocalRateLimitCalculator();
        }

        // Add binding for interface -> implementation
        components.add(new PluginComponentBinding<>(InferenceServiceRateLimitCalculator.class, calculator));

        return components;
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        inferenceServiceExtensions = loader.loadExtensions(InferenceServiceExtension.class);
    }

    public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
        return List.of(
            context -> new HuggingFaceElserService(httpFactory.get(), serviceComponents.get(), context),
            context -> new HuggingFaceService(httpFactory.get(), serviceComponents.get(), context),
            context -> new OpenAiService(httpFactory.get(), serviceComponents.get(), context),
            context -> new CohereService(httpFactory.get(), serviceComponents.get(), context),
            context -> new AzureOpenAiService(httpFactory.get(), serviceComponents.get(), context),
            context -> new AzureAiStudioService(httpFactory.get(), serviceComponents.get(), context),
            context -> new GoogleAiStudioService(httpFactory.get(), serviceComponents.get(), context),
            context -> new GoogleVertexAiService(httpFactory.get(), serviceComponents.get(), context),
            context -> new MistralService(httpFactory.get(), serviceComponents.get(), context),
            context -> new AnthropicService(httpFactory.get(), serviceComponents.get(), context),
            context -> new AmazonBedrockService(httpFactory.get(), amazonBedrockFactory.get(), serviceComponents.get(), context),
            context -> new AlibabaCloudSearchService(httpFactory.get(), serviceComponents.get(), context),
            context -> new IbmWatsonxService(httpFactory.get(), serviceComponents.get(), context),
            context -> new JinaAIService(httpFactory.get(), serviceComponents.get(), context),
            context -> new VoyageAIService(httpFactory.get(), serviceComponents.get(), context),
            context -> new DeepSeekService(httpFactory.get(), serviceComponents.get(), context),
            ElasticsearchInternalService::new,
            context -> new CustomService(httpFactory.get(), serviceComponents.get(), context)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        var entries = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(RankBuilder.class, TextSimilarityRankBuilder.NAME, TextSimilarityRankBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(RankBuilder.class, RandomRankBuilder.NAME, RandomRankBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(RankDoc.class, TextSimilarityRankDoc.NAME, TextSimilarityRankDoc::new));
        entries.add(new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, ModelRegistryMetadata.TYPE, ModelRegistryMetadata::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, ModelRegistryMetadata.TYPE, ModelRegistryMetadata::readDiffFrom));
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(ModelRegistryMetadata.TYPE),
                ModelRegistryMetadata::fromXContent
            )
        );
        return namedXContent;
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
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setNetNew()
                .build()
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settingsToUse) {
        return List.of(inferenceUtilityExecutor(settings));
    }

    public static ExecutorBuilder<?> inferenceUtilityExecutor(Settings settings) {
        return new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            0,
            10,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.inference.utility_thread_pool"
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(HttpSettings.getSettingsDefinitions());
        settings.addAll(HttpClientManager.getSettingsDefinitions());
        settings.addAll(ThrottlerManager.getSettingsDefinitions());
        settings.addAll(RetrySettings.getSettingsDefinitions());
        settings.addAll(Truncator.getSettingsDefinitions());
        settings.addAll(RequestExecutorServiceSettings.getSettingsDefinitions());
        settings.add(SKIP_VALIDATE_AND_START);
        settings.add(INDICES_INFERENCE_BATCH_SIZE);
        settings.addAll(ElasticInferenceServiceSettings.getSettingsDefinitions());

        return settings;
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
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(SemanticInferenceMetadataFieldsMapper.NAME, SemanticInferenceMetadataFieldsMapper.PARSER);
    }

    // Overridable for tests
    protected Supplier<ModelRegistry> getModelRegistry() {
        return () -> modelRegistry.get();
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
            SemanticTextFieldMapper.CONTENT_TYPE,
            SemanticTextFieldMapper.parser(getModelRegistry()),
            OffsetSourceFieldMapper.CONTENT_TYPE,
            OffsetSourceFieldMapper.PARSER
        );
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return singletonList(shardBulkInferenceActionFilter.get());
    }

    public List<QuerySpec<?>> getQueries() {
        return List.of(new QuerySpec<>(SemanticQueryBuilder.NAME, SemanticQueryBuilder::new, SemanticQueryBuilder::fromXContent));
    }

    @Override
    public List<QueryRewriteInterceptor> getQueryRewriteInterceptors() {
        return List.of(
            new SemanticKnnVectorQueryRewriteInterceptor(),
            new SemanticMatchQueryRewriteInterceptor(),
            new SemanticSparseVectorQueryRewriteInterceptor()
        );
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return List.of(
            new RetrieverSpec<>(
                new ParseField(TextSimilarityRankBuilder.NAME),
                (parser, context) -> TextSimilarityRankRetrieverBuilder.fromXContent(parser, context, getLicenseState())
            ),
            new RetrieverSpec<>(new ParseField(RandomRankBuilder.NAME), RandomRankRetrieverBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Highlighter> getHighlighters() {
        return Map.of(SemanticTextHighlighter.NAME, new SemanticTextHighlighter());
    }

    @Override
    public void onNodeStarted() {
        var registry = inferenceServiceRegistry.get();

        if (registry != null) {
            registry.onNodeStarted();
        }
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        return Set.of(new RestHeaderDefinition(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, false));
    }

    @Override
    public Collection<String> getTaskHeaders() {
        return Set.of(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
    }

    protected SSLService getSslService() {
        return XPackPlugin.getSharedSslService();
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
