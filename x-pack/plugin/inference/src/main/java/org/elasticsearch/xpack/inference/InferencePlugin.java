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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
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
import org.elasticsearch.xpack.core.inference.action.DeleteCCMConfigurationAction;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.GetCCMConfigurationAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceServicesAction;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.action.TransportDeleteCCMConfigurationAction;
import org.elasticsearch.xpack.inference.action.TransportDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.action.TransportGetCCMConfigurationAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceFieldsAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportGetInferenceServicesAction;
import org.elasticsearch.xpack.inference.action.TransportGetRerankerWindowSizeAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportInferenceActionProxy;
import org.elasticsearch.xpack.inference.action.TransportInferenceUsageAction;
import org.elasticsearch.xpack.inference.action.TransportPutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.action.TransportPutInferenceModelAction;
import org.elasticsearch.xpack.inference.action.TransportStoreEndpointsAction;
import org.elasticsearch.xpack.inference.action.TransportUnifiedCompletionInferenceAction;
import org.elasticsearch.xpack.inference.action.TransportUpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.queries.InterceptedInferenceKnnVectorQueryBuilder;
import org.elasticsearch.xpack.inference.queries.InterceptedInferenceMatchQueryBuilder;
import org.elasticsearch.xpack.inference.queries.InterceptedInferenceSparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticKnnVectorQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticMatchQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticSparseVectorQueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.rank.random.RandomRankBuilder;
import org.elasticsearch.xpack.inference.rank.random.RandomRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankDoc;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;
import org.elasticsearch.xpack.inference.registry.ClearInferenceEndpointCacheAction;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ModelRegistryMetadata;
import org.elasticsearch.xpack.inference.rest.RestDeleteCCMConfigurationAction;
import org.elasticsearch.xpack.inference.rest.RestDeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.rest.RestGetCCMConfigurationAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestGetInferenceServicesAction;
import org.elasticsearch.xpack.inference.rest.RestInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestPutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.rest.RestPutInferenceModelAction;
import org.elasticsearch.xpack.inference.rest.RestStreamInferenceAction;
import org.elasticsearch.xpack.inference.rest.RestUpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ai21.Ai21Service;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicService;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioService;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiService;
import org.elasticsearch.xpack.inference.services.custom.CustomService;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMCache;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMIndex;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMInformedSettings;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMPersistentStorageService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioService;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserService;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.llama.LlamaService;
import org.elasticsearch.xpack.inference.services.mistral.MistralService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiService;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerClient;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerService;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerConfiguration;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;

public class InferencePlugin extends Plugin
    implements
        ActionPlugin,
        ExtensiblePlugin,
        SystemIndexPlugin,
        MapperPlugin,
        SearchPlugin,
        InternalSearchPlugin,
        ClusterPlugin,
        PersistentTaskPlugin {

    private static final Logger logger = LogManager.getLogger(InferencePlugin.class);

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
    public static final Setting<TimeValue> INFERENCE_QUERY_TIMEOUT = Setting.timeSetting(
        "xpack.inference.query_timeout",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final LicensedFeature.Momentary INFERENCE_API_FEATURE = LicensedFeature.momentary(
        "inference",
        "api",
        License.OperationMode.ENTERPRISE
    );

    public static final LicensedFeature.Momentary EIS_INFERENCE_FEATURE = LicensedFeature.momentary(
        "inference",
        "Elastic Inference Service",
        License.OperationMode.BASIC
    );

    public static final String X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER = "X-elastic-product-use-case";
    public static final String X_ELASTIC_ES_VERSION = "X-elastic-es-version";

    public static final String NAME = "inference";
    public static final String UTILITY_THREAD_POOL_NAME = "inference_utility";
    public static final String INFERENCE_RESPONSE_THREAD_POOL_NAME = "inference_response";

    private final Settings settings;
    private final SetOnce<HttpRequestSender.Factory> httpFactory = new SetOnce<>();
    private final SetOnce<AmazonBedrockRequestSender.Factory> amazonBedrockFactory = new SetOnce<>();
    private final SetOnce<HttpRequestSender.Factory> elasticInferenceServiceFactory = new SetOnce<>();
    private final SetOnce<ServiceComponents> serviceComponents = new SetOnce<>();
    // This is mainly so that the rest handlers can access the ThreadPool in a way that avoids potential null pointers from it
    // not being initialized yet
    private final SetOnce<ThreadPool> threadPoolSetOnce = new SetOnce<>();
    private final SetOnce<InferenceServiceRegistry> inferenceServiceRegistry = new SetOnce<>();
    private final SetOnce<ShardBulkInferenceActionFilter> shardBulkInferenceActionFilter = new SetOnce<>();
    private final SetOnce<ModelRegistry> modelRegistry = new SetOnce<>();
    private final SetOnce<CCMFeature> ccmFeature = new SetOnce<>();
    private List<InferenceServiceExtension> inferenceServiceExtensions;
    private final SetOnce<AuthorizationTaskExecutor> authorizationTaskExecutorRef = new SetOnce<>();

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
            new ActionHandler(UnifiedCompletionAction.INSTANCE, TransportUnifiedCompletionInferenceAction.class),
            new ActionHandler(GetRerankerWindowSizeAction.INSTANCE, TransportGetRerankerWindowSizeAction.class),
            new ActionHandler(ClearInferenceEndpointCacheAction.INSTANCE, ClearInferenceEndpointCacheAction.class),
            new ActionHandler(StoreInferenceEndpointsAction.INSTANCE, TransportStoreEndpointsAction.class),
            new ActionHandler(GetCCMConfigurationAction.INSTANCE, TransportGetCCMConfigurationAction.class),
            new ActionHandler(PutCCMConfigurationAction.INSTANCE, TransportPutCCMConfigurationAction.class),
            new ActionHandler(DeleteCCMConfigurationAction.INSTANCE, TransportDeleteCCMConfigurationAction.class),
            new ActionHandler(CCMCache.ClearCCMCacheAction.INSTANCE, CCMCache.ClearCCMCacheAction.class),
            new ActionHandler(AuthorizationTaskExecutor.Action.INSTANCE, AuthorizationTaskExecutor.Action.class),
            new ActionHandler(GetInferenceFieldsAction.INSTANCE, TransportGetInferenceFieldsAction.class)
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
            new RestGetInferenceServicesAction(),
            new RestGetCCMConfigurationAction(ccmFeature.get()),
            new RestPutCCMConfigurationAction(ccmFeature.get()),
            new RestDeleteCCMConfigurationAction(ccmFeature.get())
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        var components = new ArrayList<>();
        ccmFeature.set(new CCMFeature(settings));
        components.add(ccmFeature.get());

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

        var inferenceServiceSettings = new CCMInformedSettings(settings, ccmFeature.get());
        inferenceServiceSettings.init(services.clusterService());

        var eisRequestSenderFactoryComponents = createEisRequestSenderFactory(
            services,
            throttlerManager,
            inferenceServiceSettings,
            ccmFeature.get()
        );
        var elasticInferenceServiceHttpClientManager = eisRequestSenderFactoryComponents.httpClientManager();
        elasticInferenceServiceFactory.set(eisRequestSenderFactoryComponents.factory());

        var sageMakerSchemas = new SageMakerSchemas();
        var sageMakerConfigurations = new LazyInitializable<>(new SageMakerConfiguration(sageMakerSchemas));

        var ccmRelatedComponents = createCCMDependentComponents(
            services,
            inferenceServiceSettings,
            serviceComponents.get(),
            elasticInferenceServiceFactory.get().createSender(),
            modelRegistry.get(),
            ccmFeature.get()
        );
        components.addAll(ccmRelatedComponents.components());

        inferenceServices.add(() -> List.of(context -> {
            var eisService = new ElasticInferenceService(
                elasticInferenceServiceFactory.get(),
                serviceComponents.get(),
                inferenceServiceSettings,
                context,
                ccmRelatedComponents.ccmAuthApplierFactory()
            );
            eisService.init();
            return eisService;
        },
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
        ));

        var meterRegistry = services.telemetryProvider().getMeterRegistry();
        var inferenceStats = InferenceStats.create(meterRegistry);
        var inferenceStatsBinding = new PluginComponentBinding<>(InferenceStats.class, inferenceStats);

        var factoryContext = new InferenceServiceExtension.InferenceServiceFactoryContext(
            services.client(),
            services.threadPool(),
            services.clusterService(),
            settings,
            inferenceStats
        );

        // This must be done after the HttpRequestSenderFactory is created so that the services can get the
        // reference correctly
        var serviceRegistry = new InferenceServiceRegistry(inferenceServices, factoryContext);
        for (var service : serviceRegistry.getServices().values()) {
            service.defaultConfigIds().forEach(modelRegistry.get()::addDefaultIds);
        }
        inferenceServiceRegistry.set(serviceRegistry);

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
        components.add(
            new TransportGetInferenceDiagnosticsAction.ClientManagers(httpClientManager, elasticInferenceServiceHttpClientManager)
        );
        components.add(inferenceStatsBinding);
        components.add(new PluginComponentBinding<>(Sender.class, elasticInferenceServiceFactory.get().createSender()));
        components.add(
            new InferenceEndpointRegistry(
                services.clusterService(),
                settings,
                modelRegistry.get(),
                serviceRegistry,
                services.projectResolver(),
                services.featureService()
            )
        );

        return components;
    }

    private record CCMRelatedComponents(Collection<?> components, CCMAuthenticationApplierFactory ccmAuthApplierFactory) {}

    private CCMRelatedComponents createCCMDependentComponents(
        PluginServices services,
        ElasticInferenceServiceSettings inferenceServiceSettings,
        ServiceComponents serviceComponents,
        Sender sender,
        ModelRegistry modelRegistry,
        CCMFeature ccmFeature
    ) {
        var ccmPersistentStorageService = new CCMPersistentStorageService(services.client());
        var ccmService = new CCMService(ccmPersistentStorageService, services.client());
        var ccmAuthApplierFactory = new CCMAuthenticationApplierFactory(ccmFeature, ccmService);

        var authorizationHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            inferenceServiceSettings.getElasticInferenceServiceUrl(),
            services.threadPool(),
            ccmAuthApplierFactory
        );

        var authTaskExecutor = AuthorizationTaskExecutor.create(
            services.clusterService(),
            services.featureService(),
            new AuthorizationPoller.Parameters(
                serviceComponents,
                authorizationHandler,
                sender,
                inferenceServiceSettings,
                modelRegistry,
                services.client(),
                ccmFeature,
                ccmService
            )
        );
        authorizationTaskExecutorRef.set(authTaskExecutor);

        // If CCM is not allowed in this environment then we can initialize the auth poller task because
        // authentication with EIS will be through certs that are already configured. If CCM configuration is allowed,
        // we need to wait for the user to provide an API key before we can start polling EIS
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            logger.info("CCM configuration is not permitted - starting EIS authorization task executor");
            authTaskExecutor.startAndLazyCreateTask();
        }

        return new CCMRelatedComponents(
            List.of(
                authorizationHandler,
                authTaskExecutor,
                ccmService,
                ccmPersistentStorageService,
                new CCMCache(
                    ccmPersistentStorageService,
                    services.clusterService(),
                    settings,
                    services.featureService(),
                    services.projectResolver(),
                    services.client()
                )
            ),
            ccmAuthApplierFactory
        );
    }

    private record EisRequestSenderComponents(HttpRequestSender.Factory factory, HttpClientManager httpClientManager) {}

    private EisRequestSenderComponents createEisRequestSenderFactory(
        PluginServices services,
        ThrottlerManager throttlerManager,
        ElasticInferenceServiceSettings inferenceServiceSettings,
        CCMFeature ccmFeature
    ) {
        // Create a separate instance of HTTPClientManager with its own SSL configuration (`xpack.inference.elastic.http.ssl.*`).
        HttpClientManager manager;
        if (ccmFeature.isCcmSupportedEnvironment()) {
            // If ccm is configurable then we aren't using mTLS so ignore the ssl service
            manager = HttpClientManager.create(
                settings,
                services.threadPool(),
                services.clusterService(),
                throttlerManager,
                inferenceServiceSettings.getConnectionTtl()
            );
        } else {
            manager = HttpClientManager.create(
                settings,
                services.threadPool(),
                services.clusterService(),
                throttlerManager,
                getSslService(),
                inferenceServiceSettings.getConnectionTtl()
            );
        }

        return new EisRequestSenderComponents(
            new HttpRequestSender.Factory(serviceComponents.get(), manager, services.clusterService()),
            manager
        );
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(authorizationTaskExecutorRef.get());
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
            context -> new ContextualAiService(httpFactory.get(), serviceComponents.get(), context),
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
            context -> new LlamaService(httpFactory.get(), serviceComponents.get(), context),
            context -> new Ai21Service(httpFactory.get(), serviceComponents.get(), context),
            context -> new OpenShiftAiService(httpFactory.get(), serviceComponents.get(), context),
            ElasticsearchInternalService::new,
            context -> new CustomService(httpFactory.get(), serviceComponents.get(), context)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Stream.of(
            List.of(
                new NamedWriteableRegistry.Entry(RankBuilder.class, TextSimilarityRankBuilder.NAME, TextSimilarityRankBuilder::new),
                new NamedWriteableRegistry.Entry(RankBuilder.class, RandomRankBuilder.NAME, RandomRankBuilder::new),
                new NamedWriteableRegistry.Entry(RankDoc.class, TextSimilarityRankDoc.NAME, TextSimilarityRankDoc::new),
                new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, ModelRegistryMetadata.TYPE, ModelRegistryMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, ModelRegistryMetadata.TYPE, ModelRegistryMetadata::readDiffFrom),
                new NamedWriteableRegistry.Entry(
                    QueryBuilder.class,
                    InterceptedInferenceMatchQueryBuilder.NAME,
                    InterceptedInferenceMatchQueryBuilder::new
                ),
                new NamedWriteableRegistry.Entry(
                    QueryBuilder.class,
                    InterceptedInferenceKnnVectorQueryBuilder.NAME,
                    InterceptedInferenceKnnVectorQueryBuilder::new
                ),
                new NamedWriteableRegistry.Entry(
                    QueryBuilder.class,
                    InterceptedInferenceSparseVectorQueryBuilder.NAME,
                    InterceptedInferenceSparseVectorQueryBuilder::new
                )
            ),
            InferenceNamedWriteablesProvider.getNamedWriteables(),
            AuthorizationTaskExecutor.getNamedWriteables()
        ).flatMap(List::stream).toList();

    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Stream.of(
            List.of(
                new NamedXContentRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    new ParseField(ModelRegistryMetadata.TYPE),
                    ModelRegistryMetadata::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    new ParseField(ClearInferenceEndpointCacheAction.InvalidateCacheMetadata.NAME),
                    ClearInferenceEndpointCacheAction.InvalidateCacheMetadata::fromXContent
                )
            ),
            AuthorizationTaskExecutor.getNamedXContentParsers()
        ).flatMap(List::stream).toList();
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
                .setSettings(getIndexSettings())
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setPriorSystemIndexDescriptors(List.of(inferenceIndexV1Descriptor))
                .build(),
            SystemIndexDescriptor.builder()
                .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                .setIndexPattern(InferenceSecretsIndex.INDEX_PATTERN)
                .setPrimaryIndex(InferenceSecretsIndex.INDEX_NAME)
                .setDescription("Contains inference service secrets")
                .setMappings(InferenceSecretsIndex.mappings())
                .setSettings(getSecretsIndexSettings())
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setNetNew()
                .build(),
            SystemIndexDescriptor.builder()
                .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                .setIndexPattern(CCMIndex.INDEX_PATTERN)
                .setPrimaryIndex(CCMIndex.INDEX_NAME)
                .setDescription("Contains Elastic Inference Service Cloud Connected Mode settings")
                .setMappings(CCMIndex.mappings())
                .setSettings(CCMIndex.settings())
                .setOrigin(ClientHelper.INFERENCE_ORIGIN)
                .setNetNew()
                .build()
        );
    }

    // Overridable for tests
    protected Settings getIndexSettings() {
        return InferenceIndex.settings();
    }

    // Overridable for tests
    protected Settings getSecretsIndexSettings() {
        return InferenceSecretsIndex.settings();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settingsToUse) {
        return List.of(inferenceUtilityExecutor(), inferenceResponseExecutor());
    }

    private static ExecutorBuilder<?> inferenceUtilityExecutor() {
        return new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            0,
            10,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.inference.utility_thread_pool"
        );
    }

    private static ExecutorBuilder<?> inferenceResponseExecutor() {
        return new ScalingExecutorBuilder(
            INFERENCE_RESPONSE_THREAD_POOL_NAME,
            0,
            10,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.inference.inference_response_thread_pool"
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.copyOf(getInferenceSettings());
    }

    public static Set<Setting<?>> getInferenceSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(HttpSettings.getSettingsDefinitions());
        settings.addAll(HttpClientManager.getSettingsDefinitions());
        settings.addAll(ThrottlerManager.getSettingsDefinitions());
        settings.addAll(RetrySettings.getSettingsDefinitions());
        settings.addAll(Truncator.getSettingsDefinitions());
        settings.addAll(RequestExecutorServiceSettings.getSettingsDefinitions());
        settings.add(SKIP_VALIDATE_AND_START);
        settings.add(INDICES_INFERENCE_BATCH_SIZE);
        settings.add(INFERENCE_QUERY_TIMEOUT);
        settings.addAll(InferenceEndpointRegistry.getSettingsDefinitions());
        settings.addAll(ElasticInferenceServiceSettings.getSettingsDefinitions());
        settings.addAll(CCMSettings.getSettingsDefinitions());
        settings.addAll(CCMCache.getSettingsDefinitions());
        return Collections.unmodifiableSet(settings);
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
        return modelRegistry::get;
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
        return Set.of(new RestHeaderDefinition(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, true));
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
