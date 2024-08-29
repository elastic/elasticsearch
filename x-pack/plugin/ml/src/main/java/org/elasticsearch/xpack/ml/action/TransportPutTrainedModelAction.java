/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlPlatformArchitecturesUtil;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.TaskRetriever;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.MODEL_ALREADY_EXISTS_ERROR_MESSAGE_FRAGMENT;

public class TransportPutTrainedModelAction extends TransportMasterNodeAction<Request, Response> {

    private static final ByteSizeValue MAX_NATIVE_DEFINITION_INDEX_SIZE = ByteSizeValue.ofGb(50);

    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;
    private final NamedXContentRegistry xContentRegistry;
    private final OriginSettingClient client;

    @Inject
    public TransportPutTrainedModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        TrainedModelProvider trainedModelProvider,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            PutTrainedModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.xContentRegistry = xContentRegistry;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutTrainedModelAction.Request request,
        ClusterState state,
        ActionListener<Response> finalResponseListener
    ) {
        TrainedModelConfig config = request.getTrainedModelConfig();
        try {
            if (request.isDeferDefinitionDecompression() == false) {
                config.ensureParsedDefinition(xContentRegistry);
            }
        } catch (IOException ex) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException("Failed to parse definition for [{}]", ex, config.getModelId())
            );
            return;
        }

        // NOTE: hasModelDefinition is false if we don't parse it. But, if the fully parsed model was already provided, continue
        boolean hasModelDefinition = config.getModelDefinition() != null;
        if (hasModelDefinition) {
            if (validateModelDefinition(config, state, licenseState, finalResponseListener) == false) {
                return;
            }
        }

        TrainedModelConfig.Builder trainedModelConfig = new TrainedModelConfig.Builder(config).setVersion(MlConfigVersion.CURRENT)
            .setCreateTime(Instant.now())
            .setCreatedBy("api_user")
            .setLicenseLevel(License.OperationMode.PLATINUM.description());
        AtomicReference<ModelPackageConfig> modelPackageConfigHolder = new AtomicReference<>();

        if (hasModelDefinition) {
            trainedModelConfig.setModelSize(config.getModelDefinition().ramBytesUsed())
                .setEstimatedOperations(config.getModelDefinition().getTrainedModel().estimatedNumOperations());
        } else {
            // Set default location for the given model type.
            trainedModelConfig.setLocation(
                Optional.ofNullable(config.getModelType()).orElse(TrainedModelType.TREE_ENSEMBLE).getDefaultLocation(config.getModelId())
            );
        }

        if (ModelAliasMetadata.fromState(state).getModelId(trainedModelConfig.getModelId()) != null) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException(
                    "requested model_id [{}] is the same as an existing model_alias. Model model_aliases and ids must be unique",
                    config.getModelId()
                )
            );
            return;
        }

        if (TrainedModelAssignmentMetadata.fromState(state).hasDeployment(trainedModelConfig.getModelId())) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException(
                    "Cannot create model [{}] " + MODEL_ALREADY_EXISTS_ERROR_MESSAGE_FRAGMENT,
                    config.getModelId()
                )
            );
            return;
        }

        var isPackageModel = config.isPackagedModel();
        ActionListener<Void> checkStorageIndexSizeListener = finalResponseListener.<Boolean>delegateFailureAndWrap((delegate, bool) -> {
            TrainedModelConfig configToReturn = trainedModelConfig.clearDefinition().build();
            if (modelPackageConfigHolder.get() != null) {
                triggerModelFetchIfNecessary(
                    configToReturn.getModelId(),
                    modelPackageConfigHolder.get(),
                    request.isWaitForCompletion(),
                    delegate.<TrainedModelConfig>delegateFailureAndWrap((l, cfg) -> l.onResponse(new Response(cfg)))
                        .<TrainedModelConfig>delegateFailureAndWrap(
                            (l, cfg) -> verifyMlNodesAndModelArchitectures(cfg, client, threadPool, l)
                        )
                        .delegateFailureAndWrap((l, downloadTriggered) -> l.onResponse(configToReturn))
                );
            } else {
                delegate.onResponse(new PutTrainedModelAction.Response(configToReturn));
            }
        }).delegateFailureAndWrap((l, r) -> trainedModelProvider.storeTrainedModel(trainedModelConfig.build(), l, isPackageModel));

        ActionListener<Void> tagsModelIdCheckListener = ActionListener.wrap(r -> {
            if (TrainedModelType.PYTORCH.equals(trainedModelConfig.getModelType())) {
                client.admin()
                    .indices()
                    .prepareStats(InferenceIndexConstants.nativeDefinitionStore())
                    .clear()
                    .setStore(true)
                    .execute(ActionListener.wrap(stats -> {
                        IndexStats indexStats = stats.getIndices().get(InferenceIndexConstants.nativeDefinitionStore());
                        if (indexStats != null
                            && indexStats.getTotal().getStore().sizeInBytes() > MAX_NATIVE_DEFINITION_INDEX_SIZE.getBytes()) {
                            finalResponseListener.onFailure(
                                new ElasticsearchStatusException(
                                    "Native model store has exceeded the maximum acceptable size of {}, "
                                        + "please delete older unused pytorch models",
                                    RestStatus.CONFLICT,
                                    MAX_NATIVE_DEFINITION_INDEX_SIZE.toString()
                                )
                            );
                            return;
                        }

                        checkStorageIndexSizeListener.onResponse(null);
                    }, e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                            checkStorageIndexSizeListener.onResponse(null);
                            return;
                        }
                        finalResponseListener.onFailure(
                            new ElasticsearchStatusException(
                                "Unable to calculate stats for definition storage index [{}], please try again later",
                                RestStatus.SERVICE_UNAVAILABLE,
                                e,
                                InferenceIndexConstants.nativeDefinitionStore()
                            )
                        );
                    }));
                return;
            }
            checkStorageIndexSizeListener.onResponse(null);
        }, finalResponseListener::onFailure);

        ActionListener<Void> modelIdTagCheckListener = ActionListener.wrap(
            r -> checkTagsAgainstModelIds(request.getTrainedModelConfig().getTags(), tagsModelIdCheckListener),
            finalResponseListener::onFailure
        );

        ActionListener<Void> handlePackageAndTagsListener = ActionListener.wrap(r -> {
            if (isPackageModel) {
                resolvePackageConfig(trainedModelConfig.getModelId(), ActionListener.wrap(resolvedModelPackageConfig -> {
                    try {
                        TrainedModelValidator.validatePackage(trainedModelConfig, resolvedModelPackageConfig, state);
                    } catch (ValidationException e) {
                        finalResponseListener.onFailure(e);
                        return;
                    }
                    modelPackageConfigHolder.set(resolvedModelPackageConfig);
                    setTrainedModelConfigFieldsFromPackagedModel(trainedModelConfig, resolvedModelPackageConfig, xContentRegistry);

                    checkModelIdAgainstTags(trainedModelConfig.getModelId(), modelIdTagCheckListener);
                }, finalResponseListener::onFailure));
            } else {
                checkModelIdAgainstTags(trainedModelConfig.getModelId(), modelIdTagCheckListener);
            }
        }, finalResponseListener::onFailure);

        checkForExistingModelDownloadTask(
            client,
            trainedModelConfig.getModelId(),
            request.isWaitForCompletion(),
            finalResponseListener,
            () -> handlePackageAndTagsListener.onResponse(null),
            request.ackTimeout()
        );
    }

    void verifyMlNodesAndModelArchitectures(
        TrainedModelConfig configToReturn,
        Client client,
        ThreadPool threadPool,
        ActionListener<TrainedModelConfig> configToReturnListener
    ) {
        ActionListener<TrainedModelConfig> addWarningHeaderOnFailureListener = new ActionListener<>() {
            @Override
            public void onResponse(TrainedModelConfig config) {
                assert Objects.equals(config, configToReturn);
                configToReturnListener.onResponse(configToReturn);
            }

            @Override
            public void onFailure(Exception e) {
                HeaderWarning.addWarning(e.getMessage());
                configToReturnListener.onResponse(configToReturn);
            }
        };

        callVerifyMlNodesAndModelArchitectures(configToReturn, addWarningHeaderOnFailureListener, client, threadPool);
    }

    void callVerifyMlNodesAndModelArchitectures(
        TrainedModelConfig configToReturn,
        ActionListener<TrainedModelConfig> failureListener,
        Client client,
        ThreadPool threadPool
    ) {
        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(
            failureListener,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            configToReturn
        );
    }

    /**
     * Check if the model is being downloaded.
     * If the download is in progress then the response will be on
     * the {@code isBeingDownloadedListener} otherwise {@code createModelAction}
     * is called to trigger the next step in the model install.
     * Should only be called for Elasticsearch hosted models.
     *
     * @param client Client
     * @param modelId Model Id
     * @param isWaitForCompletion Wait for the download to complete
     * @param isBeingDownloadedListener The listener called if the download is in progress
     * @param createModelAction If no download is in progress this is called to continue
     *                          the model install process.
     * @param timeout Model download timeout
     */
    static void checkForExistingModelDownloadTask(
        Client client,
        String modelId,
        boolean isWaitForCompletion,
        ActionListener<Response> isBeingDownloadedListener,
        Runnable createModelAction,
        TimeValue timeout
    ) {
        TaskRetriever.getDownloadTaskInfo(
            client,
            modelId,
            isWaitForCompletion,
            timeout,
            () -> "Timed out waiting for model download to complete",
            ActionListener.wrap(taskInfo -> {
                if (taskInfo != null) {
                    getModelInformation(client, modelId, isBeingDownloadedListener);
                } else {
                    // no task exists so proceed with creating the model
                    createModelAction.run();
                }
            }, isBeingDownloadedListener::onFailure)
        );
    }

    private static void getModelInformation(Client client, String modelId, ActionListener<Response> listener) {
        client.execute(GetTrainedModelsAction.INSTANCE, new GetTrainedModelsAction.Request(modelId), ActionListener.wrap(models -> {
            if (models.getResources().results().isEmpty()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "No model information found for a concurrent create model execution for model id [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        modelId
                    )
                );
            } else {
                listener.onResponse(new PutTrainedModelAction.Response(models.getResources().results().get(0)));
            }
        }, e -> {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Unable to retrieve model information for a concurrent create model execution for model id [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e,
                    modelId
                )
            );
        }));
    }

    private void triggerModelFetchIfNecessary(
        String modelId,
        ModelPackageConfig modelPackageConfig,
        boolean waitForCompletion,
        ActionListener<Void> listener
    ) {
        client.execute(
            LoadTrainedModelPackageAction.INSTANCE,
            new LoadTrainedModelPackageAction.Request(modelId, modelPackageConfig, waitForCompletion),
            ActionListener.wrap(ack -> listener.onResponse(null), listener::onFailure)
        );
    }

    private void resolvePackageConfig(String modelId, ActionListener<ModelPackageConfig> listener) {
        client.execute(
            GetTrainedModelPackageConfigAction.INSTANCE,
            new GetTrainedModelPackageConfigAction.Request(modelId.substring(1)),
            ActionListener.wrap(packageConfig -> listener.onResponse(packageConfig.getModelPackageConfig()), listener::onFailure)
        );
    }

    private void checkModelIdAgainstTags(String modelId, ActionListener<Void> listener) {
        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), modelId))
        );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                if (response.getHits().getTotalHits().value > 0) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_MODEL_ID_AND_TAGS_UNIQUE, modelId))
                    );
                    return;
                }
                listener.onResponse(null);
            }, listener::onFailure),
            client::search
        );
    }

    private void checkTagsAgainstModelIds(List<String> tags, ActionListener<Void> listener) {
        if (tags.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), tags))
        );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                if (response.getHits().getTotalHits().value > 0) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_TAGS_AND_MODEL_IDS_UNIQUE, tags))
                    );
                    return;
                }
                listener.onResponse(null);
            }, listener::onFailure),
            client::search
        );
    }

    public static boolean validateModelDefinition(
        TrainedModelConfig config,
        ClusterState state,
        XPackLicenseState licenseState,
        ActionListener<Response> finalResponseListener
    ) {
        try {
            config.getModelDefinition().getTrainedModel().validate();
        } catch (ElasticsearchException ex) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException("Definition for [{}] has validation failures.", ex, config.getModelId())
            );
            return false;
        }

        TrainedModelType trainedModelType = TrainedModelType.typeFromTrainedModel(config.getModelDefinition().getTrainedModel());
        if (trainedModelType == null) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException(
                    "Unknown trained model definition class [{}]",
                    config.getModelDefinition().getTrainedModel().getName()
                )
            );
            return false;
        }

        var configModelType = config.getModelType();
        if (configModelType == null) {
            // Set the model type from the definition
            config = new TrainedModelConfig.Builder(config).setModelType(trainedModelType).build();
        } else if (trainedModelType != configModelType) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException(
                    "{} [{}] does not match the model definition type [{}]",
                    TrainedModelConfig.MODEL_TYPE.getPreferredName(),
                    configModelType,
                    trainedModelType
                )
            );
            return false;
        }

        var inferenceConfig = config.getInferenceConfig();
        if (inferenceConfig.isTargetTypeSupported(config.getModelDefinition().getTrainedModel().targetType()) == false) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException(
                    "Model [{}] inference config type [{}] does not support definition target type [{}]",
                    config.getModelId(),
                    config.getInferenceConfig().getName(),
                    config.getModelDefinition().getTrainedModel().targetType()
                )
            );
            return false;
        }

        var minLicenseSupported = inferenceConfig.getMinLicenseSupportedForAction(RestRequest.Method.PUT);
        if (licenseState.isAllowedByLicense(minLicenseSupported) == false) {
            finalResponseListener.onFailure(
                new ElasticsearchSecurityException(
                    "Model of type [{}] requires [{}] license level",
                    RestStatus.FORBIDDEN,
                    config.getInferenceConfig().getName(),
                    minLicenseSupported
                )
            );
            return false;
        }

        TransportVersion minCompatibilityVersion = config.getModelDefinition().getTrainedModel().getMinimalCompatibilityVersion();
        if (state.getMinTransportVersion().before(minCompatibilityVersion)) {
            finalResponseListener.onFailure(
                ExceptionsHelper.badRequestException("Cannot create model [{}] while cluster upgrade is in progress.", config.getModelId())
            );
            return false;
        }

        return true;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }

    static void setTrainedModelConfigFieldsFromPackagedModel(
        TrainedModelConfig.Builder trainedModelConfig,
        ModelPackageConfig resolvedModelPackageConfig,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        trainedModelConfig.setDescription(resolvedModelPackageConfig.getDescription());
        trainedModelConfig.setModelType(TrainedModelType.fromString(resolvedModelPackageConfig.getModelType()));
        trainedModelConfig.setPlatformArchitecture(resolvedModelPackageConfig.getPlatformArchitecture());
        trainedModelConfig.setMetadata(resolvedModelPackageConfig.getMetadata());
        trainedModelConfig.setInferenceConfig(
            parseInferenceConfigFromModelPackage(resolvedModelPackageConfig.getInferenceConfigSource(), xContentRegistry)
        );
        trainedModelConfig.setTags(resolvedModelPackageConfig.getTags());
        trainedModelConfig.setPrefixStrings(resolvedModelPackageConfig.getPrefixStrings());
        trainedModelConfig.setModelPackageConfig(
            new ModelPackageConfig.Builder(resolvedModelPackageConfig).resetPackageOnlyFields().build()
        );

        trainedModelConfig.setLocation(trainedModelConfig.getModelType().getDefaultLocation(trainedModelConfig.getModelId()));
    }

    static InferenceConfig parseInferenceConfigFromModelPackage(Map<String, Object> source, NamedXContentRegistry namedXContentRegistry)
        throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
            XContentParser sourceParser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(namedXContentRegistry),
                BytesReference.bytes(xContentBuilder),
                XContentType.JSON
            )
        ) {

            XContentParser.Token token = sourceParser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            token = sourceParser.nextToken();
            assert token == XContentParser.Token.FIELD_NAME;
            String currentName = sourceParser.currentName();

            InferenceConfig inferenceConfig = sourceParser.namedObject(LenientlyParsedInferenceConfig.class, currentName, null);
            // consume the end object token
            token = sourceParser.nextToken();
            assert token == XContentParser.Token.END_OBJECT;
            return inferenceConfig;
        }
    }
}
