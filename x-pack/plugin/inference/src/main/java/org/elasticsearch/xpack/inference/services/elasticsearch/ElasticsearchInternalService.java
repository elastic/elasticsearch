/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigUpdate;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels.ELSER_V2_MODEL;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels.ELSER_V2_MODEL_LINUX_X86;

public class ElasticsearchInternalService extends BaseElasticsearchInternalService {

    public static final String NAME = "elasticsearch";
    public static final String OLD_ELSER_SERVICE_NAME = "elser";

    static final String MULTILINGUAL_E5_SMALL_MODEL_ID = ".multilingual-e5-small";
    static final String MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86 = ".multilingual-e5-small_linux-x86_64";
    public static final Set<String> MULTILINGUAL_E5_SMALL_VALID_IDS = Set.of(
        MULTILINGUAL_E5_SMALL_MODEL_ID,
        MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86
    );

    public static final int EMBEDDING_MAX_BATCH_SIZE = 10;
    public static final String DEFAULT_ELSER_ID = ".elser-2-elasticsearch";
    public static final String DEFAULT_E5_ID = ".multilingual-e5-small-elasticsearch";

    private static final Logger logger = LogManager.getLogger(ElasticsearchInternalService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(ElasticsearchInternalService.class);

    public ElasticsearchInternalService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        super(context);
    }

    // for testing
    ElasticsearchInternalService(
        InferenceServiceExtension.InferenceServiceFactoryContext context,
        Consumer<ActionListener<PreferredModelVariant>> platformArch
    ) {
        super(context, platformArch);
    }

    @Override
    protected EnumSet<TaskType> supportedTaskTypes() {
        return EnumSet.of(TaskType.RERANK, TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING);
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> modelListener
    ) {
        if (inferenceEntityId.equals(DEFAULT_ELSER_ID)) {
            modelListener.onFailure(
                new ElasticsearchStatusException(
                    "[{}] is a reserved inference Id. Cannot create a new inference endpoint with a reserved Id",
                    RestStatus.BAD_REQUEST,
                    inferenceEntityId
                )
            );
            return;
        }

        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
            String serviceName = (String) config.remove(ModelConfigurations.SERVICE); // required for elser service in elasticsearch service

            ChunkingSettings chunkingSettings;
            if (ChunkingSettingsFeatureFlag.isEnabled()
                && (TaskType.TEXT_EMBEDDING.equals(taskType) || TaskType.SPARSE_EMBEDDING.equals(taskType))) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            } else {
                chunkingSettings = null;
            }

            throwIfNotEmptyMap(config, name());

            String modelId = (String) serviceSettingsMap.get(ElasticsearchInternalServiceSettings.MODEL_ID);
            String deploymentId = (String) serviceSettingsMap.get(ElasticsearchInternalServiceSettings.DEPLOYMENT_ID);
            if (deploymentId != null) {
                validateAgainstDeployment(modelId, deploymentId, taskType, modelListener.delegateFailureAndWrap((l, settings) -> {
                    l.onResponse(new ElasticDeployedModel(inferenceEntityId, taskType, NAME, settings.build(), chunkingSettings));
                }));
            } else if (modelId == null) {
                if (OLD_ELSER_SERVICE_NAME.equals(serviceName)) {
                    // TODO complete deprecation of null model ID
                    // throw new ValidationException().addValidationError("Error parsing request config, model id is missing");
                    DEPRECATION_LOGGER.critical(
                        DeprecationCategory.API,
                        "inference_api_null_model_id_in_elasticsearch_service",
                        "Putting elasticsearch service inference endpoints (including elser service) without a model_id field is"
                            + " deprecated and will be removed in a future release. Please specify a model_id field."
                    );
                    preferredModelVariantFn.accept(
                        modelListener.delegateFailureAndWrap(
                            (delegate, preferredModelVariant) -> elserCase(
                                inferenceEntityId,
                                taskType,
                                config,
                                preferredModelVariant,
                                serviceSettingsMap,
                                true,
                                chunkingSettings,
                                modelListener
                            )
                        )
                    );
                } else {
                    throw new IllegalArgumentException("Error parsing service settings, model_id must be provided");
                }
            } else if (MULTILINGUAL_E5_SMALL_VALID_IDS.contains(modelId)) {
                preferredModelVariantFn.accept(
                    modelListener.delegateFailureAndWrap(
                        (delegate, preferredModelVariant) -> e5Case(
                            inferenceEntityId,
                            taskType,
                            config,
                            preferredModelVariant,
                            serviceSettingsMap,
                            chunkingSettings,
                            modelListener
                        )
                    )
                );
            } else if (ElserModels.isValidModel(modelId)) {
                preferredModelVariantFn.accept(
                    modelListener.delegateFailureAndWrap(
                        (delegate, preferredModelVariant) -> elserCase(
                            inferenceEntityId,
                            taskType,
                            config,
                            preferredModelVariant,
                            serviceSettingsMap,
                            OLD_ELSER_SERVICE_NAME.equals(serviceName),
                            chunkingSettings,
                            modelListener
                        )
                    )
                );
            } else {
                customElandCase(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, chunkingSettings, modelListener);
            }
        } catch (Exception e) {
            modelListener.onFailure(e);
        }
    }

    private void customElandCase(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettingsMap,
        Map<String, Object> taskSettingsMap,
        ChunkingSettings chunkingSettings,
        ActionListener<Model> modelListener
    ) {
        String modelId = (String) serviceSettingsMap.get(ElasticsearchInternalServiceSettings.MODEL_ID);
        var request = new GetTrainedModelsAction.Request(modelId);

        var getModelsListener = modelListener.<GetTrainedModelsAction.Response>delegateFailureAndWrap((delegate, response) -> {
            if (response.getResources().count() < 1) {
                throw new IllegalArgumentException(
                    "Error parsing request config, model id does not match any models available on this platform. Was ["
                        + modelId
                        + "]. You may need to load it into the cluster using eland."
                );
            } else {
                throwIfUnsupportedTaskType(modelId, taskType, response.getResources().results().get(0).getInferenceConfig());

                var model = createCustomElandModel(
                    inferenceEntityId,
                    taskType,
                    serviceSettingsMap,
                    taskSettingsMap,
                    chunkingSettings,
                    ConfigurationParseContext.REQUEST
                );

                throwIfNotEmptyMap(serviceSettingsMap, name());
                throwIfNotEmptyMap(taskSettingsMap, name());

                delegate.onResponse(model);
            }
        });

        client.execute(GetTrainedModelsAction.INSTANCE, request, getModelsListener);
    }

    private static CustomElandModel createCustomElandModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        ConfigurationParseContext context
    ) {

        return switch (taskType) {
            case TEXT_EMBEDDING -> new CustomElandEmbeddingModel(
                inferenceEntityId,
                taskType,
                NAME,
                CustomElandInternalTextEmbeddingServiceSettings.fromMap(serviceSettings, context),
                chunkingSettings
            );
            case SPARSE_EMBEDDING -> new CustomElandModel(
                inferenceEntityId,
                taskType,
                NAME,
                elandServiceSettings(serviceSettings, context),
                chunkingSettings
            );
            case RERANK -> new CustomElandRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                elandServiceSettings(serviceSettings, context),
                CustomElandRerankTaskSettings.fromMap(taskSettings)
            );
            default -> throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        };
    }

    private static CustomElandInternalServiceSettings elandServiceSettings(
        Map<String, Object> settingsMap,
        ConfigurationParseContext context
    ) {
        return switch (context) {
            case REQUEST -> new CustomElandInternalServiceSettings(
                ElasticsearchInternalServiceSettings.fromRequestMap(settingsMap).build()
            );
            case PERSISTENT -> new CustomElandInternalServiceSettings(ElasticsearchInternalServiceSettings.fromPersistedMap(settingsMap));
        };
    }

    private void e5Case(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        PreferredModelVariant preferredModelVariant,
        Map<String, Object> serviceSettingsMap,
        ChunkingSettings chunkingSettings,
        ActionListener<Model> modelListener
    ) {
        var esServiceSettingsBuilder = ElasticsearchInternalServiceSettings.fromRequestMap(serviceSettingsMap);

        if (esServiceSettingsBuilder.getModelId() == null) {
            esServiceSettingsBuilder.setModelId(
                selectDefaultModelVariantBasedOnClusterArchitecture(
                    preferredModelVariant,
                    MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86,
                    MULTILINGUAL_E5_SMALL_MODEL_ID
                )
            );
        } else if (modelVariantValidForArchitecture(preferredModelVariant, esServiceSettingsBuilder.getModelId()) == false) {
            throw new IllegalArgumentException(
                "Error parsing request config, model id does not match any models available on this platform. Was ["
                    + esServiceSettingsBuilder.getModelId()
                    + "]"
            );
        }

        throwIfNotEmptyMap(config, name());
        throwIfNotEmptyMap(serviceSettingsMap, name());

        modelListener.onResponse(
            new MultilingualE5SmallModel(
                inferenceEntityId,
                taskType,
                NAME,
                new MultilingualE5SmallInternalServiceSettings(esServiceSettingsBuilder.build()),
                chunkingSettings
            )
        );
    }

    static boolean modelVariantValidForArchitecture(PreferredModelVariant modelVariant, String modelId) {
        if (modelId.equals(MULTILINGUAL_E5_SMALL_MODEL_ID)) {
            // platform agnostic model is always compatible
            return true;
        }
        return modelId.equals(
            selectDefaultModelVariantBasedOnClusterArchitecture(
                modelVariant,
                MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86,
                MULTILINGUAL_E5_SMALL_MODEL_ID
            )
        );
    }

    private void elserCase(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        PreferredModelVariant preferredModelVariant,
        Map<String, Object> serviceSettingsMap,
        boolean isElserService,
        ChunkingSettings chunkingSettings,
        ActionListener<Model> modelListener
    ) {
        var esServiceSettingsBuilder = ElasticsearchInternalServiceSettings.fromRequestMap(serviceSettingsMap);
        final String defaultModelId = selectDefaultModelVariantBasedOnClusterArchitecture(
            preferredModelVariant,
            ELSER_V2_MODEL_LINUX_X86,
            ELSER_V2_MODEL
        );
        if (false == defaultModelId.equals(esServiceSettingsBuilder.getModelId())) {

            if (esServiceSettingsBuilder.getModelId() == null) {
                // TODO remove this case once we remove the option to not pass model ID
                esServiceSettingsBuilder.setModelId(defaultModelId);
            } else if (esServiceSettingsBuilder.getModelId().equals(ELSER_V2_MODEL)) {
                logger.warn(
                    "The platform agnostic model [{}] was requested on Linux x86_64. "
                        + "It is recommended to use the optimized model instead [{}]",
                    ELSER_V2_MODEL,
                    ELSER_V2_MODEL_LINUX_X86
                );
            } else {
                throw new IllegalArgumentException(
                    "Error parsing request config, model id does not match any models available on this platform. Was ["
                        + esServiceSettingsBuilder.getModelId()
                        + "]. You may need to use a platform agnostic model."
                );
            }
        }

        if (isElserService) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.API,
                "inference_api_elser_service",
                "The [{}] service is deprecated and will be removed in a future release. Use the [{}] service instead, with"
                    + " [model_id] set to [{}] in the [service_settings]",
                OLD_ELSER_SERVICE_NAME,
                ElasticsearchInternalService.NAME,
                defaultModelId
            );
        }

        throwIfNotEmptyMap(config, name());
        throwIfNotEmptyMap(serviceSettingsMap, name());

        modelListener.onResponse(
            new ElserInternalModel(
                inferenceEntityId,
                taskType,
                NAME,
                new ElserInternalServiceSettings(esServiceSettingsBuilder.build()),
                ElserMlNodeTaskSettings.DEFAULT,
                chunkingSettings
            )
        );
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfig(inferenceEntityId, taskType, config);
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (ChunkingSettingsFeatureFlag.isEnabled()
            && (TaskType.TEXT_EMBEDDING.equals(taskType) || TaskType.SPARSE_EMBEDDING.equals(taskType))) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        String modelId = (String) serviceSettingsMap.get(ElasticsearchInternalServiceSettings.MODEL_ID);
        if (modelId == null) {
            throw new IllegalArgumentException("Error parsing request config, model id is missing");
        }

        if (MULTILINGUAL_E5_SMALL_VALID_IDS.contains(modelId)) {
            return new MultilingualE5SmallModel(
                inferenceEntityId,
                taskType,
                NAME,
                new MultilingualE5SmallInternalServiceSettings(ElasticsearchInternalServiceSettings.fromPersistedMap(serviceSettingsMap)),
                chunkingSettings
            );
        } else if (ElserModels.isValidModel(modelId)) {
            return new ElserInternalModel(
                inferenceEntityId,
                taskType,
                NAME,
                new ElserInternalServiceSettings(ElasticsearchInternalServiceSettings.fromPersistedMap(serviceSettingsMap)),
                ElserMlNodeTaskSettings.DEFAULT,
                chunkingSettings
            );
        } else {
            return createCustomElandModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                ConfigurationParseContext.PERSISTENT
            );
        }
    }

    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof CustomElandEmbeddingModel elandModel && elandModel.getTaskType() == TaskType.TEXT_EMBEDDING) {
            // At this point the inference endpoint configuration has not been persisted yet, if we attempt to do inference using the
            // inference id we'll get an error because the trained model code needs to use the persisted inference endpoint to retrieve the
            // model id. To get around this we'll have the getEmbeddingSize() method use the model id instead of inference id. So we need
            // to create a temporary model that overrides the inference id with the model id.
            var temporaryModelWithModelId = new CustomElandEmbeddingModel(
                elandModel.getServiceSettings().modelId(),
                elandModel.getTaskType(),
                elandModel.getConfigurations().getService(),
                elandModel.getServiceSettings(),
                elandModel.getConfigurations().getChunkingSettings()
            );

            ServiceUtils.getEmbeddingSize(
                temporaryModelWithModelId,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(elandModel, size)))
            );
        } else {
            listener.onResponse(model);
        }
    }

    private static CustomElandEmbeddingModel updateModelWithEmbeddingDetails(CustomElandEmbeddingModel model, int embeddingSize) {
        CustomElandInternalTextEmbeddingServiceSettings serviceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
            model.getServiceSettings().getNumAllocations(),
            model.getServiceSettings().getNumThreads(),
            model.getServiceSettings().modelId(),
            model.getServiceSettings().getAdaptiveAllocationsSettings(),
            embeddingSize,
            model.getServiceSettings().similarity(),
            model.getServiceSettings().elementType()
        );

        return new CustomElandEmbeddingModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            serviceSettings,
            model.getConfigurations().getChunkingSettings()
        );
    }

    @Override
    public void infer(
        Model model,
        @Nullable String query,
        List<String> input,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ElasticsearchInternalModel esModel) {
            var taskType = model.getConfigurations().getTaskType();
            if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
                inferTextEmbedding(esModel, input, inputType, timeout, listener);
            } else if (TaskType.RERANK.equals(taskType)) {
                inferRerank(esModel, query, input, inputType, timeout, taskSettings, listener);
            } else if (TaskType.SPARSE_EMBEDDING.equals(taskType)) {
                inferSparseEmbedding(esModel, input, inputType, timeout, listener);
            } else {
                throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
            }
        } else {
            listener.onFailure(notElasticsearchModelException(model));
        }
    }

    public void inferTextEmbedding(
        ElasticsearchInternalModel model,
        List<String> inputs,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var request = buildInferenceRequest(
            model.mlNodeDeploymentId(),
            TextEmbeddingConfigUpdate.EMPTY_INSTANCE,
            inputs,
            inputType,
            timeout
        );

        ActionListener<InferModelAction.Response> mlResultsListener = listener.delegateFailureAndWrap(
            (l, inferenceResult) -> l.onResponse(InferenceTextEmbeddingFloatResults.of(inferenceResult.getInferenceResults()))
        );

        var maybeDeployListener = mlResultsListener.delegateResponse(
            (l, exception) -> maybeStartDeployment(model, exception, request, mlResultsListener)
        );

        client.execute(InferModelAction.INSTANCE, request, maybeDeployListener);
    }

    public void inferSparseEmbedding(
        ElasticsearchInternalModel model,
        List<String> inputs,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var request = buildInferenceRequest(model.mlNodeDeploymentId(), TextExpansionConfigUpdate.EMPTY_UPDATE, inputs, inputType, timeout);

        ActionListener<InferModelAction.Response> mlResultsListener = listener.delegateFailureAndWrap(
            (l, inferenceResult) -> l.onResponse(SparseEmbeddingResults.of(inferenceResult.getInferenceResults()))
        );

        var maybeDeployListener = mlResultsListener.delegateResponse(
            (l, exception) -> maybeStartDeployment(model, exception, request, mlResultsListener)
        );

        client.execute(InferModelAction.INSTANCE, request, maybeDeployListener);
    }

    public void inferRerank(
        ElasticsearchInternalModel model,
        String query,
        List<String> inputs,
        InputType inputType,
        TimeValue timeout,
        Map<String, Object> requestTaskSettings,
        ActionListener<InferenceServiceResults> listener
    ) {
        var request = buildInferenceRequest(model.mlNodeDeploymentId(), new TextSimilarityConfigUpdate(query), inputs, inputType, timeout);

        var modelSettings = (CustomElandRerankTaskSettings) model.getTaskSettings();
        var requestSettings = CustomElandRerankTaskSettings.fromMap(requestTaskSettings);
        Boolean returnDocs = CustomElandRerankTaskSettings.of(modelSettings, requestSettings).returnDocuments();

        Function<Integer, String> inputSupplier = returnDocs == Boolean.TRUE ? inputs::get : i -> null;

        client.execute(
            InferModelAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap(
                (l, inferenceResult) -> l.onResponse(
                    textSimilarityResultsToRankedDocs(inferenceResult.getInferenceResults(), inputSupplier)
                )
            )
        );
    }

    public void chunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        chunkedInfer(model, null, input, taskSettings, inputType, chunkingOptions, timeout, listener);
    }

    @Override
    public void chunkedInfer(
        Model model,
        @Nullable String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        if ((TaskType.TEXT_EMBEDDING.equals(model.getTaskType()) || TaskType.SPARSE_EMBEDDING.equals(model.getTaskType())) == false) {
            listener.onFailure(
                new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(model.getTaskType(), NAME), RestStatus.BAD_REQUEST)
            );
            return;
        }

        if (model instanceof ElasticsearchInternalModel esModel) {

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests;
            if (ChunkingSettingsFeatureFlag.isEnabled()) {
                batchedRequests = new EmbeddingRequestChunker(
                    input,
                    EMBEDDING_MAX_BATCH_SIZE,
                    embeddingTypeFromTaskTypeAndSettings(model.getTaskType(), esModel.internalServiceSettings),
                    esModel.getConfigurations().getChunkingSettings()
                ).batchRequestsWithListeners(listener);
            } else {
                batchedRequests = new EmbeddingRequestChunker(
                    input,
                    EMBEDDING_MAX_BATCH_SIZE,
                    embeddingTypeFromTaskTypeAndSettings(model.getTaskType(), esModel.internalServiceSettings)
                ).batchRequestsWithListeners(listener);
            }

            for (var batch : batchedRequests) {
                var inferenceRequest = buildInferenceRequest(
                    esModel.mlNodeDeploymentId(),
                    EmptyConfigUpdate.INSTANCE,
                    batch.batch().inputs(),
                    inputType,
                    timeout
                );

                ActionListener<InferModelAction.Response> mlResultsListener = batch.listener()
                    .delegateFailureAndWrap(
                        (l, inferenceResult) -> translateToChunkedResult(model.getTaskType(), inferenceResult.getInferenceResults(), l)
                    );

                var maybeDeployListener = mlResultsListener.delegateResponse(
                    (l, exception) -> maybeStartDeployment(esModel, exception, inferenceRequest, mlResultsListener)
                );

                client.execute(InferModelAction.INSTANCE, inferenceRequest, maybeDeployListener);
            }
        } else {
            listener.onFailure(notElasticsearchModelException(model));
        }
    }

    private static void translateToChunkedResult(
        TaskType taskType,
        List<InferenceResults> inferenceResults,
        ActionListener<InferenceServiceResults> chunkPartListener
    ) {
        if (taskType == TaskType.TEXT_EMBEDDING) {
            var translated = new ArrayList<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding>();

            for (var inferenceResult : inferenceResults) {
                if (inferenceResult instanceof MlTextEmbeddingResults mlTextEmbeddingResult) {
                    translated.add(
                        new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(mlTextEmbeddingResult.getInferenceAsFloat())
                    );
                } else if (inferenceResult instanceof ErrorInferenceResults error) {
                    chunkPartListener.onFailure(error.getException());
                    return;
                } else {
                    chunkPartListener.onFailure(
                        createInvalidChunkedResultException(MlTextEmbeddingResults.NAME, inferenceResult.getWriteableName())
                    );
                    return;
                }
            }
            chunkPartListener.onResponse(new InferenceTextEmbeddingFloatResults(translated));
        } else { // sparse
            var translated = new ArrayList<SparseEmbeddingResults.Embedding>();

            for (var inferenceResult : inferenceResults) {
                if (inferenceResult instanceof TextExpansionResults textExpansionResult) {
                    translated.add(
                        new SparseEmbeddingResults.Embedding(textExpansionResult.getWeightedTokens(), textExpansionResult.isTruncated())
                    );
                } else if (inferenceResult instanceof ErrorInferenceResults error) {
                    chunkPartListener.onFailure(error.getException());
                    return;
                } else {
                    chunkPartListener.onFailure(
                        createInvalidChunkedResultException(TextExpansionResults.NAME, inferenceResult.getWriteableName())
                    );
                    return;
                }
            }
            chunkPartListener.onResponse(new SparseEmbeddingResults(translated));
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public String name() {
        return NAME;
    }

    private RankedDocsResults textSimilarityResultsToRankedDocs(
        List<? extends InferenceResults> results,
        Function<Integer, String> inputSupplier
    ) {
        List<RankedDocsResults.RankedDoc> rankings = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            var result = results.get(i);
            if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.TextSimilarityInferenceResults similarity) {
                rankings.add(new RankedDocsResults.RankedDoc(i, (float) similarity.score(), inputSupplier.apply(i)));
            } else if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults errorResult) {
                if (errorResult.getException() instanceof ElasticsearchStatusException statusException) {
                    throw statusException;
                } else {
                    throw new ElasticsearchStatusException(
                        "Received error inference result.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        errorResult.getException()
                    );
                }
            } else {
                throw new IllegalArgumentException(
                    "Received invalid inference result, of type "
                        + result.getClass().getName()
                        + " but expected TextSimilarityInferenceResults."
                );
            }
        }

        Collections.sort(rankings);
        return new RankedDocsResults(rankings);
    }

    public List<DefaultConfigId> defaultConfigIds() {
        return List.of(
            new DefaultConfigId(DEFAULT_ELSER_ID, TaskType.SPARSE_EMBEDDING, this),
            new DefaultConfigId(DEFAULT_E5_ID, TaskType.TEXT_EMBEDDING, this)
        );
    }

    @Override
    public void updateModelsWithDynamicFields(List<Model> models, ActionListener<List<Model>> listener) {

        if (models.isEmpty()) {
            listener.onResponse(models);
            return;
        }

        var modelsByDeploymentIds = new HashMap<String, ElasticsearchInternalModel>();
        for (var model : models) {
            assert model instanceof ElasticsearchInternalModel;

            if (model instanceof ElasticsearchInternalModel esModel) {
                modelsByDeploymentIds.put(esModel.mlNodeDeploymentId(), esModel);
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot update model [{}] as it is not an Elasticsearch service model",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        model.getInferenceEntityId()
                    )
                );
                return;
            }
        }

        String deploymentIds = String.join(",", modelsByDeploymentIds.keySet());
        client.execute(
            GetDeploymentStatsAction.INSTANCE,
            new GetDeploymentStatsAction.Request(deploymentIds),
            ActionListener.wrap(stats -> {
                for (var deploymentStats : stats.getStats().results()) {
                    var model = modelsByDeploymentIds.get(deploymentStats.getDeploymentId());
                    model.updateNumAllocations(deploymentStats.getNumberOfAllocations());
                }
                listener.onResponse(new ArrayList<>(modelsByDeploymentIds.values()));
            }, e -> {
                logger.warn("Get deployment stats failed, cannot update the endpoint's number of allocations", e);
                // continue with the original response
                listener.onResponse(models);
            })
        );
    }

    public void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
        preferredModelVariantFn.accept(defaultsListener.delegateFailureAndWrap((delegate, preferredModelVariant) -> {
            if (PreferredModelVariant.LINUX_X86_OPTIMIZED.equals(preferredModelVariant)) {
                defaultsListener.onResponse(defaultConfigsLinuxOptimized());
            } else {
                defaultsListener.onResponse(defaultConfigsPlatfromAgnostic());
            }
        }));
    }

    private List<Model> defaultConfigsLinuxOptimized() {
        return defaultConfigs(true);
    }

    private List<Model> defaultConfigsPlatfromAgnostic() {
        return defaultConfigs(false);
    }

    private List<Model> defaultConfigs(boolean useLinuxOptimizedModel) {
        var defaultElser = new ElserInternalModel(
            DEFAULT_ELSER_ID,
            TaskType.SPARSE_EMBEDDING,
            NAME,
            new ElserInternalServiceSettings(
                null,
                1,
                useLinuxOptimizedModel ? ELSER_V2_MODEL_LINUX_X86 : ELSER_V2_MODEL,
                new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 8)
            ),
            ElserMlNodeTaskSettings.DEFAULT,
            null // default chunking settings
        );
        var defaultE5 = new MultilingualE5SmallModel(
            DEFAULT_E5_ID,
            TaskType.TEXT_EMBEDDING,
            NAME,
            new MultilingualE5SmallInternalServiceSettings(
                null,
                1,
                useLinuxOptimizedModel ? MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86 : MULTILINGUAL_E5_SMALL_MODEL_ID,
                new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 8)
            ),
            null // default chunking settings
        );
        return List.of(defaultElser, defaultE5);
    }

    @Override
    boolean isDefaultId(String inferenceId) {
        return DEFAULT_ELSER_ID.equals(inferenceId) || DEFAULT_E5_ID.equals(inferenceId);
    }

    static EmbeddingRequestChunker.EmbeddingType embeddingTypeFromTaskTypeAndSettings(
        TaskType taskType,
        ElasticsearchInternalServiceSettings serviceSettings
    ) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> EmbeddingRequestChunker.EmbeddingType.SPARSE;
            case TEXT_EMBEDDING -> serviceSettings.elementType() == null
                ? EmbeddingRequestChunker.EmbeddingType.FLOAT
                : EmbeddingRequestChunker.EmbeddingType.fromDenseVectorElementType(serviceSettings.elementType());
            default -> throw new ElasticsearchStatusException(
                "Chunking is not supported for task type [{}]",
                RestStatus.BAD_REQUEST,
                taskType
            );
        };
    }

    private void validateAgainstDeployment(
        String modelId,
        String deploymentId,
        TaskType taskType,
        ActionListener<ElasticsearchInternalServiceSettings.Builder> listener
    ) {
        getDeployment(deploymentId, listener.delegateFailureAndWrap((l, response) -> {
            if (response.isPresent()) {
                if (modelId != null && modelId.equals(response.get().getModelId()) == false) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Deployment [{}] uses model [{}] which does not match the model [{}] in the request.",
                            RestStatus.BAD_REQUEST, // TODO better message
                            deploymentId,
                            response.get().getModelId(),
                            modelId
                        )
                    );
                    return;
                }

                var updatedSettings = new ElasticsearchInternalServiceSettings.Builder().setNumAllocations(
                    response.get().getNumberOfAllocations()
                )
                    .setNumThreads(response.get().getThreadsPerAllocation())
                    .setAdaptiveAllocationsSettings(response.get().getAdaptiveAllocationsSettings())
                    .setDeploymentId(deploymentId)
                    .setModelId(response.get().getModelId());

                checkTaskTypeForMlNodeModel(response.get().getModelId(), taskType, l.delegateFailureAndWrap((l2, compatibleTaskType) -> {
                    l2.onResponse(updatedSettings);
                }));
            }
        }));
    }

    private void getDeployment(String deploymentId, ActionListener<Optional<AssignmentStats>> listener) {
        client.execute(
            GetTrainedModelsStatsAction.INSTANCE,
            new GetTrainedModelsStatsAction.Request(deploymentId),
            listener.delegateFailureAndWrap((l, response) -> {
                l.onResponse(
                    response.getResources()
                        .results()
                        .stream()
                        .filter(s -> s.getDeploymentStats() != null && s.getDeploymentStats().getDeploymentId().equals(deploymentId))
                        .map(GetTrainedModelsStatsAction.Response.TrainedModelStats::getDeploymentStats)
                        .findFirst()
                );
            })
        );
    }

    private void checkTaskTypeForMlNodeModel(String modelId, TaskType taskType, ActionListener<Boolean> listener) {
        client.execute(
            GetTrainedModelsAction.INSTANCE,
            new GetTrainedModelsAction.Request(modelId),
            listener.delegateFailureAndWrap((l, response) -> {
                if (response.getResources().results().isEmpty()) {
                    l.onFailure(new IllegalStateException("this shouldn't happen"));
                    return;
                }

                var inferenceConfig = response.getResources().results().get(0).getInferenceConfig();
                throwIfUnsupportedTaskType(modelId, taskType, inferenceConfig);
                l.onResponse(Boolean.TRUE);
            })
        );
    }

    static void throwIfUnsupportedTaskType(String modelId, TaskType taskType, InferenceConfig inferenceConfig) {
        var deploymentTaskType = inferenceConfigToTaskType(inferenceConfig);
        if (deploymentTaskType == null) {
            throw new ElasticsearchStatusException(
                "Deployed model [{}] has type [{}] which does not map to any supported task types",
                RestStatus.BAD_REQUEST,
                modelId,
                inferenceConfig.getWriteableName()
            );
        }
        if (deploymentTaskType != taskType) {
            throw new ElasticsearchStatusException(
                "Deployed model [{}] with type [{}] does not match the requested task type [{}]",
                RestStatus.BAD_REQUEST,
                modelId,
                inferenceConfig.getWriteableName(),
                taskType
            );
        }

    }

    static TaskType inferenceConfigToTaskType(InferenceConfig config) {
        if (config instanceof TextExpansionConfig) {
            return TaskType.SPARSE_EMBEDDING;
        } else if (config instanceof TextEmbeddingConfig) {
            return TaskType.TEXT_EMBEDDING;
        } else if (config instanceof TextSimilarityConfig) {
            return TaskType.RERANK;
        } else {
            return null;
        }
    }
}
