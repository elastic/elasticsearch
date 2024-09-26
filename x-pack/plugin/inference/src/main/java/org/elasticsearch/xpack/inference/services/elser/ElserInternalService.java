/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elasticsearch.BaseElasticsearchInternalService;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.elser.ElserModels.ELSER_V2_MODEL;
import static org.elasticsearch.xpack.inference.services.elser.ElserModels.ELSER_V2_MODEL_LINUX_X86;

public class ElserInternalService extends BaseElasticsearchInternalService {

    public static final String NAME = "elser";

    private static final String OLD_MODEL_ID_FIELD_NAME = "model_version";

    public ElserInternalService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        super(context);
    }

    @Override
    protected EnumSet<TaskType> supportedTaskTypes() {
        return EnumSet.of(TaskType.SPARSE_EMBEDDING);
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> modelArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            var serviceSettingsBuilder = ElserInternalServiceSettings.fromRequestMap(serviceSettingsMap);

            if (serviceSettingsBuilder.getModelId() == null) {
                serviceSettingsBuilder.setModelId(
                    selectDefaultModelVariantBasedOnClusterArchitecture(modelArchitectures, ELSER_V2_MODEL_LINUX_X86, ELSER_V2_MODEL)
                );
            }

            Map<String, Object> taskSettingsMap;
            // task settings are optional
            if (config.containsKey(ModelConfigurations.TASK_SETTINGS)) {
                taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
            } else {
                taskSettingsMap = Map.of();
            }

            var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(
                new ElserInternalModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    new ElserInternalServiceSettings(serviceSettingsBuilder.build()),
                    taskSettings
                )
            );
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public ElserInternalModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfig(inferenceEntityId, taskType, config);
    }

    @Override
    public ElserInternalModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);

        // Change from old model_version field name to new model_id field name as of
        // TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED
        if (serviceSettingsMap.containsKey(OLD_MODEL_ID_FIELD_NAME)) {
            String modelId = ServiceUtils.removeAsType(serviceSettingsMap, OLD_MODEL_ID_FIELD_NAME, String.class);
            serviceSettingsMap.put(ElserInternalServiceSettings.MODEL_ID, modelId);
        }

        var serviceSettings = ElserInternalServiceSettings.fromPersistedMap(serviceSettingsMap);

        Map<String, Object> taskSettingsMap;
        // task settings are optional
        if (config.containsKey(ModelConfigurations.TASK_SETTINGS)) {
            taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        } else {
            taskSettingsMap = Map.of();
        }

        var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

        return new ElserInternalModel(inferenceEntityId, taskType, NAME, new ElserInternalServiceSettings(serviceSettings), taskSettings);
    }

    @Override
    public void infer(
        Model model,
        @Nullable String query,
        List<String> inputs,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        // No task settings to override with requestTaskSettings

        try {
            checkCompatibleTaskType(model.getConfigurations().getTaskType());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        var request = buildInferenceRequest(
            model.getConfigurations().getInferenceEntityId(),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            inputs,
            inputType,
            timeout,
            false // chunk
        );

        client.execute(
            InferModelAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap(
                (l, inferenceResult) -> l.onResponse(SparseEmbeddingResults.of(inferenceResult.getInferenceResults()))
            )
        );
    }

    public void chunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        @Nullable ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        chunkedInfer(model, null, input, taskSettings, inputType, chunkingOptions, timeout, listener);
    }

    @Override
    public void chunkedInfer(
        Model model,
        @Nullable String query,
        List<String> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        @Nullable ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        try {
            checkCompatibleTaskType(model.getConfigurations().getTaskType());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        var configUpdate = chunkingOptions != null
            ? new TokenizationConfigUpdate(chunkingOptions.windowSize(), chunkingOptions.span())
            : new TokenizationConfigUpdate(null, null);

        var request = buildInferenceRequest(
            model.getConfigurations().getInferenceEntityId(),
            configUpdate,
            inputs,
            inputType,
            timeout,
            true // chunk
        );

        client.execute(
            InferModelAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap(
                (l, inferenceResult) -> l.onResponse(translateChunkedResults(inferenceResult.getInferenceResults()))
            )
        );
    }

    private void checkCompatibleTaskType(TaskType taskType) {
        if (TaskType.SPARSE_EMBEDDING.isAnyOrSame(taskType) == false) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }
    }

    private static ElserMlNodeTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }

        // no config options yet
        return ElserMlNodeTaskSettings.DEFAULT;
    }

    private List<ChunkedInferenceServiceResults> translateChunkedResults(List<InferenceResults> inferenceResults) {
        var translated = new ArrayList<ChunkedInferenceServiceResults>();

        for (var inferenceResult : inferenceResults) {
            if (inferenceResult instanceof MlChunkedTextExpansionResults mlChunkedResult) {
                translated.add(InferenceChunkedSparseEmbeddingResults.ofMlResult(mlChunkedResult));
            } else if (inferenceResult instanceof ErrorInferenceResults error) {
                translated.add(new ErrorChunkedInferenceResults(error.getException()));
            } else {
                throw new ElasticsearchStatusException(
                    "Expected a chunked inference [{}] received [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    MlChunkedTextExpansionResults.NAME,
                    inferenceResult.getWriteableName()
                );
            }
        }
        return translated;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }
}
