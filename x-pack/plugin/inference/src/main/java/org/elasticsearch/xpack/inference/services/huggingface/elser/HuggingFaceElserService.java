/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceBaseService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModelParameters;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedUnifiedCompletionOperation;
import static org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings.URL;

public class HuggingFaceElserService extends HuggingFaceBaseService {
    public static final String NAME = "hugging_face_elser";

    private static final String SERVICE_NAME = "Hugging Face ELSER";
    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.SPARSE_EMBEDDING);

    public HuggingFaceElserService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected HuggingFaceModel createModel(HuggingFaceModelParameters input) {
        return switch (input.taskType()) {
            case SPARSE_EMBEDDING -> new HuggingFaceElserModel(
                input.inferenceEntityId(),
                input.taskType(),
                NAME,
                input.serviceSettings(),
                input.secretSettings(),
                input.context()
            );
            default -> throw new ElasticsearchStatusException(input.failureMessage(), RestStatus.BAD_REQUEST);
        };
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throwUnsupportedUnifiedCompletionOperation(NAME);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        EmbeddingsInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        ActionListener<InferenceServiceResults> inferListener = listener.delegateFailureAndWrap(
            (delegate, response) -> delegate.onResponse(translateToChunkedResults(inputs, response))
        );

        // TODO chunking sparse embeddings not implemented
        doInfer(model, inputs, taskSettings, timeout, inferListener);
    }

    private static List<ChunkedInference> translateToChunkedResults(EmbeddingsInput inputs, InferenceServiceResults inferenceResults) {
        if (inferenceResults instanceof TextEmbeddingFloatResults textEmbeddingResults) {
            validateInputSizeAgainstEmbeddings(ChunkInferenceInput.inputs(inputs.getInputs()), textEmbeddingResults.embeddings().size());

            var results = new ArrayList<ChunkedInference>(inputs.getInputs().size());

            for (int i = 0; i < inputs.getInputs().size(); i++) {
                results.add(
                    new ChunkedInferenceEmbedding(
                        List.of(
                            new EmbeddingResults.Chunk(
                                textEmbeddingResults.embeddings().get(i),
                                new ChunkedInference.TextOffset(0, inputs.getInputs().get(i).input().length())
                            )
                        )
                    )
                );
            }
            return results;
        } else if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            var inputsAsList = ChunkInferenceInput.inputs(EmbeddingsInput.of(inputs).getInputs());
            return ChunkedInferenceEmbedding.listOf(inputsAsList, sparseEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ChunkedInferenceError(error.getException()));
        } else {
            String expectedClasses = Strings.format(
                "One of [%s,%s]",
                TextEmbeddingFloatResults.class.getSimpleName(),
                SparseEmbeddingResults.class.getSimpleName()
            );
            throw createInvalidChunkedResultException(expectedClasses, inferenceResults.getWriteableName());
        }
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public boolean hideFromConfigurationApi() {
        return true;
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return supportedTaskTypes;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(supportedTaskTypes).setDescription("The URL endpoint to use for the requests.")
                        .setLabel("URL")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(supportedTaskTypes));
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration(supportedTaskTypes));

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(supportedTaskTypes)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
