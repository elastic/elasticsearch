/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Represents the preconfigured endpoints that are included in Elasticsearch. EIS will support dynamic preconfigured endpoints which means
 * it can provide new preconfigured endpoints that do not exist in the source here.
 */
public class InternalPreconfiguredEndpoints {

    // rainbow-sprinkles
    public static final String DEFAULT_CHAT_COMPLETION_MODEL_ID_V1 = "rainbow-sprinkles";
    public static final String DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1 = ".rainbow-sprinkles-elastic";

    // gp-llm-v2
    public static final String GP_LLM_V2_MODEL_ID = "gp-llm-v2";
    public static final String GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID = ".gp-llm-v2-chat_completion";

    // elser-2
    public static final String DEFAULT_ELSER_2_MODEL_ID = "elser_model_2";
    public static final String DEFAULT_ELSER_ENDPOINT_ID_V2 = ".elser-2-elastic";

    // multilingual-text-embed
    public static final Integer DENSE_TEXT_EMBEDDINGS_DIMENSIONS = 1024;
    public static final String DEFAULT_MULTILINGUAL_EMBED_MODEL_ID = "jina-embeddings-v3";
    public static final String DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID = ".jina-embeddings-v3";

    // rerank-v1
    public static final String DEFAULT_RERANK_MODEL_ID_V1 = "elastic-rerank-v1";
    public static final String DEFAULT_RERANK_ENDPOINT_ID_V1 = ".elastic-rerank-v1";

    public record MinimalModel(
        ModelConfigurations configurations,
        ElasticInferenceServiceRateLimitServiceSettings rateLimitServiceSettings
    ) {}

    private static final ElasticInferenceServiceCompletionServiceSettings COMPLETION_SERVICE_SETTINGS =
        new ElasticInferenceServiceCompletionServiceSettings(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);
    private static final ElasticInferenceServiceCompletionServiceSettings GP_LLM_V2_COMPLETION_SERVICE_SETTINGS =
        new ElasticInferenceServiceCompletionServiceSettings(GP_LLM_V2_MODEL_ID);
    private static final ElasticInferenceServiceSparseEmbeddingsServiceSettings SPARSE_EMBEDDINGS_SERVICE_SETTINGS =
        new ElasticInferenceServiceSparseEmbeddingsServiceSettings(DEFAULT_ELSER_2_MODEL_ID, null);
    private static final ElasticInferenceServiceDenseTextEmbeddingsServiceSettings DENSE_TEXT_EMBEDDINGS_SERVICE_SETTINGS =
        new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
            defaultDenseTextEmbeddingsSimilarity(),
            DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
            null
        );
    private static final ElasticInferenceServiceRerankServiceSettings RERANK_SERVICE_SETTINGS =
        new ElasticInferenceServiceRerankServiceSettings(DEFAULT_RERANK_MODEL_ID_V1);

    // A single model name can map to multiple inference endpoints, so we need a String to a List
    private static final Map<String, List<MinimalModel>> MODEL_NAME_TO_MINIMAL_MODELS = Map.of(
        DEFAULT_CHAT_COMPLETION_MODEL_ID_V1,
        List.of(
            new MinimalModel(
                new ModelConfigurations(
                    DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                    TaskType.CHAT_COMPLETION,
                    ElasticInferenceService.NAME,
                    COMPLETION_SERVICE_SETTINGS,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                COMPLETION_SERVICE_SETTINGS
            )
        ),
        GP_LLM_V2_MODEL_ID,
        List.of(
            new MinimalModel(
                new ModelConfigurations(
                    GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID,
                    TaskType.CHAT_COMPLETION,
                    ElasticInferenceService.NAME,
                    GP_LLM_V2_COMPLETION_SERVICE_SETTINGS,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                GP_LLM_V2_COMPLETION_SERVICE_SETTINGS
            )
        ),
        DEFAULT_ELSER_2_MODEL_ID,
        List.of(
            new MinimalModel(
                new ModelConfigurations(
                    DEFAULT_ELSER_ENDPOINT_ID_V2,
                    TaskType.SPARSE_EMBEDDING,
                    ElasticInferenceService.NAME,
                    SPARSE_EMBEDDINGS_SERVICE_SETTINGS,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                SPARSE_EMBEDDINGS_SERVICE_SETTINGS
            )
        ),
        DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
        List.of(
            new MinimalModel(
                new ModelConfigurations(
                    DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                    TaskType.TEXT_EMBEDDING,
                    ElasticInferenceService.NAME,
                    DENSE_TEXT_EMBEDDINGS_SERVICE_SETTINGS,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                DENSE_TEXT_EMBEDDINGS_SERVICE_SETTINGS
            )
        ),
        DEFAULT_RERANK_MODEL_ID_V1,
        List.of(
            new MinimalModel(
                new ModelConfigurations(
                    DEFAULT_RERANK_ENDPOINT_ID_V1,
                    TaskType.RERANK,
                    ElasticInferenceService.NAME,
                    RERANK_SERVICE_SETTINGS,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                RERANK_SERVICE_SETTINGS
            )
        )
    );

    private static final Map<String, MinimalModel> INFERENCE_ID_TO_MINIMAL_MODEL = MODEL_NAME_TO_MINIMAL_MODELS.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(toMap(m -> m.configurations().getInferenceEntityId(), Function.identity()));

    public static final Set<String> EIS_PRECONFIGURED_ENDPOINT_IDS = Set.copyOf(INFERENCE_ID_TO_MINIMAL_MODEL.keySet());

    public static SimilarityMeasure defaultDenseTextEmbeddingsSimilarity() {
        return SimilarityMeasure.COSINE;
    }

    public static List<MinimalModel> getWithModelName(String modelName) {
        var minimalModels = MODEL_NAME_TO_MINIMAL_MODELS.get(modelName);
        if (minimalModels == null) {
            return List.of();
        }

        return minimalModels;
    }

    public static MinimalModel getWithInferenceId(String inferenceId) {
        return INFERENCE_ID_TO_MINIMAL_MODEL.get(inferenceId);
    }

    private InternalPreconfiguredEndpoints() {}
}
