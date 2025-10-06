/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public class ElasticInferenceServiceMinimalSettings {

    // rainbow-sprinkles
    static final String DEFAULT_CHAT_COMPLETION_MODEL_ID_V1 = "rainbow-sprinkles";
    static final String DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1 = defaultEndpointId(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);

    // elser-2
    static final String DEFAULT_ELSER_2_MODEL_ID = "elser_model_2";
    static final String DEFAULT_ELSER_ENDPOINT_ID_V2 = defaultEndpointId("elser-2");

    // multilingual-text-embed
    static final Integer DENSE_TEXT_EMBEDDINGS_DIMENSIONS = 1024;
    static final String DEFAULT_MULTILINGUAL_EMBED_MODEL_ID = "multilingual-embed-v1";
    static final String DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID = defaultEndpointId(DEFAULT_MULTILINGUAL_EMBED_MODEL_ID);

    // rerank-v1
    static final String DEFAULT_RERANK_MODEL_ID_V1 = "rerank-v1";
    static final String DEFAULT_RERANK_ENDPOINT_ID_V1 = defaultEndpointId(DEFAULT_RERANK_MODEL_ID_V1);

    public static final Set<String> EIS_PRECONFIGURED_ENDPOINTS = Set.of(
        DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
        DEFAULT_ELSER_ENDPOINT_ID_V2,
        DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
        DEFAULT_RERANK_ENDPOINT_ID_V1
    );

    static final MinimalServiceSettings CHAT_COMPLETION_V1_MINIMAL_SETTINGS = MinimalServiceSettings.chatCompletion(
        ElasticInferenceService.NAME
    );
    static final MinimalServiceSettings ELSER_V2_MINIMAL_SETTINGS = MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME);
    static final MinimalServiceSettings MULTILINGUAL_EMBED_MINIMAL_SETTINGS = MinimalServiceSettings.textEmbedding(
        ElasticInferenceService.NAME,
        DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
        defaultDenseTextEmbeddingsSimilarity(),
        DenseVectorFieldMapper.ElementType.FLOAT
    );
    static final MinimalServiceSettings RERANK_V1_MINIMAL_SETTINGS = MinimalServiceSettings.rerank(ElasticInferenceService.NAME);

    public record SettingsWithEndpointInfo(String inferenceId, String modelId, MinimalServiceSettings minimalSettings) {}

    private static final Map<String, SettingsWithEndpointInfo> MODEL_NAME_TO_MINIMAL_SETTINGS = Map.of(
        DEFAULT_CHAT_COMPLETION_MODEL_ID_V1,
        new SettingsWithEndpointInfo(
            DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
            DEFAULT_CHAT_COMPLETION_MODEL_ID_V1,
            CHAT_COMPLETION_V1_MINIMAL_SETTINGS
        ),
        DEFAULT_ELSER_2_MODEL_ID,
        new SettingsWithEndpointInfo(DEFAULT_ELSER_ENDPOINT_ID_V2, DEFAULT_ELSER_2_MODEL_ID, ELSER_V2_MINIMAL_SETTINGS),
        DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
        new SettingsWithEndpointInfo(
            DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
            DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
            MULTILINGUAL_EMBED_MINIMAL_SETTINGS
        ),
        DEFAULT_RERANK_MODEL_ID_V1,
        new SettingsWithEndpointInfo(DEFAULT_RERANK_ENDPOINT_ID_V1, DEFAULT_RERANK_MODEL_ID_V1, RERANK_V1_MINIMAL_SETTINGS)
    );

    private static final Map<String, SettingsWithEndpointInfo> INFERENCE_ID_TO_MINIMAL_SETTINGS = MODEL_NAME_TO_MINIMAL_SETTINGS.entrySet()
        .stream()
        .collect(toMap(e -> e.getValue().inferenceId(), Map.Entry::getValue));

    public static SimilarityMeasure defaultDenseTextEmbeddingsSimilarity() {
        return SimilarityMeasure.COSINE;
    }

    public static String defaultEndpointId(String modelId) {
        return Strings.format(".%s-elastic", modelId);
    }

    public static boolean isEisPreconfiguredEndpoint(String inferenceEntityId) {
        return EIS_PRECONFIGURED_ENDPOINTS.contains(inferenceEntityId);
    }

    public static boolean containsModelName(String modelName) {
        return MODEL_NAME_TO_MINIMAL_SETTINGS.containsKey(modelName);
    }

    public static SettingsWithEndpointInfo getWithModelName(String modelName) {
        return MODEL_NAME_TO_MINIMAL_SETTINGS.get(modelName);
    }

    public static SettingsWithEndpointInfo getWithInferenceId(String inferenceId) {
        return INFERENCE_ID_TO_MINIMAL_SETTINGS.get(inferenceId);
    }

    private ElasticInferenceServiceMinimalSettings() {}
}
