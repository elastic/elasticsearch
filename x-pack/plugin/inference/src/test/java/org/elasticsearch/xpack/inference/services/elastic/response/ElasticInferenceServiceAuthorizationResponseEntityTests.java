/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.metadata.EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED;
import static org.elasticsearch.xpack.inference.services.elastic.authorization.EndpointSchemaMigration.ENDPOINT_SCHEMA_VERSION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationResponseEntityTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceAuthorizationResponseEntity> {

    // rainbow-sprinkles
    public static final String RAINBOW_SPRINKLES_ENDPOINT_ID = ".rainbow-sprinkles-elastic";
    public static final String RAINBOW_SPRINKLES_MODEL_NAME = "rainbow-sprinkles";
    public static final String EIS_CHAT_PATH = "chat";

    // gp-llm-v2
    public static final String GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID = ".gp-llm-v2-chat_completion";
    public static final String GP_LLM_V2_COMPLETION_ENDPOINT_ID = ".gp-llm-v2-completion";
    public static final String GP_LLM_V2_MODEL_NAME = "gp-llm-v2";

    // elser-2
    public static final String ELSER_V2_ENDPOINT_ID = ".elser-2-elastic";
    public static final String ELSER_V2_MODEL_NAME = "elser_model_2";
    public static final String EIS_SPARSE_PATH = "embed/text/sparse";

    // multilingual-text-embed
    public static final String JINA_EMBED_V3_ENDPOINT_ID = ".jina-embeddings-v3";
    public static final String JINA_EMBED_V3_MODEL_NAME = "jina-embeddings-v3";
    public static final String EIS_TEXT_EMBED_PATH = "embed/text/dense";

    // multimodal embedding
    public static final String JINA_CLIP_V2_ENDPOINT_ID = ".jina-clip-v2";
    public static final String JINA_CLIP_V2_MODEL_NAME = "jina-clip-v2";
    public static final String EIS_MULTIMODAL_EMBED_PATH = "embed/dense";

    // rerank-v1
    public static final String RERANK_V1_ENDPOINT_ID = ".jina-reranker-v2";
    public static final String RERANK_V1_MODEL_NAME = "jina-reranker-v2";
    public static final String EIS_RERANK_PATH = "rerank/text/text-similarity";

    public record EisAuthorizationResponse(
        String responseJson,
        ElasticInferenceServiceAuthorizationResponseEntity responseEntity,
        List<ElasticInferenceServiceModel> expectedEndpoints,
        Set<String> inferenceIds
    ) {}

    public static final String EIS_EMPTY_RESPONSE = """
        {
          "inference_endpoints": []
        }
        """;

    public static final String EIS_RAINBOW_SPRINKLES_RESPONSE = """
        {
          "inference_endpoints": [
            {
              "id": ".rainbow-sprinkles-elastic",
              "model_name": "rainbow-sprinkles",
              "task_types": {
                "eis": "chat",
                "elasticsearch": "chat_completion"
              },
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "end_of_life_date": "2024-05-02",
              "display_name": "Rainbow Sprinkles Elastic",
              "fingerprint": "fingerprint123"
            }
          ]
        }
        """;

    public static final String EIS_JINA_TEXT_EMBED_RESPONSE = """
        {
          "inference_endpoints": [
            {
              "id": ".jina-embeddings-v3",
              "model_name": "jina-embeddings-v3",
              "task_types": {
                "eis": "embed/text/dense",
                "elasticsearch": "text_embedding"
              },
              "status": "beta",
              "properties": [
                "multilingual",
                "open-weights"
              ],
              "release_date": "2024-05-01",
              "configuration": {
                "similarity": "cosine",
                "dimensions": 1024,
                "element_type": "float",
                "chunking_settings": {
                  "strategy": "word",
                  "max_chunk_size": 500,
                  "overlap": 2
                }
              },
              "display_name": "Jina Embeddings V3",
              "fingerprint": "fingerprint456"
            }
          ]
        }
        """;

    public static final String EIS_ELSER_RESPONSE = """
        {
          "inference_endpoints": [
            {
              "id": ".elser-2-elastic",
              "model_name": "elser_model_2",
              "task_types": {
                "eis": "embed/text/sparse",
                "elasticsearch": "sparse_embedding"
              },
              "status": "preview",
              "properties": [
                "english"
              ],
              "release_date": "2024-05-01",
              "configuration": {
                "chunking_settings": {
                  "strategy": "sentence",
                  "max_chunk_size": 250,
                  "sentence_overlap": 1
                }
              },
              "display_name": "Elser 2 Elastic",
              "fingerprint": "fingerprint789"
            }
          ]
        }
        """;

    public static String EIS_AUTHORIZATION_RESPONSE_V2 = """
        {
          "inference_endpoints": [
            {
              "id": ".rainbow-sprinkles-elastic",
              "model_name": "rainbow-sprinkles",
              "task_types": {
                "eis": "chat",
                "elasticsearch": "chat_completion"
              },
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "end_of_life_date": "2024-05-02",
              "display_name": "Rainbow Sprinkles Elastic",
              "fingerprint": "fingerprint123"
            },
            {
              "id": ".gp-llm-v2-chat_completion",
              "model_name": "gp-llm-v2",
              "task_types": {
                "eis": "chat",
                "elasticsearch": "chat_completion"
              },
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "display_name": "Gp Llm V2 Chat Completion",
              "fingerprint": "fingerprint234"
            },
            {
              "id": ".gp-llm-v2-completion",
              "model_name": "gp-llm-v2",
              "task_types": {
                "eis": "chat",
                "elasticsearch": "completion"
              },
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "display_name": "Gp Llm V2 Completion",
              "fingerprint": "fingerprint345"
            },
            {
              "id": ".elser-2-elastic",
              "model_name": "elser_model_2",
              "task_types": {
                "eis": "embed/text/sparse",
                "elasticsearch": "sparse_embedding"
              },
              "status": "preview",
              "properties": [
                "english"
              ],
              "release_date": "2024-05-01",
              "configuration": {
                "chunking_settings": {
                  "strategy": "sentence",
                  "max_chunk_size": 250,
                  "sentence_overlap": 1
                }
              },
              "display_name": "Elser 2 Elastic",
              "fingerprint": "fingerprint789"
            },
            {
              "id": ".jina-embeddings-v3",
              "model_name": "jina-embeddings-v3",
              "task_types": {
                "eis": "embed/text/dense",
                "elasticsearch": "text_embedding"
              },
              "status": "beta",
              "properties": [
                "multilingual",
                "open-weights"
              ],
              "release_date": "2024-05-01",
              "configuration": {
                "similarity": "cosine",
                "dimensions": 1024,
                "element_type": "float",
                "chunking_settings": {
                  "strategy": "word",
                  "max_chunk_size": 500,
                  "overlap": 2
                }
              },
              "display_name": "Jina Embeddings V3",
              "fingerprint": "fingerprint456"
            },
            {
              "id": ".jina-clip-v2",
              "model_name": "jina-clip-v2",
              "task_types": {
                "eis": "embed/dense",
                "elasticsearch": "embedding"
              },
              "status": "beta",
              "properties": [
                "multilingual",
                "multimodal",
                "open-weights"
              ],
              "release_date": "2024-05-01",
              "configuration": {
                "similarity": "cosine",
                "dimensions": 1024,
                "element_type": "float",
                "chunking_settings": {
                  "strategy": "word",
                  "max_chunk_size": 500,
                  "overlap": 2
                }
              },
              "display_name": "Jina Clip V2",
              "fingerprint": "fingerprint_clip_v2"
            },
            {
              "id": ".jina-reranker-v2",
              "model_name": "jina-reranker-v2",
              "task_types": {
                "eis": "rerank/text/text-similarity",
                "elasticsearch": "rerank"
              },
              "status": "preview",
              "properties": [],
              "release_date": "2024-05-01",
              "display_name": "Jina Reranker V2",
              "fingerprint": "fingerprint567"
            }
          ]
        }
        """;

    private static final String RELEASE_DATE_STRING = "2024-05-01";
    private static final String END_OF_LIFE_DATE_STRING = "2024-05-02";
    private static final String RAINBOW_SPRINKLES_DISPLAY_NAME = "Rainbow Sprinkles Elastic";
    private static final String RAINBOW_SPRINKLES_FINGERPRINT = "fingerprint123";
    private static final String GP_LLM_V2_CHAT_COMPLETION_DISPLAY_NAME = "Gp Llm V2 Chat Completion";
    private static final String GP_LLM_V2_CHAT_COMPLETION_FINGERPRINT = "fingerprint234";
    private static final String GP_LLM_V2_COMPLETION_DISPLAY_NAME = "Gp Llm V2 Completion";
    private static final String GP_LLM_V2_COMPLETION_FINGERPRINT = "fingerprint345";
    private static final String ELSER_V2_DISPLAY_NAME = "Elser 2 Elastic";
    private static final String ELSER_V2_FINGERPRINT = "fingerprint789";
    private static final String JINA_EMBED_V3_DISPLAY_NAME = "Jina Embeddings V3";
    private static final String JINA_EMBED_V3_FINGERPRINT = "fingerprint456";
    private static final String JINA_CLIP_V2_DISPLAY_NAME = "Jina Clip V2";
    private static final String JINA_CLIP_V2_FINGERPRINT = "fingerprint_clip_v2";
    private static final String RERANK_V1_DISPLAY_NAME = "Jina Reranker V2";
    private static final String RERANK_V1_FINGERPRINT = "fingerprint567";
    private static final LocalDate RELEASE_DATE_PARSED = LocalDate.parse(RELEASE_DATE_STRING);
    private static final LocalDate END_OF_LIFE_DATE_PARSED = LocalDate.parse(END_OF_LIFE_DATE_STRING);

    public static EisAuthorizationResponse getEisElserAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createElserAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream()
            .map(ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint::id)
            .collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_ELSER_RESPONSE,
            new ElasticInferenceServiceAuthorizationResponseEntity(authorizedEndpoints),
            List.of(createElserExpectedEndpoint(url)),
            inferenceIds
        );
    }

    private static ElasticInferenceServiceModel createElserExpectedEndpoint(String url) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            ELSER_V2_ENDPOINT_ID,
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(ELSER_V2_MODEL_NAME, null, null),
            new ElasticInferenceServiceComponents(url),
            new SentenceBoundaryChunkingSettings(250, 1),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of("english"), StatusHeuristic.fromString("preview"), RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(ELSER_V2_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(ELSER_V2_DISPLAY_NAME)
            )
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createElserAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            ELSER_V2_ENDPOINT_ID,
            ELSER_V2_MODEL_NAME,
            createTaskTypeObject(EIS_SPARSE_PATH, "sparse_embedding"),
            "preview",
            List.of("english"),
            RELEASE_DATE_STRING,
            null,
            new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                null,
                null,
                null,
                Map.of("strategy", "sentence", "max_chunk_size", 250, "sentence_overlap", 1)
            ),
            ELSER_V2_DISPLAY_NAME,
            ELSER_V2_FINGERPRINT
        );
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity.TaskTypeObject createTaskTypeObject(
        String eisTaskType,
        String elasticsearchTaskType
    ) {
        return new ElasticInferenceServiceAuthorizationResponseEntity.TaskTypeObject(eisTaskType, elasticsearchTaskType);
    }

    public static EisAuthorizationResponse getEisAuthorizationResponseWithMultipleEndpoints(String url) {
        var authorizedEndpoints = List.of(
            createRainbowSprinklesAuthorizedEndpoint(),
            createGpLlmV2ChatCompletionAuthorizedEndpoint(),
            createGpLlmV2CompletionAuthorizedEndpoint(),
            createElserAuthorizedEndpoint(),
            createJinaTextEmbedAuthorizedEndpoint(),
            createJinaMultimodalEmbedAuthorizedEndpoint(),
            createRerankV1AuthorizedEndpoint()
        );

        var inferenceIds = authorizedEndpoints.stream()
            .map(ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint::id)
            .collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_AUTHORIZATION_RESPONSE_V2,
            new ElasticInferenceServiceAuthorizationResponseEntity(authorizedEndpoints),
            List.of(
                createRainbowSprinklesExpectedEndpoint(url),
                createGpLlmV2ChatCompletionExpectedEndpoint(url),
                createGpLlmV2CompletionExpectedEndpoint(url),
                createElserExpectedEndpoint(url),
                createJinaExpectedTextEmbeddingEndpoint(url),
                createJinaExpectedMultimodalEmbeddingEndpoint(url),
                createRerankV1ExpectedEndpoint(url)
            ),
            inferenceIds
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createRainbowSprinklesAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            RAINBOW_SPRINKLES_ENDPOINT_ID,
            RAINBOW_SPRINKLES_MODEL_NAME,
            createTaskTypeObject(EIS_CHAT_PATH, "chat_completion"),
            "ga",
            List.of("multilingual"),
            RELEASE_DATE_STRING,
            END_OF_LIFE_DATE_STRING,
            null,
            RAINBOW_SPRINKLES_DISPLAY_NAME,
            RAINBOW_SPRINKLES_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceModel createGpLlmV2ChatCompletionExpectedEndpoint(String url) {
        return new ElasticInferenceServiceCompletionModel(
            GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(GP_LLM_V2_MODEL_NAME),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of("multilingual"), StatusHeuristic.fromString("ga"), RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(GP_LLM_V2_CHAT_COMPLETION_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(GP_LLM_V2_CHAT_COMPLETION_DISPLAY_NAME)
            )
        );
    }

    private static ElasticInferenceServiceModel createGpLlmV2CompletionExpectedEndpoint(String url) {
        return new ElasticInferenceServiceCompletionModel(
            GP_LLM_V2_COMPLETION_ENDPOINT_ID,
            TaskType.COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(GP_LLM_V2_MODEL_NAME),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of("multilingual"), StatusHeuristic.fromString("ga"), RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(GP_LLM_V2_COMPLETION_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(GP_LLM_V2_COMPLETION_DISPLAY_NAME)
            )
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createGpLlmV2ChatCompletionAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID,
            GP_LLM_V2_MODEL_NAME,
            createTaskTypeObject(EIS_CHAT_PATH, "chat_completion"),
            "ga",
            List.of("multilingual"),
            RELEASE_DATE_STRING,
            null,
            null,
            GP_LLM_V2_CHAT_COMPLETION_DISPLAY_NAME,
            GP_LLM_V2_CHAT_COMPLETION_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createGpLlmV2CompletionAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            GP_LLM_V2_COMPLETION_ENDPOINT_ID,
            GP_LLM_V2_MODEL_NAME,
            createTaskTypeObject(EIS_CHAT_PATH, "completion"),
            "ga",
            List.of("multilingual"),
            RELEASE_DATE_STRING,
            null,
            null,
            GP_LLM_V2_COMPLETION_DISPLAY_NAME,
            GP_LLM_V2_COMPLETION_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceModel createRainbowSprinklesExpectedEndpoint(String url) {
        return new ElasticInferenceServiceCompletionModel(
            RAINBOW_SPRINKLES_ENDPOINT_ID,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(RAINBOW_SPRINKLES_MODEL_NAME),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of("multilingual"),
                    StatusHeuristic.fromString("ga"),
                    RELEASE_DATE_PARSED,
                    END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(RAINBOW_SPRINKLES_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(RAINBOW_SPRINKLES_DISPLAY_NAME)
            )
        );
    }

    public static EisAuthorizationResponse getEisRainbowSprinklesAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createRainbowSprinklesAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream()
            .map(ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint::id)
            .collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_RAINBOW_SPRINKLES_RESPONSE,
            new ElasticInferenceServiceAuthorizationResponseEntity(authorizedEndpoints),
            List.of(createRainbowSprinklesExpectedEndpoint(url)),
            inferenceIds
        );
    }

    public static EisAuthorizationResponse getEisJinaTextEmbedAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createJinaTextEmbedAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream()
            .map(ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint::id)
            .collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_JINA_TEXT_EMBED_RESPONSE,
            new ElasticInferenceServiceAuthorizationResponseEntity(authorizedEndpoints),
            List.of(createJinaExpectedTextEmbeddingEndpoint(url)),
            inferenceIds
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createJinaTextEmbedAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            JINA_EMBED_V3_ENDPOINT_ID,
            JINA_EMBED_V3_MODEL_NAME,
            createTaskTypeObject(EIS_TEXT_EMBED_PATH, "text_embedding"),
            "beta",
            List.of("multilingual", "open-weights"),
            RELEASE_DATE_STRING,
            null,
            new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                "cosine",
                1024,
                "float",
                Map.of("strategy", "word", "max_chunk_size", 500, "overlap", 2)
            ),
            JINA_EMBED_V3_DISPLAY_NAME,
            JINA_EMBED_V3_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceModel createJinaExpectedTextEmbeddingEndpoint(String url) {
        return new ElasticInferenceServiceDenseEmbeddingsModel(
            JINA_EMBED_V3_ENDPOINT_ID,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(JINA_EMBED_V3_MODEL_NAME, SimilarityMeasure.COSINE, 1024, null),
            new ElasticInferenceServiceComponents(url),
            new WordBoundaryChunkingSettings(500, 2),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of("multilingual", "open-weights"),
                    StatusHeuristic.fromString("beta"),
                    RELEASE_DATE_PARSED,
                    null
                ),
                new EndpointMetadata.Internal(JINA_EMBED_V3_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(JINA_EMBED_V3_DISPLAY_NAME)
            )
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createJinaMultimodalEmbedAuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            JINA_CLIP_V2_ENDPOINT_ID,
            JINA_CLIP_V2_MODEL_NAME,
            createTaskTypeObject(EIS_MULTIMODAL_EMBED_PATH, "embedding"),
            "beta",
            List.of("multilingual", "multimodal", "open-weights"),
            RELEASE_DATE_STRING,
            null,
            new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                "cosine",
                1024,
                "float",
                Map.of("strategy", "word", "max_chunk_size", 500, "overlap", 2)
            ),
            JINA_CLIP_V2_DISPLAY_NAME,
            JINA_CLIP_V2_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceModel createJinaExpectedMultimodalEmbeddingEndpoint(String url) {
        return new ElasticInferenceServiceDenseEmbeddingsModel(
            JINA_CLIP_V2_ENDPOINT_ID,
            TaskType.EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(JINA_CLIP_V2_MODEL_NAME, SimilarityMeasure.COSINE, 1024, null),
            new ElasticInferenceServiceComponents(url),
            new WordBoundaryChunkingSettings(500, 2),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of("multilingual", "multimodal", "open-weights"),
                    StatusHeuristic.fromString("beta"),
                    LocalDate.parse(RELEASE_DATE_STRING),
                    null
                ),
                new EndpointMetadata.Internal(JINA_CLIP_V2_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(JINA_CLIP_V2_DISPLAY_NAME)
            )
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createRerankV1AuthorizedEndpoint() {
        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            RERANK_V1_ENDPOINT_ID,
            RERANK_V1_MODEL_NAME,
            createTaskTypeObject(EIS_RERANK_PATH, "rerank"),
            "preview",
            List.of(),
            RELEASE_DATE_STRING,
            null,
            null,
            RERANK_V1_DISPLAY_NAME,
            RERANK_V1_FINGERPRINT
        );
    }

    private static ElasticInferenceServiceRerankModel createRerankV1ExpectedEndpoint(String url) {
        return new ElasticInferenceServiceRerankModel(
            RERANK_V1_ENDPOINT_ID,
            TaskType.RERANK,
            new ElasticInferenceServiceRerankServiceSettings(RERANK_V1_MODEL_NAME),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of(), StatusHeuristic.fromString("preview"), RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(RERANK_V1_FINGERPRINT, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(RERANK_V1_DISPLAY_NAME)
            )
        );
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity createResponse() {
        return new ElasticInferenceServiceAuthorizationResponseEntity(
            randomList(1, 5, () -> createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)))
        );
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createInvalidTaskTypeAuthorizedEndpoint() {
        var id = randomAlphaOfLength(10);
        var name = randomAlphaOfLength(10);
        var status = randomFrom("ga", "beta", "preview");
        var kibanaConnectorName = "Test Connector Name";
        var fingerprint = "fingerprint" + randomAlphaOfLength(5);

        return new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
            id,
            name,
            createTaskTypeObject("invalid/task/type", TaskType.ANY.toString()),
            status,
            null,
            "",
            "",
            null,
            kibanaConnectorName,
            fingerprint
        );
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint createAuthorizedEndpoint(TaskType taskType) {
        var id = randomAlphaOfLength(10);
        var name = randomAlphaOfLength(10);
        var status = randomFrom("ga", "beta", "preview");
        var fingerprintPrefix = "fingerprint_";

        return switch (taskType) {
            case CHAT_COMPLETION -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                null,
                null,
                "Chat Completion Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            case SPARSE_EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                null,
                null,
                "Sparse Embedding Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            case TEXT_EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                "",
                new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                    randomFrom(SimilarityMeasure.values()).toString(),
                    randomInt(),
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    null
                ),
                "Text Embedding Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            case EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_MULTIMODAL_EMBED_PATH, TaskType.EMBEDDING.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                null,
                new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                    randomFrom(SimilarityMeasure.values()).toString(),
                    randomInt(),
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    null
                ),
                "Embedding Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            case RERANK -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_RERANK_PATH, TaskType.RERANK.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                null,
                null,
                "Rerank Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            case COMPLETION -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                createTaskTypeObject(EIS_CHAT_PATH, TaskType.COMPLETION.toString()),
                status,
                null,
                RELEASE_DATE_STRING,
                END_OF_LIFE_DATE_STRING,
                null,
                "Completion Connector",
                fingerprintPrefix + randomAlphaOfLength(5)
            );
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    public void testParseAllFields() throws IOException {
        var url = "http://example.com/authorize";
        var responseData = getEisAuthorizationResponseWithMultipleEndpoints(url);
        try (var parser = createParser(JsonXContent.jsonXContent, responseData.responseJson())) {
            var entity = ElasticInferenceServiceAuthorizationResponseEntity.PARSER.apply(parser, null);

            assertThat(entity, is(responseData.responseEntity()));

            var authModel = ElasticInferenceServiceAuthorizationModel.of(responseData.responseEntity(), url);
            assertThat(authModel.getEndpointIds(), containsInAnyOrder(responseData.inferenceIds().toArray(String[]::new)));

            assertThat(
                authModel.getTaskTypes(),
                is(
                    EnumSet.of(
                        TaskType.CHAT_COMPLETION,
                        TaskType.SPARSE_EMBEDDING,
                        TaskType.TEXT_EMBEDDING,
                        TaskType.EMBEDDING,
                        TaskType.RERANK,
                        TaskType.COMPLETION
                    )
                )
            );
            assertThat(
                authModel.getEndpoints(responseData.inferenceIds()),
                containsInAnyOrder(responseData.expectedEndpoints().toArray(ElasticInferenceServiceModel[]::new))
            );
        }
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntity mutateInstanceForVersion(
        ElasticInferenceServiceAuthorizationResponseEntity instance,
        TransportVersion version
    ) {
        if (version.supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED)) {
            return instance;
        }

        return new ElasticInferenceServiceAuthorizationResponseEntity(
            instance.getAuthorizedEndpoints()
                .stream()
                .map(
                    endpoint -> new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                        endpoint.id(),
                        endpoint.modelName(),
                        endpoint.taskType(),
                        endpoint.status(),
                        endpoint.properties(),
                        endpoint.releaseDate(),
                        endpoint.endOfLifeDate(),
                        endpoint.configuration(),
                        null,
                        null
                    )
                )
                .toList()
        );
    }

    @Override
    protected Writeable.Reader<ElasticInferenceServiceAuthorizationResponseEntity> instanceReader() {
        return ElasticInferenceServiceAuthorizationResponseEntity::new;
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntity createTestInstance() {
        return createResponse();
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntity mutateInstance(ElasticInferenceServiceAuthorizationResponseEntity instance)
        throws IOException {
        var newEndpoints = new ArrayList<>(instance.getAuthorizedEndpoints());
        newEndpoints.add(createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)));
        return new ElasticInferenceServiceAuthorizationResponseEntity(newEndpoints);
    }
}
