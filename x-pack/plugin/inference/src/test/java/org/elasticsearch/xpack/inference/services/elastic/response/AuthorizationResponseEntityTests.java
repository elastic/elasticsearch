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
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class AuthorizationResponseEntityTests extends AbstractBWCWireSerializationTestCase<AuthorizationResponseEntity> {

    // rainbow-sprinkles
    public static final String RAINBOW_SPRINKLES_ENDPOINT_ID_V1 = ".rainbow-sprinkles-elastic";
    public static final String RAINBOW_SPRINKLES_MODEL_NAME = "rainbow-sprinkles";

    // elser-2
    public static final String ELSER_V2_ENDPOINT_ID = ".elser-2-elastic";
    public static final String ELSER_V2_MODEL_NAME = "elser_model_2";

    // multilingual-text-embed
    public static final String JINA_EMBED_ENDPOINT_ID = ".jina-embeddings-v3";
    public static final String JINA_EMBED_MODEL_NAME = "jina-embeddings-v3";

    // rerank-v1
    public static final String RERANK_V1_ENDPOINT_ID = ".elastic-rerank-v1";
    public static final String RERANK_V1_MODEL_NAME = "elastic-rerank-v2";

    public record EisAuthorizationResponse(
        String responseJson,
        AuthorizationResponseEntity responseEntity,
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
              "task_type": "chat_completion",
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "end_of_life_date": "2025-12-31"
            }
          ]
        }
        """;

    public static final String EIS_JINA_EMBED_RESPONSE = """
        {
          "inference_endpoints": [
            {
              "id": ".jina-embeddings-v3",
              "model_name": "jina-embeddings-v3",
              "task_type": "text_embedding",
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
              }
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
              "task_type": "sparse_embedding",
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
              }
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
              "task_type": "chat_completion",
              "status": "ga",
              "properties": [
                "multilingual"
              ],
              "release_date": "2024-05-01",
              "end_of_life_date": "2025-12-31"
            },
            {
              "id": ".elser-2-elastic",
              "model_name": "elser_model_2",
              "task_type": "sparse_embedding",
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
              }
            },
            {
              "id": ".jina-embeddings-v3",
              "model_name": "jina-embeddings-v3",
              "task_type": "text_embedding",
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
              }
            },
            {
              "id": ".elastic-rerank-v1",
              "model_name": "elastic-rerank-v1",
              "task_type": "rerank",
              "status": "preview",
              "properties": [],
              "release_date": "2024-05-01"
            }
          ]
        }
        """;

    public static EisAuthorizationResponse getEisElserAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createElserAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream().map(AuthorizationResponseEntity.AuthorizedEndpoint::id).collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_ELSER_RESPONSE,
            new AuthorizationResponseEntity(authorizedEndpoints),
            List.of(createElserExpectedEndpoint(url)),
            inferenceIds
        );
    }

    private static ElasticInferenceServiceModel createElserExpectedEndpoint(String url) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            ELSER_V2_ENDPOINT_ID,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(ELSER_V2_MODEL_NAME, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            new SentenceBoundaryChunkingSettings(250, 1)
        );
    }

    private static AuthorizationResponseEntity.AuthorizedEndpoint createElserAuthorizedEndpoint() {
        return new AuthorizationResponseEntity.AuthorizedEndpoint(
            ELSER_V2_ENDPOINT_ID,
            ELSER_V2_MODEL_NAME,
            "sparse_embedding",
            "preview",
            List.of("english"),
            "2024-05-01",
            null,
            new AuthorizationResponseEntity.Configuration(
                null,
                null,
                null,
                Map.of("strategy", "sentence", "max_chunk_size", 250, "sentence_overlap", 1)
            )
        );
    }

    public static EisAuthorizationResponse getEisAuthorizationResponseWithMultipleEndpoints(String url) {
        var authorizedEndpoints = List.of(
            createRainbowSprinklesAuthorizedEndpoint(),
            createElserAuthorizedEndpoint(),
            createJinaEmbedAuthorizedEndpoint(),
            new AuthorizationResponseEntity.AuthorizedEndpoint(
                RERANK_V1_ENDPOINT_ID,
                RERANK_V1_MODEL_NAME,
                "rerank",
                "preview",
                List.of(),
                "2024-05-01",
                null,
                null
            )
        );

        var inferenceIds = authorizedEndpoints.stream().map(AuthorizationResponseEntity.AuthorizedEndpoint::id).collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_AUTHORIZATION_RESPONSE_V2,
            new AuthorizationResponseEntity(authorizedEndpoints),
            List.of(
                createRainbowSprinklesExpectedEndpoint(url),
                createElserExpectedEndpoint(url),
                createJinaExpectedEndpoint(url),
                new ElasticInferenceServiceRerankModel(
                    RERANK_V1_ENDPOINT_ID,
                    TaskType.RERANK,
                    ElasticInferenceService.NAME,
                    new ElasticInferenceServiceRerankServiceSettings(RERANK_V1_MODEL_NAME),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    new ElasticInferenceServiceComponents(url)
                )
            ),
            inferenceIds
        );
    }

    private static AuthorizationResponseEntity.AuthorizedEndpoint createRainbowSprinklesAuthorizedEndpoint() {
        return new AuthorizationResponseEntity.AuthorizedEndpoint(
            RAINBOW_SPRINKLES_ENDPOINT_ID_V1,
            RAINBOW_SPRINKLES_MODEL_NAME,
            "chat_completion",
            "ga",
            List.of("multilingual"),
            "2024-05-01",
            "2025-12-31",
            null
        );
    }

    private static ElasticInferenceServiceModel createRainbowSprinklesExpectedEndpoint(String url) {
        return new ElasticInferenceServiceCompletionModel(
            RAINBOW_SPRINKLES_ENDPOINT_ID_V1,
            TaskType.CHAT_COMPLETION,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(RAINBOW_SPRINKLES_MODEL_NAME),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
        );
    }

    public static EisAuthorizationResponse getEisRainbowSprinklesAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createRainbowSprinklesAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream().map(AuthorizationResponseEntity.AuthorizedEndpoint::id).collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_RAINBOW_SPRINKLES_RESPONSE,
            new AuthorizationResponseEntity(authorizedEndpoints),
            List.of(createRainbowSprinklesExpectedEndpoint(url)),
            inferenceIds
        );
    }

    public static EisAuthorizationResponse getEisJinaEmbedAuthorizationResponse(String url) {
        var authorizedEndpoints = List.of(createJinaEmbedAuthorizedEndpoint());

        var inferenceIds = authorizedEndpoints.stream().map(AuthorizationResponseEntity.AuthorizedEndpoint::id).collect(Collectors.toSet());

        return new EisAuthorizationResponse(
            EIS_JINA_EMBED_RESPONSE,
            new AuthorizationResponseEntity(authorizedEndpoints),
            List.of(createJinaExpectedEndpoint(url)),
            inferenceIds
        );
    }

    private static AuthorizationResponseEntity.AuthorizedEndpoint createJinaEmbedAuthorizedEndpoint() {
        return new AuthorizationResponseEntity.AuthorizedEndpoint(
            JINA_EMBED_ENDPOINT_ID,
            JINA_EMBED_MODEL_NAME,
            "text_embedding",
            "beta",
            List.of("multilingual", "open-weights"),
            "2024-05-01",
            null,
            new AuthorizationResponseEntity.Configuration(
                "cosine",
                1024,
                "float",
                Map.of("strategy", "word", "max_chunk_size", 500, "overlap", 2)
            )
        );
    }

    private static ElasticInferenceServiceModel createJinaExpectedEndpoint(String url) {
        return new ElasticInferenceServiceDenseTextEmbeddingsModel(
            JINA_EMBED_ENDPOINT_ID,
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(JINA_EMBED_MODEL_NAME, SimilarityMeasure.COSINE, 1024, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            new WordBoundaryChunkingSettings(500, 2)
        );
    }

    public static AuthorizationResponseEntity createResponse() {
        return new AuthorizationResponseEntity(
            randomList(1, 5, () -> createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)))
        );
    }

    public static AuthorizationResponseEntity.AuthorizedEndpoint createAuthorizedEndpoint(TaskType taskType) {
        var id = randomAlphaOfLength(10);
        var name = randomAlphaOfLength(10);
        var status = randomFrom("ga", "beta", "preview");

        return switch (taskType) {
            case CHAT_COMPLETION -> new AuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                TaskType.CHAT_COMPLETION.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case SPARSE_EMBEDDING -> new AuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                TaskType.SPARSE_EMBEDDING.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case TEXT_EMBEDDING -> new AuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                TaskType.TEXT_EMBEDDING.toString(),
                status,
                null,
                "",
                "",
                new AuthorizationResponseEntity.Configuration(
                    randomFrom(SimilarityMeasure.values()).toString(),
                    randomInt(),
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    null
                )
            );
            case RERANK -> new AuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                TaskType.RERANK.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case COMPLETION -> new AuthorizationResponseEntity.AuthorizedEndpoint(
                id,
                name,
                TaskType.COMPLETION.toString(),
                status,
                null,
                "",
                "",
                null
            );
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    public void testParseAllFields() throws IOException {

        var url = "http://example.com/authorize";
        var responseData = getEisAuthorizationResponseWithMultipleEndpoints(url);
        try (var parser = createParser(JsonXContent.jsonXContent, responseData.responseJson())) {
            var entity = AuthorizationResponseEntity.PARSER.apply(parser, null);

            assertThat(entity, is(responseData.responseEntity()));

            var authModel = AuthorizationModel.of(responseData.responseEntity(), url);
            assertThat(authModel.getEndpointIds(), containsInAnyOrder(responseData.inferenceIds().toArray(String[]::new)));

            assertThat(
                authModel.getTaskTypes(),
                is(EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING, TaskType.RERANK))
            );
            assertThat(
                authModel.getEndpoints(responseData.inferenceIds()),
                containsInAnyOrder(responseData.expectedEndpoints().toArray(ElasticInferenceServiceModel[]::new))
            );
        }
    }

    @Override
    protected AuthorizationResponseEntity mutateInstanceForVersion(AuthorizationResponseEntity instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AuthorizationResponseEntity> instanceReader() {
        return AuthorizationResponseEntity::new;
    }

    @Override
    protected AuthorizationResponseEntity createTestInstance() {
        return createResponse();
    }

    @Override
    protected AuthorizationResponseEntity mutateInstance(AuthorizationResponseEntity instance) throws IOException {
        var newEndpoints = new ArrayList<>(instance.getAuthorizedEndpoints());
        newEndpoints.add(createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)));
        return new AuthorizationResponseEntity(newEndpoints);
    }
}
