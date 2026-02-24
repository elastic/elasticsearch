/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.time.LocalDate;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.elastic.authorization.EndpointSchemaMigration.ENDPOINT_SCHEMA_VERSION;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_CHAT_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_MULTIMODAL_EMBED_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_SPARSE_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_TEXT_EMBED_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createTaskTypeObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class ElasticInferenceServiceAuthorizationModelTests extends ESTestCase {

    private static final String TEST_RELEASE_DATE = "2024-05-01";
    private static final String TEST_END_OF_LIFE_DATE = "2025-12-31";
    private static final LocalDate TEST_RELEASE_DATE_PARSED = LocalDate.parse(TEST_RELEASE_DATE);
    private static final LocalDate TEST_END_OF_LIFE_DATE_PARSED = LocalDate.parse(TEST_END_OF_LIFE_DATE);
    private static final String STATUS_GA = "ga";

    private static final EndpointMetadata DEFAULT_ENDPOINT_METADATA = new EndpointMetadata(
        new EndpointMetadata.Heuristics(
            List.of(),
            StatusHeuristic.fromString(STATUS_GA),
            TEST_RELEASE_DATE_PARSED,
            TEST_END_OF_LIFE_DATE_PARSED
        ),
        new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
        new EndpointMetadata.Display((String) null)
    );

    public void testIsAuthorized_ReturnsFalse_WithEmptyMap() {
        assertFalse(new ElasticInferenceServiceAuthorizationModel(List.of()).isAuthorized());
        {
            var emptyAuthUsingMethod = ElasticInferenceServiceAuthorizationModel.unauthorized();
            assertFalse(emptyAuthUsingMethod.isAuthorized());
            assertThat(emptyAuthUsingMethod.getEndpointIds(), empty());
            assertThat(emptyAuthUsingMethod, is(new ElasticInferenceServiceAuthorizationModel(List.of())));
        }
        {
            var emptyAuthUsingOf = ElasticInferenceServiceAuthorizationModel.of(
                new ElasticInferenceServiceAuthorizationResponseEntity(List.of()),
                "url"
            );
            assertFalse(emptyAuthUsingOf.isAuthorized());
            assertThat(emptyAuthUsingOf.getEndpointIds(), empty());
            assertThat(emptyAuthUsingOf, is(new ElasticInferenceServiceAuthorizationModel(List.of())));
        }
    }

    public void testExcludes_EndpointsWithoutValidTaskTypes() {
        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    "id",
                    "name",
                    createTaskTypeObject("", "invalid_task_type"),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    "id2",
                    "name",
                    createTaskTypeObject("", TaskType.ANY.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );
        var auth = ElasticInferenceServiceAuthorizationModel.of(response, "url");
        assertTrue(auth.getTaskTypes().isEmpty());
        assertFalse(auth.isAuthorized());
    }

    public void testReturnsAuthorizedTaskTypes() {
        var id1 = "id1";
        var id2 = "id2";

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name1",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    "name2",
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, "url");
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2)));
        assertTrue(auth.isAuthorized());
    }

    public void testIgnoresDuplicateId() {
        var id1 = "id1";
        var name1 = "name1";

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name2",
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, "url");
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertTrue(auth.isAuthorized());

        var url = "url";
        var chatCompletionEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            new ElasticInferenceServiceComponents(url),
            DEFAULT_ENDPOINT_METADATA
        );

        assertThat(auth.getEndpoints(Set.of(id1)), is(List.of(chatCompletionEndpoint)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertTrue(auth.isAuthorized());
    }

    public void testReturnsAuthorizedEndpoints() {
        var id1 = "id1";
        var id2 = "id2";

        var name1 = "name1";
        var name2 = "name2";

        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 123;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                )
            )
        );

        var url = "url";

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION, TaskType.TEXT_EMBEDDING)));
        assertTrue(auth.isAuthorized());

        var chatCompletionEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            new ElasticInferenceServiceComponents(url),
            DEFAULT_ENDPOINT_METADATA
        );
        var textEmbeddingEndpoint = new ElasticInferenceServiceDenseEmbeddingsModel(
            id2,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(name2, similarity, dimensions, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS,
            DEFAULT_ENDPOINT_METADATA
        );

        assertThat(auth.getEndpoints(Set.of(id1, id2)), containsInAnyOrder(chatCompletionEndpoint, textEmbeddingEndpoint));
        assertThat(auth.getEndpoints(Set.of(id2)), is(List.of(textEmbeddingEndpoint)));
        assertThat(auth.getEndpoints(Set.of()), is(List.of()));
    }

    public void testScopesToTaskType() {
        var id1 = "id1";
        var id2 = "id2";

        var name1 = "name1";
        var name2 = "name2";

        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 123;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                )
            )
        );

        var url = "url";

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION, TaskType.TEXT_EMBEDDING)));
        assertTrue(auth.isAuthorized());

        var scopedToChatCompletion = auth.newLimitedToTaskTypes(EnumSet.of(TaskType.CHAT_COMPLETION));
        assertThat(scopedToChatCompletion.getEndpointIds(), is(Set.of(id1)));
        assertThat(scopedToChatCompletion.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
        assertTrue(scopedToChatCompletion.isAuthorized());

        var chatCompletionEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            new ElasticInferenceServiceComponents(url),
            DEFAULT_ENDPOINT_METADATA
        );

        assertThat(auth.getEndpoints(Set.of(id1)), is(List.of(chatCompletionEndpoint)));

        var scopedToNone = auth.newLimitedToTaskTypes(EnumSet.noneOf(TaskType.class));
        assertThat(scopedToNone.getEndpointIds(), is(Set.of()));
        assertThat(scopedToNone.getTaskTypes(), is(Set.of()));
        assertFalse(scopedToNone.isAuthorized());
    }

    public void testReturnsAuthorizedEndpoints_FiltersInvalid() {
        var id1 = "id1";
        var invalidTextEmbedding1 = "invalid_text_embedding1";
        var invalidTextEmbedding2 = "invalid_text_embedding2";
        var invalidTextEmbedding3 = "invalid_text_embedding3";
        var invalidTextEmbedding4 = "invalid_text_embedding4";

        var name = "name1";

        var dimensions = 123;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                // Valid chat completion
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                // Missing similarity measure
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding1,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        null,
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                ),
                // Invalid chunking settings
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding2,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        Map.of("unexpected_field", "unexpected_value")
                    ),
                    null,
                    null
                ),
                // Invalid similarity measure
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding3,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        "invalid_similarity",
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                ),
                // Missing dimensions
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding4,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.COSINE.toString(),
                        null,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                ),
                // Missing element type
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding4,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.COSINE.toString(),
                        123,
                        null,
                        null
                    ),
                    null,
                    null
                )
            )
        );

        var url = "url";

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
        assertTrue(auth.isAuthorized());

        var chatCompletionEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            DEFAULT_ENDPOINT_METADATA
        );

        assertThat(
            auth.getEndpoints(Set.of(id1, invalidTextEmbedding1, invalidTextEmbedding2, invalidTextEmbedding3, invalidTextEmbedding4)),
            is(List.of(chatCompletionEndpoint))
        );
    }

    public void testReturnsAuthorizedEndpoints_FiltersUnsupportedElementType() {
        var id1 = "id1";
        var id2 = "id2";
        var id3 = "id3";
        var invalidTextEmbedding1 = "invalid_text_embedding1";
        var invalidTextEmbedding2 = "invalid_text_embedding2";
        var invalidTextEmbedding3 = "invalid_text_embedding3";
        var invalidTextEmbedding4 = "invalid_text_embedding4";

        var name = "name1";

        var dimensions = 123;
        var similarityMeasure = SimilarityMeasure.DOT_PRODUCT;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                // Valid
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        // Valid element type as it should be converted to lower case
                        "fLoaT",
                        null
                    ),
                    null,
                    null
                ),
                // Valid with element type all caps
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        // Valid element type as it should be converted to lower case
                        "FLOAT",
                        null
                    ),
                    null,
                    null
                ),
                // Valid with element type all lower case
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id3,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        "float",
                        null
                    ),
                    null,
                    null
                ),
                // Unsupported element type byte
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding1,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.BYTE.toString(),
                        null
                    ),
                    null,
                    null
                ),
                // Unsupported element type
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding2,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        "invalid-element-type",
                        null
                    ),
                    null,
                    null
                )
            )
        );

        var url = "url";

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2, id3)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.TEXT_EMBEDDING)));
        assertTrue(auth.isAuthorized());

        var textEmbeddingsModel1 = new ElasticInferenceServiceDenseEmbeddingsModel(
            id1,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS,
            DEFAULT_ENDPOINT_METADATA
        );

        var textEmbeddingsModel2 = new ElasticInferenceServiceDenseEmbeddingsModel(
            id2,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS,
            DEFAULT_ENDPOINT_METADATA
        );

        var textEmbeddingsModel3 = new ElasticInferenceServiceDenseEmbeddingsModel(
            id3,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS,
            DEFAULT_ENDPOINT_METADATA
        );

        assertThat(
            auth.getEndpoints(
                Set.of(id1, id2, id3, invalidTextEmbedding1, invalidTextEmbedding2, invalidTextEmbedding3, invalidTextEmbedding4)
            ),
            containsInAnyOrder(textEmbeddingsModel1, textEmbeddingsModel2, textEmbeddingsModel3)
        );
    }

    public void testCreatesAllSupportedTaskTypesAndReturnsCorrectModels() {
        var idCompletion = "id_completion";
        var idChat = "id_chat";
        var idSparse = "id_sparse";
        var idDenseMultimodal = "id_dense_multimodal";
        var idDenseText = "id_dense_text";
        var idRerank = "id_rerank";

        var nameCompletion = "completion_model";
        var nameChat = "chat_model";
        var nameSparse = "sparse_model";
        var nameDenseMultimodal = "dense_multimodal_model";
        var nameDenseText = "dense_text_model";
        var nameRerank = "rerank_model";

        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 256;
        var elementType = DenseVectorFieldMapper.ElementType.FLOAT.toString();

        var url = "base_url";

        var denseEmbeddingConfiguration = new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
            similarity.toString(),
            dimensions,
            elementType,
            null
        );
        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idCompletion,
                    nameCompletion,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idChat,
                    nameChat,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idSparse,
                    nameSparse,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idDenseMultimodal,
                    nameDenseMultimodal,
                    createTaskTypeObject(EIS_MULTIMODAL_EMBED_PATH, TaskType.EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    denseEmbeddingConfiguration,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idDenseText,
                    nameDenseText,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    denseEmbeddingConfiguration,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idRerank,
                    nameRerank,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.RERANK.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);

        var ids = Set.of(idCompletion, idChat, idSparse, idDenseMultimodal, idDenseText, idRerank);
        var endpoints = auth.getEndpoints(ids);
        assertThat(endpoints.size(), is(ids.size()));
        assertThat(
            endpoints,
            containsInAnyOrder(
                new ElasticInferenceServiceCompletionModel(
                    idCompletion,
                    TaskType.COMPLETION,
                    new ElasticInferenceServiceCompletionServiceSettings(nameCompletion),
                    new ElasticInferenceServiceComponents(url),
                    DEFAULT_ENDPOINT_METADATA
                ),
                new ElasticInferenceServiceCompletionModel(
                    idChat,
                    TaskType.CHAT_COMPLETION,
                    new ElasticInferenceServiceCompletionServiceSettings(nameChat),
                    new ElasticInferenceServiceComponents(url),
                    DEFAULT_ENDPOINT_METADATA
                ),
                new ElasticInferenceServiceSparseEmbeddingsModel(
                    idSparse,
                    TaskType.SPARSE_EMBEDDING,
                    new ElasticInferenceServiceSparseEmbeddingsServiceSettings(nameSparse, null, null),
                    new ElasticInferenceServiceComponents(url),
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS,
                    DEFAULT_ENDPOINT_METADATA
                ),
                new ElasticInferenceServiceDenseEmbeddingsModel(
                    idDenseMultimodal,
                    TaskType.EMBEDDING,
                    new ElasticInferenceServiceDenseEmbeddingsServiceSettings(nameDenseMultimodal, similarity, dimensions, null),
                    new ElasticInferenceServiceComponents(url),
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS,
                    DEFAULT_ENDPOINT_METADATA
                ),
                new ElasticInferenceServiceDenseEmbeddingsModel(
                    idDenseText,
                    TaskType.TEXT_EMBEDDING,
                    new ElasticInferenceServiceDenseEmbeddingsServiceSettings(nameDenseText, similarity, dimensions, null),
                    new ElasticInferenceServiceComponents(url),
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS,
                    DEFAULT_ENDPOINT_METADATA
                ),
                new ElasticInferenceServiceRerankModel(
                    idRerank,
                    TaskType.RERANK,
                    new ElasticInferenceServiceRerankServiceSettings(nameRerank),
                    new ElasticInferenceServiceComponents(url),
                    DEFAULT_ENDPOINT_METADATA
                )
            )
        );
    }

    public void testCreatesEndpointMetadataWithHeuristics() {
        var id = "id1";
        var name = "model1";
        var url = "base_url";
        var properties = List.of("multilingual", "preview");
        var statusHeuristic = randomFrom(StatusHeuristic.values());
        var status = statusHeuristic.toString();
        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    properties,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(properties, statusHeuristic, TEST_RELEASE_DATE_PARSED, TEST_END_OF_LIFE_DATE_PARSED),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testCreatesEndpointMetadataWithInternalFields() {
        var id = "id1";
        var name = "model1";
        var url = "base_url";
        var fingerprint = "fingerprint123";
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    fingerprint
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(status),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(fingerprint, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testCreatesEndpointMetadataWithDisplayName() {
        var id = "id1";
        var name = "model1";
        var url = "base_url";
        var displayName = "my-connector";
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    displayName,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(status),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display(displayName)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testHandlesNullPropertiesInHeuristics() {
        var id = "id1";
        var name = "model1";
        var url = "base_url";
        var statusHeuristic = randomFrom(StatusHeuristic.values());
        var status = statusHeuristic.toString();

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of(), statusHeuristic, TEST_RELEASE_DATE_PARSED, TEST_END_OF_LIFE_DATE_PARSED),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testHandlesNullDatesInHeuristics() {
        var id = "id1";
        var name = "model1";
        var url = "base_url";
        var statusHeuristic = randomFrom(StatusHeuristic.values());
        var status = statusHeuristic.toString();

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    null,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of(), statusHeuristic, TEST_RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testFiltersEndpointsWithInvalidReleaseDate() {
        var id1 = "id1";
        var invalidId = "invalid_id";
        var invalidId2 = "invalid_id2";
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name1",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidId,
                    "name2",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    "invalid-date-format",
                    null,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidId2,
                    "name3",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    "",
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, "url");
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertTrue(auth.isAuthorized());
    }

    public void testFiltersEndpointsWithInvalidEndOfLifeDate() {
        var id1 = "id1";
        var invalidId = "invalid_id";
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name1",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidId,
                    "name2",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    null,
                    "",
                    "invalid-date-format",
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, "url");
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertTrue(auth.isAuthorized());
    }

    public void testHandlesChunkingSettingsInSparseEmbeddings() {
        var id = "id_sparse";
        var name = "sparse_model";
        var url = "base_url";
        var status = STATUS_GA;
        Map<String, Object> chunkingSettings = ChunkingSettingsBuilder.DEFAULT_SETTINGS.asMap();

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(null, null, null, chunkingSettings),
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceSparseEmbeddingsModel(
            id,
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(name, null, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.fromMap(chunkingSettings),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(status),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testHandlesChunkingSettingsInDenseTextEmbeddings() {
        var id = "id_dense";
        var name = "dense_model";
        var url = "base_url";
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 256;
        var status = STATUS_GA;
        Map<String, Object> chunkingSettings = Map.of(
            ChunkingSettingsOptions.STRATEGY.toString(),
            ChunkingStrategy.WORD.toString(),
            ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
            ChunkingSettingsBuilder.ELASTIC_RERANKER_TOKEN_LIMIT,
            ChunkingSettingsOptions.OVERLAP.toString(),
            1
        );

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        chunkingSettings
                    ),
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceDenseEmbeddingsModel(
            id,
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(name, similarity, dimensions, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.fromMap(chunkingSettings),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(status),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testHandlesEmptyChunkingSettings() {
        var id = "id_sparse";
        var name = "sparse_model";
        var url = "base_url";
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(null, null, null, Map.of()),
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertTrue(auth.isAuthorized());

        var expectedEndpoint = new ElasticInferenceServiceSparseEmbeddingsModel(
            id,
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(name, null, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.fromMap(Map.of()),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(status),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(auth.getEndpoints(Set.of(id)).get(0), is(expectedEndpoint));
    }

    public void testGetEndpointsFiltersUnknownIds() {
        var id1 = "id1";
        var id2 = "id2";
        var name = "name";
        var url = "url";

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(
                    List.of(),
                    StatusHeuristic.fromString(STATUS_GA),
                    TEST_RELEASE_DATE_PARSED,
                    TEST_END_OF_LIFE_DATE_PARSED
                ),
                new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );
        assertThat(auth.getEndpoints(Set.of(id1, id2, "nonexistent")).get(0), is(expectedEndpoint));
    }

    public void testGetEndpointsWithEmptySet() {
        var id = "id1";
        var name = "name";
        var url = "url";

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    name,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    STATUS_GA,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    null,
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        var endpoints = auth.getEndpoints(Set.of());
        assertThat(endpoints, is(List.of()));
    }

    public void testNewLimitedToTaskTypesPreservesMetadata() {
        var id1 = "id1";
        var id2 = "id2";
        var name1 = "name1";
        var name2 = "name2";
        var url = "url";
        var fingerprint = "fingerprint123";
        var properties = List.of("multilingual");
        var status = STATUS_GA;

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    status,
                    properties,
                    TEST_RELEASE_DATE,
                    null,
                    null,
                    null,
                    fingerprint
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_TEXT_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    status,
                    null,
                    TEST_RELEASE_DATE,
                    TEST_END_OF_LIFE_DATE,
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.COSINE.toString(),
                        256,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    ),
                    null,
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        var scoped = auth.newLimitedToTaskTypes(EnumSet.of(TaskType.CHAT_COMPLETION));

        var expectedEndpoint = new ElasticInferenceServiceCompletionModel(
            id1,
            TaskType.CHAT_COMPLETION,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            new ElasticInferenceServiceComponents(url),
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(properties, StatusHeuristic.fromString(status), TEST_RELEASE_DATE_PARSED, null),
                new EndpointMetadata.Internal(fingerprint, ENDPOINT_SCHEMA_VERSION),
                new EndpointMetadata.Display((String) null)
            )
        );

        assertThat(scoped.getEndpoints(Set.of(id1, id2)).get(0), is(expectedEndpoint));
    }
}
