/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_CHAT_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_EMBED_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_SPARSE_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createTaskTypeObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class ElasticInferenceServiceAuthorizationModelTests extends ESTestCase {

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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    "id2",
                    "name",
                    createTaskTypeObject("", TaskType.ANY.toString()),
                    "ga",
                    null,
                    "",
                    "",
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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    "name2",
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name2",
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
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
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
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
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
        );
        var textEmbeddingEndpoint = new ElasticInferenceServiceDenseTextEmbeddingsModel(
            id2,
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(name2, similarity, dimensions, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
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
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(name1),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
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
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                // Missing similarity measure
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding1,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        null,
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                ),
                // Invalid chunking settings
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding2,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        Map.of("unexpected_field", "unexpected_value")
                    )
                ),
                // Invalid similarity measure
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding3,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        "invalid_similarity",
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                ),
                // Missing dimensions
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding4,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.COSINE.toString(),
                        null,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                ),
                // Missing element type
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding4,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.COSINE.toString(),
                        123,
                        null,
                        null
                    )
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
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(name),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
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
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        // Valid element type as it should be converted to lower case
                        "fLoaT",
                        null
                    )
                ),
                // Valid with element type all caps
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        // Valid element type as it should be converted to lower case
                        "FLOAT",
                        null
                    )
                ),
                // Valid with element type all lower case
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    id3,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        "float",
                        null
                    )
                ),
                // Unsupported element type byte
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding1,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.BYTE.toString(),
                        null
                    )
                ),
                // Unsupported element type
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    invalidTextEmbedding2,
                    name,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarityMeasure.toString(),
                        dimensions,
                        "invalid-element-type",
                        null
                    )
                )
            )
        );

        var url = "url";

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2, id3)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.TEXT_EMBEDDING)));
        assertTrue(auth.isAuthorized());

        var textEmbeddingsModel1 = new ElasticInferenceServiceDenseTextEmbeddingsModel(
            id1,
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );

        var textEmbeddingsModel2 = new ElasticInferenceServiceDenseTextEmbeddingsModel(
            id2,
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );

        var textEmbeddingsModel3 = new ElasticInferenceServiceDenseTextEmbeddingsModel(
            id3,
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(name, similarityMeasure, dimensions, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );

        assertThat(
            auth.getEndpoints(
                Set.of(id1, id2, id3, invalidTextEmbedding1, invalidTextEmbedding2, invalidTextEmbedding3, invalidTextEmbedding4)
            ),
            containsInAnyOrder(textEmbeddingsModel1, textEmbeddingsModel2, textEmbeddingsModel3)
        );
    }

    public void testCreatesAllSupportedTaskTypesAndReturnsCorrectModels() {
        var idChat = "id_chat";
        var idSparse = "id_sparse";
        var idDense = "id_dense";
        var idRerank = "id_rerank";

        var nameChat = "chat_model";
        var nameSparse = "sparse_model";
        var nameDense = "dense_model";
        var nameRerank = "rerank_model";

        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 256;
        var elementType = DenseVectorFieldMapper.ElementType.FLOAT.toString();

        var url = "base_url";

        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idChat,
                    nameChat,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idSparse,
                    nameSparse,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idDense,
                    nameDense,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new ElasticInferenceServiceAuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        elementType,
                        null
                    )
                ),
                new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint(
                    idRerank,
                    nameRerank,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.RERANK.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                )
            )
        );

        var auth = ElasticInferenceServiceAuthorizationModel.of(response, url);

        var endpoints = auth.getEndpoints(Set.of(idChat, idSparse, idDense, idRerank));
        assertThat(endpoints.size(), is(4));
        assertThat(
            endpoints,
            containsInAnyOrder(
                new ElasticInferenceServiceCompletionModel(
                    idChat,
                    TaskType.CHAT_COMPLETION,
                    ElasticInferenceService.NAME,
                    new ElasticInferenceServiceCompletionServiceSettings(nameChat),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    new ElasticInferenceServiceComponents(url)
                ),
                new ElasticInferenceServiceSparseEmbeddingsModel(
                    idSparse,
                    TaskType.SPARSE_EMBEDDING,
                    ElasticInferenceService.NAME,
                    new ElasticInferenceServiceSparseEmbeddingsServiceSettings(nameSparse, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    new ElasticInferenceServiceComponents(url),
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                new ElasticInferenceServiceDenseTextEmbeddingsModel(
                    idDense,
                    TaskType.TEXT_EMBEDDING,
                    ElasticInferenceService.NAME,
                    new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(nameDense, similarity, dimensions, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    new ElasticInferenceServiceComponents(url),
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                new ElasticInferenceServiceRerankModel(
                    idRerank,
                    TaskType.RERANK,
                    ElasticInferenceService.NAME,
                    new ElasticInferenceServiceRerankServiceSettings(nameRerank),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    new ElasticInferenceServiceComponents(url)
                )
            )
        );
    }
}
