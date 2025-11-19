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
import org.elasticsearch.xpack.inference.services.elastic.response.AuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.elastic.response.AuthorizationResponseEntityTests.EIS_CHAT_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.AuthorizationResponseEntityTests.EIS_EMBED_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.AuthorizationResponseEntityTests.EIS_SPARSE_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.response.AuthorizationResponseEntityTests.createTaskTypeObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class AuthorizationModelTests extends ESTestCase {

    public void testIsAuthorized_ReturnsFalse_WithEmptyMap() {
        assertFalse(new AuthorizationModel(List.of()).isAuthorized());
        assertFalse(AuthorizationModel.empty().isAuthorized());
    }

    public void testExcludes_EndpointsWithoutValidTaskTypes() {
        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    "id",
                    "name",
                    createTaskTypeObject("", "invalid_task_type"),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
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
        var auth = AuthorizationModel.of(response, "url");
        assertTrue(auth.getTaskTypes().isEmpty());
        assertFalse(auth.isAuthorized());
    }

    public void testReturnsAuthorizedTaskTypes() {
        var id1 = "id1";
        var id2 = "id2";

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    "name1",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
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

        var auth = AuthorizationModel.of(response, "url");
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
        assertThat(auth.getEndpointIds(), is(Set.of(id1, id2)));
        assertTrue(auth.isAuthorized());
    }

    public void testReturnsAuthorizedTaskTypes_UsesFirstInferenceId_IfDuplicates() {
        var id = "id1";

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
                    "name1",
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                // This should be ignored because the id is a duplicate
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id,
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

        var auth = AuthorizationModel.of(response, "url");
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
        assertThat(auth.getEndpointIds(), is(Set.of(id)));
        assertTrue(auth.isAuthorized());
    }

    public void testReturnsAuthorizedEndpoints() {
        var id1 = "id1";
        var id2 = "id2";

        var name1 = "name1";
        var name2 = "name2";

        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 123;

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new AuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                )
            )
        );

        var url = "url";

        var auth = AuthorizationModel.of(response, url);
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

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new AuthorizationResponseEntity.Configuration(
                        similarity.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                )
            )
        );

        var url = "url";

        var auth = AuthorizationModel.of(response, url);
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
        var id2 = "invalid_text_embedding";

        var name1 = "name1";
        var name2 = "name2";

        var dimensions = 123;

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id1,
                    name1,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new AuthorizationResponseEntity.Configuration(
                        null,
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        null
                    )
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    id2,
                    name2,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new AuthorizationResponseEntity.Configuration(
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        dimensions,
                        DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                        // Invalid chunking settings
                        Map.of("unexpected_field", "unexpected_value")
                    )
                )
            )
        );

        var url = "url";

        var auth = AuthorizationModel.of(response, url);
        assertThat(auth.getEndpointIds(), is(Set.of(id1)));
        assertThat(auth.getTaskTypes(), is(Set.of(TaskType.CHAT_COMPLETION)));
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

        assertThat(auth.getEndpoints(Set.of(id1, id2)), is(List.of(chatCompletionEndpoint)));

        assertThat(auth.getEndpoints(Set.of(id2)), is(List.of()));
        assertThat(auth.getEndpoints(Set.of()), is(List.of()));
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

        var response = new AuthorizationResponseEntity(
            List.of(
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    idChat,
                    nameChat,
                    createTaskTypeObject(EIS_CHAT_PATH, TaskType.CHAT_COMPLETION.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    idSparse,
                    nameSparse,
                    createTaskTypeObject(EIS_SPARSE_PATH, TaskType.SPARSE_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    null
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
                    idDense,
                    nameDense,
                    createTaskTypeObject(EIS_EMBED_PATH, TaskType.TEXT_EMBEDDING.toString()),
                    "ga",
                    null,
                    "",
                    "",
                    new AuthorizationResponseEntity.Configuration(similarity.toString(), dimensions, elementType, null)
                ),
                new AuthorizationResponseEntity.AuthorizedEndpoint(
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

        var auth = AuthorizationModel.of(response, url);

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
