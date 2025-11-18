/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_MODEL_ID_V1;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_MULTILINGUAL_EMBED_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_RERANK_ENDPOINT_ID_V1;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_RERANK_MODEL_ID_V1;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DENSE_TEXT_EMBEDDINGS_DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.GP_LLM_V2_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.defaultDenseTextEmbeddingsSimilarity;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PreconfiguredEndpointModelAdapterTests extends ESTestCase {

    private static final ElasticInferenceServiceSparseEmbeddingsServiceSettings SPARSE_SETTINGS =
        new ElasticInferenceServiceSparseEmbeddingsServiceSettings(DEFAULT_ELSER_2_MODEL_ID, null);
    private static final ElasticInferenceServiceCompletionServiceSettings COMPLETION_SETTINGS =
        new ElasticInferenceServiceCompletionServiceSettings(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);
    private static final ElasticInferenceServiceCompletionServiceSettings GP_LLM_V2_COMPLETION_SETTINGS =
        new ElasticInferenceServiceCompletionServiceSettings(GP_LLM_V2_MODEL_ID);
    private static final ElasticInferenceServiceDenseTextEmbeddingsServiceSettings DENSE_SETTINGS =
        new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
            defaultDenseTextEmbeddingsSimilarity(),
            DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
            null
        );
    private static final ElasticInferenceServiceRerankServiceSettings RERANK_SETTINGS = new ElasticInferenceServiceRerankServiceSettings(
        DEFAULT_RERANK_MODEL_ID_V1
    );
    private static final ElasticInferenceServiceComponents EIS_COMPONENTS = new ElasticInferenceServiceComponents("");

    public void testGetModelsWithValidId() {
        var endpointIds = Set.of(
            DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
            GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID,
            DEFAULT_ELSER_ENDPOINT_ID_V2,
            DEFAULT_RERANK_ENDPOINT_ID_V1,
            DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID
        );
        var models = PreconfiguredEndpointModelAdapter.getModels(endpointIds, EIS_COMPONENTS);

        assertThat(models, hasSize(endpointIds.size()));
        assertThat(
            models,
            containsInAnyOrder(
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_ELSER_ENDPOINT_ID_V2,
                        TaskType.SPARSE_EMBEDDING,
                        ElasticInferenceService.NAME,
                        SPARSE_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    SPARSE_SETTINGS,
                    EIS_COMPONENTS
                ),
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                        TaskType.CHAT_COMPLETION,
                        ElasticInferenceService.NAME,
                        COMPLETION_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    COMPLETION_SETTINGS,
                    EIS_COMPONENTS
                ),
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        GP_LLM_V2_CHAT_COMPLETION_ENDPOINT_ID,
                        TaskType.CHAT_COMPLETION,
                        ElasticInferenceService.NAME,
                        GP_LLM_V2_COMPLETION_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    GP_LLM_V2_COMPLETION_SETTINGS,
                    EIS_COMPONENTS
                ),
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                        TaskType.TEXT_EMBEDDING,
                        ElasticInferenceService.NAME,
                        DENSE_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    DENSE_SETTINGS,
                    EIS_COMPONENTS
                ),
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_RERANK_ENDPOINT_ID_V1,
                        TaskType.RERANK,
                        ElasticInferenceService.NAME,
                        RERANK_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    RERANK_SETTINGS,
                    EIS_COMPONENTS
                )
            )
        );
    }

    public void testGetModelsWithValidAndInvalidIds() {
        var models = PreconfiguredEndpointModelAdapter.getModels(
            Set.of(DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1, "some-invalid-id", DEFAULT_ELSER_ENDPOINT_ID_V2),
            EIS_COMPONENTS
        );

        assertThat(models, hasSize(2));
        assertThat(
            models,
            containsInAnyOrder(
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_ELSER_ENDPOINT_ID_V2,
                        TaskType.SPARSE_EMBEDDING,
                        ElasticInferenceService.NAME,
                        SPARSE_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    SPARSE_SETTINGS,
                    EIS_COMPONENTS
                ),
                new ElasticInferenceServiceModel(
                    new ModelConfigurations(
                        DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                        TaskType.CHAT_COMPLETION,
                        ElasticInferenceService.NAME,
                        COMPLETION_SETTINGS,
                        ChunkingSettingsBuilder.DEFAULT_SETTINGS
                    ),
                    new ModelSecrets(EmptySecretSettings.INSTANCE),
                    COMPLETION_SETTINGS,
                    EIS_COMPONENTS
                )
            )
        );
    }

    public void testGetModelsWithOnlyInvalidId() {
        assertThat(PreconfiguredEndpointModelAdapter.getModels(Collections.singleton("nonexistent-id"), EIS_COMPONENTS), is(List.of()));
    }
}
