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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationResponseEntityV2Tests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceAuthorizationResponseEntityV2> {
    public static ElasticInferenceServiceAuthorizationResponseEntityV2 createResponse() {
        return new ElasticInferenceServiceAuthorizationResponseEntityV2(
            randomList(1, 5, () -> createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)))
        );
    }

    public static ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint createAuthorizedEndpoint(TaskType taskType) {
        var id = randomAlphaOfLength(10);
        var name = randomAlphaOfLength(10);
        var status = randomFrom("ga", "beta", "preview");

        return switch (taskType) {
            case CHAT_COMPLETION -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.CHAT_COMPLETION.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case SPARSE_EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.SPARSE_EMBEDDING.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case TEXT_EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.TEXT_EMBEDDING.toString(),
                status,
                null,
                "",
                "",
                new ElasticInferenceServiceAuthorizationResponseEntityV2.Configuration(
                    randomFrom(SimilarityMeasure.values()).toString(),
                    randomInt(),
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    null
                )
            );
            case RERANK -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.RERANK.toString(),
                status,
                null,
                "",
                "",
                null
            );
            case COMPLETION -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
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
        var json = """
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
                  "id": ".elastic-elser-v2",
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

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var entity = ElasticInferenceServiceAuthorizationResponseEntityV2.PARSER.apply(parser, null);

            var rainbowSprinklesId = ".rainbow-sprinkles-elastic";
            var elserModelId = ".elastic-elser-v2";
            var jinaEmbeddingsId = ".jina-embeddings-v3";
            var elasticRerankId = ".elastic-rerank-v1";

            var rainbowSprinklesModelName = "rainbow-sprinkles";
            var elserModelName = "elser_model_2";
            var jinaEmbeddingsModelName = "jina-embeddings-v3";
            var elasticRerankModelName = "elastic-rerank-v1";

            var elserMaxChunkSize = 250;
            var elserSentenceOverlap = 1;

            var jinaDimensions = 1024;
            var jinaElementType = DenseVectorFieldMapper.ElementType.FLOAT;
            var jinaSimilarity = SimilarityMeasure.COSINE;
            var jinaMaxChunkSize = 500;
            var jinaOverlap = 2;

            var expected = new ElasticInferenceServiceAuthorizationResponseEntityV2(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                        rainbowSprinklesId,
                        rainbowSprinklesModelName,
                        "chat_completion",
                        "ga",
                        List.of("multilingual"),
                        "2024-05-01",
                        "2025-12-31",
                        null
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                        elserModelId,
                        elserModelName,
                        "sparse_embedding",
                        "preview",
                        List.of("english"),
                        "2024-05-01",
                        null,
                        new ElasticInferenceServiceAuthorizationResponseEntityV2.Configuration(
                            null,
                            null,
                            null,
                            Map.of("strategy", "sentence", "max_chunk_size", elserMaxChunkSize, "sentence_overlap", elserSentenceOverlap)
                        )
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                        jinaEmbeddingsId,
                        jinaEmbeddingsModelName,
                        "text_embedding",
                        "beta",
                        List.of("multilingual", "open-weights"),
                        "2024-05-01",
                        null,
                        new ElasticInferenceServiceAuthorizationResponseEntityV2.Configuration(
                            jinaSimilarity.toString(),
                            jinaDimensions,
                            jinaElementType.toString(),
                            Map.of("strategy", "word", "max_chunk_size", jinaMaxChunkSize, "overlap", jinaOverlap)
                        )
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                        elasticRerankId,
                        elasticRerankModelName,
                        "rerank",
                        "preview",
                        List.of(),
                        "2024-05-01",
                        null,
                        null
                    )
                )
            );

            assertThat(entity, is(expected));

            var url = "http://example.com/authorize";
            var authModel = AuthorizationModel.of(expected, url);

            assertThat(authModel.getEndpointIds(), containsInAnyOrder(rainbowSprinklesId, elasticRerankId, elserModelId, jinaEmbeddingsId));
            assertThat(
                authModel.getTaskTypes(),
                is(EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING, TaskType.RERANK))
            );

            assertThat(
                authModel.getEndpoints(Set.of(rainbowSprinklesId, jinaEmbeddingsId, elasticRerankId, elserModelId)),
                containsInAnyOrder(
                    new ElasticInferenceServiceCompletionModel(
                        rainbowSprinklesId,
                        TaskType.CHAT_COMPLETION,
                        ElasticInferenceService.NAME,
                        new ElasticInferenceServiceCompletionServiceSettings(rainbowSprinklesModelName),
                        EmptyTaskSettings.INSTANCE,
                        EmptySecretSettings.INSTANCE,
                        new ElasticInferenceServiceComponents(url)
                    ),
                    new ElasticInferenceServiceSparseEmbeddingsModel(
                        elserModelId,
                        TaskType.SPARSE_EMBEDDING,
                        ElasticInferenceService.NAME,
                        new ElasticInferenceServiceSparseEmbeddingsServiceSettings(elserModelName, null),
                        EmptyTaskSettings.INSTANCE,
                        EmptySecretSettings.INSTANCE,
                        new ElasticInferenceServiceComponents(url),
                        new SentenceBoundaryChunkingSettings(elserMaxChunkSize, elserSentenceOverlap)
                    ),
                    new ElasticInferenceServiceDenseTextEmbeddingsModel(
                        jinaEmbeddingsId,
                        TaskType.TEXT_EMBEDDING,
                        ElasticInferenceService.NAME,
                        new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
                            jinaEmbeddingsModelName,
                            jinaSimilarity,
                            jinaDimensions,
                            null
                        ),
                        EmptyTaskSettings.INSTANCE,
                        EmptySecretSettings.INSTANCE,
                        new ElasticInferenceServiceComponents(url),
                        new WordBoundaryChunkingSettings(jinaMaxChunkSize, jinaOverlap)
                    ),
                    new ElasticInferenceServiceRerankModel(
                        elasticRerankId,
                        TaskType.RERANK,
                        ElasticInferenceService.NAME,
                        new ElasticInferenceServiceRerankServiceSettings(elasticRerankModelName),
                        EmptyTaskSettings.INSTANCE,
                        EmptySecretSettings.INSTANCE,
                        new ElasticInferenceServiceComponents(url)
                    )
                )
            );
        }
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntityV2 mutateInstanceForVersion(
        ElasticInferenceServiceAuthorizationResponseEntityV2 instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<ElasticInferenceServiceAuthorizationResponseEntityV2> instanceReader() {
        return ElasticInferenceServiceAuthorizationResponseEntityV2::new;
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntityV2 createTestInstance() {
        return createResponse();
    }

    @Override
    protected ElasticInferenceServiceAuthorizationResponseEntityV2 mutateInstance(
        ElasticInferenceServiceAuthorizationResponseEntityV2 instance
    ) throws IOException {
        var newEndpoints = new ArrayList<>(instance.getAuthorizedEndpoints());
        newEndpoints.add(createAuthorizedEndpoint(randomFrom(ElasticInferenceService.IMPLEMENTED_TASK_TYPES)));
        return new ElasticInferenceServiceAuthorizationResponseEntityV2(newEndpoints);
    }
}
