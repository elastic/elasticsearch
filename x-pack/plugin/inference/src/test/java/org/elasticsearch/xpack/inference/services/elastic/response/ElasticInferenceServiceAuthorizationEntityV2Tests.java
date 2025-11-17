/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

public class ElasticInferenceServiceAuthorizationEntityV2Tests extends ESTestCase {
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
                null
            );
            case SPARSE_EMBEDDING -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.SPARSE_EMBEDDING.toString(),
                status,
                null,
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
                null
            );
            case COMPLETION -> new ElasticInferenceServiceAuthorizationResponseEntityV2.AuthorizedEndpoint(
                id,
                name,
                TaskType.COMPLETION.toString(),
                status,
                null,
                "",
                null
            );
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }
}
