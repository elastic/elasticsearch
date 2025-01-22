/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceAuthorizationResponseEntity;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationTests extends ESTestCase {
    public static ElasticInferenceServiceAuthorization createEnabledAuth() {
        return new ElasticInferenceServiceAuthorization(Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING)));
    }

    public void testIsEnabled_ReturnsFalse_WithEmptyMap() {
        assertFalse(ElasticInferenceServiceAuthorization.newDisabledService().isEnabled());
    }

    public void testExcludes_ModelsWithoutTaskTypes() {
        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-1", EnumSet.noneOf(TaskType.class)))
        );
        var auth = ElasticInferenceServiceAuthorization.of(response);
        assertTrue(auth.enabledTaskTypes().isEmpty());
        assertFalse(auth.isEnabled());
    }

    public void testConstructor_WithModelWithoutTaskTypes_ThrowsException() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ElasticInferenceServiceAuthorization(Map.of("model-1", EnumSet.noneOf(TaskType.class)))
        );
    }

    public void testEnabledTaskTypes_MergesFromSeparateModels() {
        assertThat(
            new ElasticInferenceServiceAuthorization(
                Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING), "model-2", EnumSet.of(TaskType.SPARSE_EMBEDDING))
            ).enabledTaskTypes(),
            is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING))
        );
    }

    public void testEnabledTaskTypes_FromSingleEntry() {
        assertThat(
            new ElasticInferenceServiceAuthorization(Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)))
                .enabledTaskTypes(),
            is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING))
        );
    }
}
