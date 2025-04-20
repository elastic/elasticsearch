/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.isA;

public class ModelValidatorBuilderTests extends ESTestCase {
    public void testBuildModelValidator_NullTaskType() {
        assertThrows(IllegalArgumentException.class, () -> { ModelValidatorBuilder.buildModelValidator(null, false); });
    }

    public void testBuildModelValidator_ValidTaskType() {
        taskTypeToModelValidatorClassMap().forEach((taskType, modelValidatorClass) -> {
            assertThat(ModelValidatorBuilder.buildModelValidator(taskType, false), isA(modelValidatorClass));
        });
    }

    private Map<TaskType, Class<? extends ModelValidator>> taskTypeToModelValidatorClassMap() {
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            TextEmbeddingModelValidator.class,
            TaskType.SPARSE_EMBEDDING,
            SimpleModelValidator.class,
            TaskType.RERANK,
            SimpleModelValidator.class,
            TaskType.COMPLETION,
            ChatCompletionModelValidator.class,
            TaskType.CHAT_COMPLETION,
            ChatCompletionModelValidator.class,
            TaskType.ANY,
            SimpleModelValidator.class
        );
    }
}
