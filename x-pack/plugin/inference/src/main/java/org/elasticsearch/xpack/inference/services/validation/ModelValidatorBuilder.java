/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;

public class ModelValidatorBuilder {
    public static ModelValidator buildModelValidator(TaskType taskType, boolean isElasticsearchInternalService) {
        if (isElasticsearchInternalService) {
            return new ElasticsearchInternalServiceModelValidator(new SimpleServiceIntegrationValidator());
        } else {
            return buildModelValidatorForTaskType(taskType);
        }
    }

    private static ModelValidator buildModelValidatorForTaskType(TaskType taskType) {
        if (taskType == null) {
            throw new IllegalArgumentException("Task type can't be null");
        }

        switch (taskType) {
            case TEXT_EMBEDDING -> {
                return new TextEmbeddingModelValidator(new SimpleServiceIntegrationValidator());
            }
            case COMPLETION -> {
                return new ChatCompletionModelValidator(new SimpleServiceIntegrationValidator());
            }
            case CHAT_COMPLETION -> {
                return new ChatCompletionModelValidator(new SimpleChatCompletionServiceIntegrationValidator());
            }
            case SPARSE_EMBEDDING, RERANK, ANY -> {
                return new SimpleModelValidator(new SimpleServiceIntegrationValidator());
            }
            default -> throw new IllegalArgumentException(Strings.format("Can't validate inference model for task type %s", taskType));
        }
    }
}
