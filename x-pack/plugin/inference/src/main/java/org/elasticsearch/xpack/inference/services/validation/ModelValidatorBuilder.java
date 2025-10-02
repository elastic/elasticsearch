/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;

import java.util.Objects;

public class ModelValidatorBuilder {
    public static ModelValidator buildModelValidator(TaskType taskType, InferenceService service) {
        if (service instanceof ElasticsearchInternalService) {
            return new ElasticsearchInternalServiceModelValidator(new SimpleServiceIntegrationValidator());
        } else {
            return buildModelValidatorForTaskType(taskType, service);
        }
    }

    private static ModelValidator buildModelValidatorForTaskType(TaskType taskType, InferenceService service) {
        if (taskType == null) {
            throw new IllegalArgumentException("Task type can't be null");
        }

        ServiceIntegrationValidator validatorFromService = null;
        if (service != null) {
            validatorFromService = service.getServiceIntegrationValidator(taskType);
        }

        switch (taskType) {
            case TEXT_EMBEDDING -> {
                return new TextEmbeddingModelValidator(
                    Objects.requireNonNullElse(validatorFromService, new SimpleServiceIntegrationValidator())
                );
            }
            case COMPLETION -> {
                return new ChatCompletionModelValidator(
                    Objects.requireNonNullElse(validatorFromService, new SimpleServiceIntegrationValidator())
                );
            }
            case CHAT_COMPLETION -> {
                return new ChatCompletionModelValidator(
                    Objects.requireNonNullElse(validatorFromService, new SimpleChatCompletionServiceIntegrationValidator())
                );
            }
            case SPARSE_EMBEDDING, RERANK, ANY -> {
                return new SimpleModelValidator(Objects.requireNonNullElse(validatorFromService, new SimpleServiceIntegrationValidator()));
            }
            default -> throw new IllegalArgumentException(Strings.format("Can't validate inference model for task type %s", taskType));
        }
    }
}
