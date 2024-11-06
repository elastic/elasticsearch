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

    // TODO: Once we merge all the other service validation code we can remove the checkModelConfig function
    // from each service, private the buildModelValidator function below this one, and call this directly from
    // TransportPutInferenceModelAction.java.
    public static ModelValidator buildModelValidator(TaskType taskType, boolean isElasticsearchInternalService) {
        if (isElasticsearchInternalService) {
            return new ElasticsearchInternalServiceModelValidator(buildModelValidator(taskType));
        } else {
            return buildModelValidator(taskType);
        }
    }

    public static ModelValidator buildModelValidator(TaskType taskType) {
        if (taskType == null) {
            throw new IllegalArgumentException("Task type can't be null");
        }

        switch (taskType) {
            case TEXT_EMBEDDING -> {
                return new TextEmbeddingModelValidator(new SimpleServiceIntegrationValidator());
            }
            case SPARSE_EMBEDDING, RERANK, COMPLETION, ANY -> {
                return new SimpleModelValidator(new SimpleServiceIntegrationValidator());
            }
            default -> throw new IllegalArgumentException(Strings.format("Can't validate inference model of for task type %s ", taskType));
        }
    }
}
