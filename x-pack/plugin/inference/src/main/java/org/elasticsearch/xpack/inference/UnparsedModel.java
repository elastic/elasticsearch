/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

public record UnparsedModel(String modelId, TaskType taskType, String service, Map<String, Object> settings, Map<String, Object> secrets) {

    public static UnparsedModel unparsedModelFromMap(Map<String, Object> configMap, Map<String, Object> secretsMap) {
        String modelId = removeStringOrThrowIfNull(configMap, ModelConfigurations.MODEL_ID);
        String service = removeStringOrThrowIfNull(configMap, ModelConfigurations.SERVICE);
        String taskTypeStr = removeStringOrThrowIfNull(configMap, TaskType.NAME);
        TaskType taskType = TaskType.fromString(taskTypeStr);

        return new UnparsedModel(modelId, taskType, service, configMap, secretsMap);
    }

    private static String removeStringOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        String value = (String) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }
}
