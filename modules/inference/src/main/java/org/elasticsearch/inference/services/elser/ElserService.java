/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.services.InferenceService;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

public class ElserService implements InferenceService {

    public static final String NAME = "elser";

    public static Model parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> settings) {
        Map<String, Object> serviceSettings = removeMApOrThrowIfNull(settings, Model.SERVICE_SETTINGS);
        Map<String, Object> taskSettings = removeMApOrThrowIfNull(settings, Model.TASK_SETTINGS);

        return new Model(modelId, taskType, NAME, serviceSettingsFromMap(serviceSettings), taskSettingsFromMap(taskType, taskSettings));
    }

    @Override
    public Model parseConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfigLenient(modelId, taskType, config);
    }

    private static ServiceSettings serviceSettingsFromMap(Map<String, Object> config) {
        // no config yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config);
        }
        return new ElserServiceSettings();
    }

    private static TaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(
                "The [{}] service does not support task type [{}]",
                RestStatus.BAD_REQUEST,
                NAME,
                taskType
            );
        }

        // no config yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config);
        }

        return new ElserSparseEmbeddingTaskSettings();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> removeMApOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

    private static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config) {
        // TOOD map as JSON
        return new ElasticsearchStatusException(
            "Model configuration contains settings [{}] unknown to the [{}] service",
            RestStatus.BAD_REQUEST,
            config,
            NAME
        );
    }

    @Override
    public String name() {
        return NAME;
    }
}
