/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

public record UnparsedModel(
    String modelId,
    TaskType taskType,
    String service,
    Map<String, Object> serviceSettings,
    Map<String, Object> taskSettings
) {

    public static Model unparsedModelFromMap(Map<String, Object> sourceMap) {
//        String modelId = removeStringOrThrowIfNull(sourceMap, Model.MODEL_ID);
        String service = removeStringOrThrowIfNull(sourceMap, Model.SERVICE);
        Map<String, Object> serviceSettings = removeMApOrThrowIfNull(sourceMap, Model.SERVICE_SETTINGS);
        Map<String, Object> taskSettings = removeMApOrThrowIfNull(sourceMap, Model.TASK_SETTINGS);


    }

    public static Tuple<Model, Map<String, Object>> unparsedModelFromMapLenient(Map<String, Object> sourceMap) {


    }

    private static String removeStringOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        String value = (String) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

    private static Map<String, Object> removeMApOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

}


