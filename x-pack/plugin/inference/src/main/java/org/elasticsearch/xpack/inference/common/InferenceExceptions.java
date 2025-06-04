/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;

public class InferenceExceptions {

    private InferenceExceptions() {}

    public static ElasticsearchStatusException mismatchedTaskTypeException(TaskType requested, TaskType expected) {
        return new ElasticsearchStatusException(
            "Requested task type [{}] does not match the inference endpoint's task type [{}]",
            RestStatus.BAD_REQUEST,
            requested,
            expected
        );
    }
}
