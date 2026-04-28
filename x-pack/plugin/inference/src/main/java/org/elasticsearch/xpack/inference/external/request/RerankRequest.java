/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.inference.TaskType;

/**
 * Implementation of {@link Request} for {@link TaskType#RERANK} requests
 */
public interface RerankRequest extends Request {
    /**
     * Should not be overridden
     */
    @Override
    default TaskType getTaskType() {
        return TaskType.RERANK;
    }
}
