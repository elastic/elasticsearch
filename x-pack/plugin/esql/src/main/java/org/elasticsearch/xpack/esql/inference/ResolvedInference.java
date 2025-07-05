/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;

public record ResolvedInference(String inferenceId, ModelConfigurations modelConfigurations) {

    public TaskType taskType() {
        return modelConfigurations.getTaskType();
    }
}
