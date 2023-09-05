/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.services.elser;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

public class ElserMlNodeServiceTests extends ESTestCase {

    public static Model randomModelConfig(String modelId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new Model(
                modelId,
                taskType,
                ElserMlNodeService.NAME,
                new ElserServiceSettings(),
                new ElserSparseEmbeddingTaskSettings()
            );
            default -> throw new IllegalArgumentException("task type " + taskType + " is not supported");
        };
    }
}
