/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SemanticFieldHighlighterIT extends AbstractInferenceFieldHighlighterIT {
    private static final Set<TaskType> SUPPORTED_TASK_TYPES = Set.of(TaskType.EMBEDDING);

    @Override
    Set<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    void addInferenceFieldsToMapping(XContentBuilder mapping, Map<String, String> fieldNameToInferenceIdMap) throws IOException {
        IntegrationTestUtils.addSemanticFieldsToMapping(mapping, fieldNameToInferenceIdMap);
    }
}
