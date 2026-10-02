/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SemanticTextInferenceFieldsIT extends AbstractInferenceFieldsIT {
    private static final Set<TaskType> SUPPORTED_TASK_TYPES = Set.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.TEXT_EMBEDDING,
        TaskType.EMBEDDING
    );

    @Override
    XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException {
        return IntegrationTestUtils.generateSemanticTextMapping(fieldNameToInferenceIdMap);
    }

    @Override
    Object generateFieldValue() {
        return SemanticTextFieldTests.randomSemanticTextInput();
    }

    public void testExcludeInferenceFieldsFromSource() throws Exception {
        excludeInferenceFieldsFromSourceTestCase(SUPPORTED_TASK_TYPES, IndexVersion.current(), IndexVersion.current(), 10);
    }

    public void testExcludeInferenceFieldsFromSourceOldIndexVersions() throws Exception {
        excludeInferenceFieldsFromSourceTestCase(
            SUPPORTED_TASK_TYPES,
            IndexVersions.SEMANTIC_TEXT_FIELD_TYPE,
            IndexVersionUtils.getPreviousVersion(IndexVersion.current()),
            40
        );
    }
}
