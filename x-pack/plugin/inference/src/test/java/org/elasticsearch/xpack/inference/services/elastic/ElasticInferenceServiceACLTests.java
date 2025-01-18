/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceACLTests extends ESTestCase {
    public static ElasticInferenceServiceACL createEnabledAcl() {
        return new ElasticInferenceServiceACL(Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING)));
    }

    public void testIsEnabled_ReturnsFalse_WithEmptyMap() {
        assertFalse(ElasticInferenceServiceACL.newDisabledService().isEnabled());
    }

    public void testEnabledTaskTypes_MergesFromSeparateModels() {
        assertThat(
            new ElasticInferenceServiceACL(
                Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING), "model-2", EnumSet.of(TaskType.SPARSE_EMBEDDING))
            ).enabledTaskTypes(),
            is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING))
        );
    }

    public void testEnabledTaskTypes_FromSingleEntry() {
        assertThat(
            new ElasticInferenceServiceACL(Map.of("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)))
                .enabledTaskTypes(),
            is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING))
        );
    }
}
