/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class InternalPreconfiguredEndpointsTests extends ESTestCase {
    public void testGetWithModelName_ReturnsAnEmptyList_IfNameDoesNotExist() {
        assertThat(InternalPreconfiguredEndpoints.getWithModelName("non-existent-model"), hasSize(0));
    }

    public void testGetWithModelName_ReturnsChatCompletionModels() {
        var models = InternalPreconfiguredEndpoints.getWithModelName(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);
        assertThat(models, hasSize(1));
    }

    public void testGetWithModelName_ReturnsGpLlmV2Models() {
        var models = InternalPreconfiguredEndpoints.getWithModelName(InternalPreconfiguredEndpoints.GP_LLM_V2_MODEL_ID);
        assertThat(models, hasSize(2));
        var taskTypes = models.stream().map(m -> m.configurations().getTaskType()).toList();
        assertTrue("Should contain CHAT_COMPLETION", taskTypes.contains(TaskType.CHAT_COMPLETION));
        assertTrue("Should contain COMPLETION", taskTypes.contains(TaskType.COMPLETION));
    }

    public void testGetWithInferenceId_ReturnsGpLlmV2CompletionEndpoint() {
        var model = InternalPreconfiguredEndpoints.getWithInferenceId(InternalPreconfiguredEndpoints.GP_LLM_V2_COMPLETION_ENDPOINT_ID);
        assertThat(model.configurations().getInferenceEntityId(), is(InternalPreconfiguredEndpoints.GP_LLM_V2_COMPLETION_ENDPOINT_ID));
        assertThat(model.configurations().getTaskType(), is(TaskType.COMPLETION));
    }
}
