/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasSize;

public class InternalPreconfiguredEndpointsTests extends ESTestCase {
    public void testGetWithModelName_ReturnsAnEmptyList_IfNameDoesNotExist() {
        assertThat(InternalPreconfiguredEndpoints.getWithModelName("non-existent-model"), hasSize(0));
    }

    public void testGetWithModelName_ReturnsChatCompletionModels() {
        var models = InternalPreconfiguredEndpoints.getWithModelName(InternalPreconfiguredEndpoints.DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);
        assertThat(models, hasSize(1));
    }
}
