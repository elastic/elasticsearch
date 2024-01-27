/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestTests extends ESTestCase {
    public static Request mockRequest(String modelId) {
        var request = mock(Request.class);
        when(request.getModelId()).thenReturn(modelId);

        return request;
    }
}
