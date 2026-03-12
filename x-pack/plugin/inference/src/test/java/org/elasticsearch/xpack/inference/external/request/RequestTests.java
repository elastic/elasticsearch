/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestTests {
    public static Request mockRequest(String modelId) {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn(modelId);

        return request;
    }
}
