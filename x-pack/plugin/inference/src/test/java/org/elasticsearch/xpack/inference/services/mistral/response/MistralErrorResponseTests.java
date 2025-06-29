/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class MistralErrorResponseTests extends ESTestCase {

    public static final String ERROR_RESPONSE_JSON = """
        {
            "error": "A valid user token is required"
        }
        """;

    public void testFromResponse() {
        var errorResponse = MistralErrorResponse.fromResponse(
            new HttpResult(mock(HttpResponse.class), ERROR_RESPONSE_JSON.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorResponse);
        assertEquals(ERROR_RESPONSE_JSON, errorResponse.getErrorMessage());
    }
}
