/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CohereErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String responseJson = """
            {
                "message": "invalid request: total number of texts must be at most 96 - received 97"
            }
            """;

        CohereErrorResponseEntity errorMessage = CohereErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorMessage);
        MatcherAssert.assertThat(
            errorMessage.getErrorMessage(),
            is("invalid request: total number of texts must be at most 96 - received 97")
        );
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
                "error": "abc"
            }
            """;

        CohereErrorResponseEntity errorMessage = CohereErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNull(errorMessage);
    }
}
