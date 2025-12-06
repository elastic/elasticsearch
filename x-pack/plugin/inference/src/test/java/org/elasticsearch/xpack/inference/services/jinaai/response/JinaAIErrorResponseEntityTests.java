/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JinaAIErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String message = "\"input\" length 2049 is larger than the largest allowed size 2048";
        String escapedMessage = message.replace("\\", "\\\\").replace("\"", "\\\"");
        String responseJson = Strings.format("""
            {
                "detail": "%s"
            }
            """, escapedMessage);

        ErrorResponse errorResponse = JinaAIErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorResponse);
        MatcherAssert.assertThat(errorResponse.getErrorMessage(), is(message));
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
                "error": "abc"
            }
            """;

        ErrorResponse errorResponse = JinaAIErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        MatcherAssert.assertThat(errorResponse, is(ErrorResponse.UNDEFINED_ERROR));
    }
}
