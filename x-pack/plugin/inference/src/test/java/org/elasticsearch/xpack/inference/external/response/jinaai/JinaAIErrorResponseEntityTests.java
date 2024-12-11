/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.jinaai;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.common.Strings;
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JinaAIErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        //TODO(JoanFM): Check it works with real response
        String message = "\"input\" length 2049 is larger than the largest allowed size 2048";
        String escapedMessage = message.replace("\\", "\\\\").replace("\"", "\\\"");
        String responseJson = Strings.format("""
            {
                "detail": "%s"
            }
            """, escapedMessage);

        JinaAIErrorResponseEntity errorMessage = JinaAIErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorMessage);
        MatcherAssert.assertThat(
            errorMessage.getErrorMessage(),
            is(message)
        );
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
                "error": "abc"
            }
            """;

        JinaAIErrorResponseEntity errorMessage = JinaAIErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNull(errorMessage);
    }
}
