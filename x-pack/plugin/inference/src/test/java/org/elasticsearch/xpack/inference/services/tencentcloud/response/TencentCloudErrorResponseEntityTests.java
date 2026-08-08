/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class TencentCloudErrorResponseEntityTests extends ESTestCase {

    public void testFromResponse_ParsesOpenAiCompatibleErrorObject() {
        String responseJson = """
            {
              "error": {
                "message": "Missing Authorization header",
                "type": "invalid_request_error",
                "code": "missing_api_key"
              }
            }
            """;

        var errorMessage = TencentCloudErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertNotNull(errorMessage);
        assertEquals("Missing Authorization header", errorMessage.getErrorMessage());
    }

    public void testFromResponse_FallsBackToTopLevelMessage() {
        String responseJson = """
            {
              "message": "internal server error"
            }
            """;

        var errorMessage = TencentCloudErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertEquals("internal server error", errorMessage.getErrorMessage());
    }

    public void testFromResponse_UnknownStructure_ReturnsUndefined() {
        String responseJson = """
            {"unexpected":"payload"}
            """;

        var errorMessage = TencentCloudErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertSame(ErrorResponse.UNDEFINED_ERROR, errorMessage);
    }

    public void testFromResponse_MalformedJson_ReturnsUndefined() {
        var errorMessage = TencentCloudErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), "not json".getBytes(StandardCharsets.UTF_8))
        );

        assertSame(ErrorResponse.UNDEFINED_ERROR, errorMessage);
    }
}
