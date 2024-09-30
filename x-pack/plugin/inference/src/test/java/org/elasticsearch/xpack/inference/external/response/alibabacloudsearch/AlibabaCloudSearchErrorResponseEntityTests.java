/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.alibabacloudsearch;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String responseJson = """
            {
                "request_id": "651B3087-8A07-4BF3-B931-9C4E7B60F52D",
                "latency": 0,
                "code": "InvalidParameter",
                "message": "JSON parse error: Cannot deserialize value of type `InputType` from String \\"xxx\\""
            }
            """;

        AlibabaCloudSearchErrorResponseEntity errorMessage = AlibabaCloudSearchErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorMessage);
        assertEquals("JSON parse error: Cannot deserialize value of type `InputType` from String \"xxx\"", errorMessage.getErrorMessage());
    }
}
