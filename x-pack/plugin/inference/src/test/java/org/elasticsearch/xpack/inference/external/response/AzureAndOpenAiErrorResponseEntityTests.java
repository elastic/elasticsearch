/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureAndOpenAiErrorResponseEntityTests extends ESTestCase {

    private static HttpResult getMockResult(String jsonString) {
        var response = mock(HttpResponse.class);
        return new HttpResult(response, Strings.toUTF8Bytes(jsonString));
    }

    public void testErrorResponse_ExtractsError() {
        var result = getMockResult("""
            {"error":{"message":"test_error_message"}}""");

        var error = AzureMistralOpenAiErrorResponseEntity.fromResponse(result);
        assertNotNull(error);
        assertThat(error.getErrorMessage(), is("test_error_message"));
    }

    public void testErrorResponse_ReturnsNullIfNoError() {
        var result = getMockResult("""
            {"noerror":true}""");

        var error = AzureMistralOpenAiErrorResponseEntity.fromResponse(result);
        assertNull(error);
    }

    public void testErrorResponse_ReturnsNullIfNotJson() {
        var result = getMockResult("not a json string");

        var error = AzureMistralOpenAiErrorResponseEntity.fromResponse(result);
        assertNull(error);
    }
}
