/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class OciGenAiErrorResponseEntityTests extends ESTestCase {

    public void testFromResponse_ParsesCodeAndMessage() {
        var error = OciGenAiErrorResponseEntity.fromResponse(httpResult("""
            { "code": "NotAuthorizedOrNotFound", "message": "Authorization failed or requested resource not found." }
            """));

        assertTrue(error.errorStructureFound());
        assertThat(error.getErrorMessage(), is("Authorization failed or requested resource not found."));
        assertThat(error.code(), is("NotAuthorizedOrNotFound"));
        assertThat(error.type(), is(OciGenAiErrorResponseEntity.OCI_GENAI_ERROR_TYPE));
        assertThat(error.param(), nullValue());
    }

    public void testFromResponse_NumericCode() {
        var error = OciGenAiErrorResponseEntity.fromResponse(httpResult("""
            { "code": 404, "message": "Entity with key cohere.does-not-exist not found" }
            """));

        assertThat(error.code(), is("404"));
        assertThat(error.getErrorMessage(), is("Entity with key cohere.does-not-exist not found"));
    }

    public void testFromResponse_MessageOnly() {
        var error = OciGenAiErrorResponseEntity.fromResponse(httpResult("""
            { "message": "boom" }
            """));

        assertThat(error.getErrorMessage(), is("boom"));
        assertThat(error.code(), nullValue());
    }

    public void testFromResponse_UnknownStructure() {
        assertThat(
            OciGenAiErrorResponseEntity.fromResponse(httpResult("{ \"error\": \"nope\" }")),
            sameInstance(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR)
        );
        assertThat(
            OciGenAiErrorResponseEntity.fromResponse(httpResult("not json")),
            sameInstance(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR)
        );
    }

    public void testParseFromString() {
        var error = OciGenAiErrorResponseEntity.ERROR_PARSER.parse("{ \"code\": \"TooManyRequests\", \"message\": \"slow down\" }");

        assertThat(error.code(), is("TooManyRequests"));
        assertThat(error.getErrorMessage(), is("slow down"));
    }

    private static HttpResult httpResult(String body) {
        return new HttpResult(mock(HttpResponse.class), body.getBytes(StandardCharsets.UTF_8));
    }
}
