/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiErrorResponseEntityTests extends ESTestCase {

    private static HttpResult getMockResult(String jsonString) {
        var response = mock(HttpResponse.class);
        return new HttpResult(response, Strings.toUTF8Bytes(jsonString));
    }

    public void testErrorResponse_ExtractsError() {
        var result = getMockResult("""
            {
                "error": {
                    "code": 400,
                    "message": "error message",
                    "status": "INVALID_ARGUMENT",
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.BadRequest",
                            "fieldViolations": [
                                {
                                    "description": "Invalid JSON payload received. Unknown name \\"abc\\": Cannot find field."
                                }
                            ]
                        }
                    ]
                }
            }
            """);

        var error = GoogleVertexAiErrorResponseEntity.fromResponse(result);
        assertNotNull(error);
        assertThat(error.getErrorMessage(), is("error message"));
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNoError() {
        var result = getMockResult("""
            {
                "foo": "bar"
            }
            """);

        var error = GoogleVertexAiErrorResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNotJson() {
        var result = getMockResult("error message");

        var error = GoogleVertexAiErrorResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

}
