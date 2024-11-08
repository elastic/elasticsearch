/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.googleaistudio;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleAiStudioErrorResponseEntityTests extends ESTestCase {

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

        var error = GoogleAiStudioErrorResponseEntity.fromResponse(result);
        assertNotNull(error);
        assertThat(error.getErrorMessage(), is("error message"));
    }

    public void testErrorResponse_ReturnsNullIfNoError() {
        var result = getMockResult("""
            {
                "foo": "bar"
            }
            """);

        var error = GoogleAiStudioErrorResponseEntity.fromResponse(result);
        assertNull(error);
    }

    public void testErrorResponse_ReturnsNullIfNotJson() {
        var result = getMockResult("error message");

        var error = GoogleAiStudioErrorResponseEntity.fromResponse(result);
        assertNull(error);
    }
}
