/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.services.custom.CustomResponseHandler.ERROR_PARSER;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomResponseHandlerTests extends ESTestCase {

    public void testErrorBodyParser() throws IOException {
        var expected = XContentHelper.stripWhitespace(Strings.format("""
                {
                    "error": {
                        "message": "%s",
                        "type": "content_too_large",
                        "param": null,
                        "code": null
                    }
                }
            """, "message"));

        assertThat(ERROR_PARSER.apply(createResult(400, "message")), is(new ErrorResponse(expected)));
    }

    private static HttpResult createResult(int statusCode, String message) throws IOException {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        String responseJson = XContentHelper.stripWhitespace(Strings.format("""
                {
                    "error": {
                        "message": "%s",
                        "type": "content_too_large",
                        "param": null,
                        "code": null
                    }
                }
            """, message));

        return new HttpResult(httpResponse, responseJson.getBytes(StandardCharsets.UTF_8));
    }
}
