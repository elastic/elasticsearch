/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureOpenAiResponseHandlerTests extends ESTestCase {

    public void testBuildRateLimitErrorMessage() {
        int statusCode = 429;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var httpResult = new HttpResult(response, new byte[] {});

        {
            when(response.getFirstHeader(AzureOpenAiResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(AzureOpenAiResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(AzureOpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(
                new BasicHeader(AzureOpenAiResponseHandler.REMAINING_TOKENS, "99800")
            );

            var error = AzureOpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(error, containsString("Remaining tokens [99800]. Remaining requests [2999]"));
        }

        {
            when(response.getFirstHeader(AzureOpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = AzureOpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(error, containsString("Remaining tokens [unknown]. Remaining requests [2999]"));
        }

        {
            when(response.getFirstHeader(AzureOpenAiResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(AzureOpenAiResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(AzureOpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = AzureOpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(error, containsString("Remaining tokens [unknown]. Remaining requests [2999]"));
        }
    }

    private static HttpResult createContentTooLargeResult(int statusCode) {
        return createResult(
            statusCode,
            "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                + "0 for the completion). Please reduce your prompt; or completion length."
        );
    }

    private static HttpResult createResult(int statusCode, String message) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        String responseJson = Strings.format("""
                {
                    "error": {
                        "message": "%s",
                        "type": "content_too_large",
                        "param": null,
                        "code": null
                    }
                }
            """, message);

        return new HttpResult(httpResponse, responseJson.getBytes(StandardCharsets.UTF_8));
    }
}
