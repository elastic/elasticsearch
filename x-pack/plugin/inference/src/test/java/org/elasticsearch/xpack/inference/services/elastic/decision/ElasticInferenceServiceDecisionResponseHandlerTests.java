/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceDecisionResponseEntity;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceDecisionResponseHandlerTests extends ESTestCase {

    private static final ElasticInferenceServiceDecisionResponseHandler HANDLER = new ElasticInferenceServiceDecisionResponseHandler(
        "decision",
        (request, response) -> ElasticInferenceServiceDecisionResponseEntity.fromResponse(response)
    );

    public void testBuildFailureStatusCodeException_NeverRetries() {
        // These status codes are retried or truncated and retried by the default handler
        var statusCode = randomFrom(400, 405, 413, 422, 500, 503, randomIntBetween(300, 428), randomIntBetween(430, 599));

        var exception = HANDLER.buildFailureStatusCodeException(mockRequest(), httpResult(statusCode, "{\"message\": \"failure\"}"));

        assertThat(exception.shouldRetry(), is(false));
        assertThat(exception.getCause(), instanceOf(ElasticsearchStatusException.class));
        assertThat(exception.getCause().getMessage(), containsString("for request from inference entity id [id] status [" + statusCode));
    }

    public void testBuildFailureStatusCodeException_RateLimit_Retries() {
        var exception = HANDLER.buildFailureStatusCodeException(mockRequest(), httpResult(429, "{\"message\": \"slow down\"}"));

        assertThat(exception.shouldRetry(), is(true));
        assertThat(exception.getCause(), instanceOf(ElasticsearchStatusException.class));
        assertThat(exception.getCause().getMessage(), containsString("Received a rate limit status code"));
    }

    public void testBuildFailureStatusCodeException_ContentTooLarge_DoesNotRebuildTheRequest() {
        var request = mockRequest();

        var exception = HANDLER.buildFailureStatusCodeException(request, httpResult(413, "{}"));

        assertThat(exception.rebuildRequest(request), is(request));
    }

    public void testParseResult_UnparsableResponse_DoesNotRetry() {
        for (var body : new String[] { "not json", "{\"answers\": [", "{}", "{\"answers\": [{\"type\": \"ranking\", \"name\": \"q\"}]}" }) {
            var exception = expectThrows(RetryException.class, () -> HANDLER.parseResult(mockRequest(), httpResult(200, body)));
            assertThat(exception.shouldRetry(), is(false));
        }
    }

    public void testParseResult_ValidResponse() {
        var result = HANDLER.parseResult(mockRequest(), httpResult(200, """
            {"answers": [{"type": "predicate", "name": "q", "probability": 0.5}]}
            """));

        assertThat(result.asMap().isEmpty(), is(false));
    }

    private static OutboundRequest mockRequest() {
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        return request;
    }

    private static HttpResult httpResult(int statusCode, String body) {
        return new HttpResult(new BasicHttpResponse(statusCode), body.getBytes(StandardCharsets.UTF_8));
    }
}
