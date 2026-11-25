/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceDocumentExtractionResponseHandlerTests extends ESTestCase {

    private static final ElasticInferenceServiceDocumentExtractionResponseHandler HANDLER =
        new ElasticInferenceServiceDocumentExtractionResponseHandler("document extraction", (request, response) -> null);

    public void testBuildFailureStatusCodeException_ContentTooLarge_DoesNotRetry() {
        var request = mockRequest();

        var exception = HANDLER.buildFailureStatusCodeException(request, httpResult(413));

        assertThat(exception.shouldRetry(), is(false));
        assertThat(exception.getCause(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(exception.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(exception.rebuildRequest(request), is(request));
    }

    public void testBuildFailureStatusCodeException_ServerError_Retries() {
        var exception = HANDLER.buildFailureStatusCodeException(mockRequest(), httpResult(randomFrom(500, 503)));

        assertThat(exception.shouldRetry(), is(true));
    }

    public void testBuildFailureStatusCodeException_BadRequest_DoesNotRetry() {
        var exception = HANDLER.buildFailureStatusCodeException(mockRequest(), httpResult(400));

        assertThat(exception.shouldRetry(), is(false));
    }

    private static OutboundRequest mockRequest() {
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        return request;
    }

    private static HttpResult httpResult(int statusCode) {
        return new HttpResult(new BasicHttpResponse(statusCode), "{\"message\": \"failure\"}".getBytes(StandardCharsets.UTF_8));
    }
}
