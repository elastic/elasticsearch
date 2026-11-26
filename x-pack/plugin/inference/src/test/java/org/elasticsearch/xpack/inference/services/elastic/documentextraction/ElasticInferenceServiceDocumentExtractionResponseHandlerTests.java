/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandlerTests;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs all of the default EIS failure status code tests against the document extraction handler, which differs only in
 * that a 413 (content too large) is not retried.
 */
public class ElasticInferenceServiceDocumentExtractionResponseHandlerTests extends ElasticInferenceServiceResponseHandlerTests {

    public ElasticInferenceServiceDocumentExtractionResponseHandlerTests(FailureTestCase failureTestCase) {
        super(failureTestCase);
    }

    @Override
    protected ElasticInferenceServiceResponseHandler createHandler() {
        return new ElasticInferenceServiceDocumentExtractionResponseHandler("document extraction", (request, response) -> null);
    }

    @Override
    protected boolean expectedShouldRetry(FailureTestCase testCase) {
        if (testCase.inputStatusCode() == 413) {
            return false;
        }
        return super.expectedShouldRetry(testCase);
    }

    public void testBuildFailureStatusCodeException_ContentTooLarge_DoesNotRebuildTheRequest() {
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        var httpResult = new HttpResult(new BasicHttpResponse(413), "{\"message\": \"failure\"}".getBytes(StandardCharsets.UTF_8));

        var exception = createHandler().buildFailureStatusCodeException(request, httpResult);

        assertThat(exception.rebuildRequest(request), is(request));
    }
}
