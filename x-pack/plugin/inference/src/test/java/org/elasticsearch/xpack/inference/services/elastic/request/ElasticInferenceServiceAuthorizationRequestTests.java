/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationRequestTests extends ESTestCase {

    private TraceContext traceContext;

    @Before
    public void init() {
        traceContext = new TraceContext("dummyTraceParent", "dummyTraceState");
    }

    public void testCreateUriThrowsForInvalidBaseUrl() {
        String invalidUrl = "http://invalid-url^";

        ElasticsearchStatusException exception = assertThrows(
            ElasticsearchStatusException.class,
            () -> new ElasticInferenceServiceAuthorizationRequest(invalidUrl, traceContext, randomElasticInferenceServiceRequestMetadata())
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Failed to create URI for service"));
    }
}
