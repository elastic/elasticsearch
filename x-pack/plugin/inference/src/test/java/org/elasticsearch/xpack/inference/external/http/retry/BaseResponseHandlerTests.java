/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.toRestStatus;
import static org.hamcrest.core.Is.is;

public class BaseResponseHandlerTests extends ESTestCase {
    public void testToRestStatus_ReturnsBadRequest_WhenStatusIs500() {
        assertThat(toRestStatus(500), is(RestStatus.BAD_REQUEST));
    }

    public void testToRestStatus_ReturnsBadRequest_WhenStatusIs501() {
        assertThat(toRestStatus(501), is(RestStatus.BAD_REQUEST));
    }

    public void testToRestStatus_ReturnsStatusCodeValue_WhenStatusIs200() {
        assertThat(toRestStatus(200), is(RestStatus.OK));
    }

    public void testToRestStatus_ReturnsBadRequest_WhenStatusIsUnknown() {
        assertThat(toRestStatus(1000), is(RestStatus.BAD_REQUEST));
    }
}
