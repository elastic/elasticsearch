/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest;

import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public final class FakeRestChannel extends AbstractRestChannel {
    private final CountDownLatch latch;
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private RestResponse capturedRestResponse;

    public FakeRestChannel(RestRequest request, boolean detailedErrorsEnabled, int responseCount) {
        super(request, detailedErrorsEnabled);
        this.latch = new CountDownLatch(responseCount);
    }

    @Override
    public void sendResponse(RestResponse response) {
        this.capturedRestResponse = response;
        if (response.status() == RestStatus.OK) {
            responses.incrementAndGet();
        } else {
            errors.incrementAndGet();
        }
        latch.countDown();
    }

    public RestResponse capturedResponse() {
        return capturedRestResponse;
    }

    public AtomicInteger responses() {
        return responses;
    }

    public AtomicInteger errors() {
        return errors;
    }
}
