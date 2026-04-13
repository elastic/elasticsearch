/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

public class PrometheusSeriesRestActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private NoOpNodeClient client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool();
        client = new NoOpNodeClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testMissingMatchSelectorThrows() {
        var action = new PrometheusSeriesRestAction();
        var httpRequest = new FakeRestRequest.FakeHttpRequest(RestRequest.Method.GET, "/_prometheus/api/v1/series", null, Map.of());
        var request = RestRequest.request(parserConfig(), httpRequest, new FakeRestRequest.FakeHttpChannel(null));
        var e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, client));
        assertEquals("At least one [match[]] selector is required", e.getMessage());
    }
}
