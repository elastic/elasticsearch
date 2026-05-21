/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertSame;

public class PrometheusQueryRestActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private CapturingNodeClient client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool();
        client = new CapturingNodeClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testInstantQueryDefaultsToMetricsIndexPattern() throws Exception {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("query", "up", "time", "2026-01-01T00:00:00Z")
        ).build();

        new PrometheusInstantQueryRestAction().handleRequest(request, new FakeRestChannel(request, false), client);

        assertThat(capturedIndexPattern(), equalTo(DEFAULT_PROMQL_INDEX_PATTERN));
    }

    public void testQueryRangeDefaultsToMetricsIndexPattern() throws Exception {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("query", "up", "start", "2026-01-01T00:00:00Z", "end", "2026-01-01T01:00:00Z", "step", "1m")
        ).build();

        new PrometheusQueryRangeRestAction().handleRequest(request, new FakeRestChannel(request, false), client);

        assertThat(capturedIndexPattern(), equalTo(DEFAULT_PROMQL_INDEX_PATTERN));
    }

    private String capturedIndexPattern() {
        assertNotNull(client.capturedRequest);
        return client.capturedRequest.statement().plan().collect(UnresolvedRelation.class).getFirst().indexPattern().indexPattern();
    }

    private static class CapturingNodeClient extends NoOpNodeClient {
        private PreparedEsqlQueryRequest capturedRequest;

        CapturingNodeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertSame(EsqlQueryAction.INSTANCE, action);
            capturedRequest = (PreparedEsqlQueryRequest) request;
        }
    }
}
