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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.telemetry.DirectClient;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PrometheusDirectClientTests extends ESTestCase {

    private static final TimeValue NO_TIMEOUT = TimeValue.MINUS_ONE;
    private static final Map<String, List<String>> GRAFANA_HEADERS = Map.of(
        "User-Agent",
        List.of("Grafana/13.3.0 (linux; amd64) prometheus/13.2.6")
    );

    private ThreadPool threadPool;
    private CapturingNodeClient client;

    @Before
    public void initClient() {
        threadPool = createThreadPool();
        client = new CapturingNodeClient(threadPool);
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testInstantQuery() throws Exception {
        Map<String, String> params = Map.of("query", "up", "time", "2026-01-01T00:00:00Z");
        assertThat(directClient(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), params, GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testRangeQuery() throws Exception {
        Map<String, String> params = Map.of("query", "up", "start", "2026-01-01T00:00:00Z", "end", "2026-01-01T01:00:00Z", "step", "1m");
        assertThat(directClient(new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT), params, GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testLabels() throws Exception {
        assertThat(directClient(new PrometheusLabelsRestAction(), Map.of(), GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testLabelValues() throws Exception {
        assertThat(directClient(new PrometheusLabelValuesRestAction(), Map.of("name", "job"), GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testSeries() throws Exception {
        assertThat(directClient(new PrometheusSeriesRestAction(), Map.of("match[]", "up"), GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testMetadata() throws Exception {
        assertThat(directClient(new PrometheusMetadataRestAction(), Map.of(), GRAFANA_HEADERS), is(DirectClient.GRAFANA));
    }

    public void testWithoutHeaders() throws Exception {
        assertThat(directClient(new PrometheusLabelsRestAction(), Map.of(), Map.of()), is(DirectClient.OTHER));
    }

    private DirectClient directClient(BaseRestHandler action, Map<String, String> params, Map<String, List<String>> headers)
        throws Exception {
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).withHeaders(headers).build();
        action.handleRequest(request, new FakeRestChannel(request, false), client);
        assertThat(client.capturedRequest, notNullValue());
        request.getHttpChannel().close();
        return client.capturedRequest.directClient();
    }

    /**
     * Captures the ES|QL request instead of running it. The query listener is never completed, so no response is built.
     */
    private static class CapturingNodeClient extends NoOpNodeClient {
        private final AtomicLong taskIdGenerator = new AtomicLong();
        private volatile EsqlQueryRequest capturedRequest;

        CapturingNodeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public String getLocalNodeId() {
            return "local-node";
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            capture(action, request);
            return new CancellableTask(taskIdGenerator.incrementAndGet(), "transport", action.name(), "", TaskId.EMPTY_TASK_ID, Map.of());
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == EsqlQueryAction.INSTANCE) {
                capture(action, request);
            } else {
                super.doExecute(action, request, listener);
            }
        }

        private void capture(ActionType<?> action, ActionRequest request) {
            assertSame(EsqlQueryAction.INSTANCE, action);
            capturedRequest = (EsqlQueryRequest) request;
        }
    }
}
