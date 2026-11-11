/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesArray;
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
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.telemetry.DirectClient;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class RestEsqlQueryActionDirectClientTests extends ESTestCase {

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

    public void testSyncQueryFromPythonClient() throws Exception {
        RestRequest request = queryRequest(
            Map.of(),
            Map.of("x-elastic-client-meta", "es=9.5.0,py=3.12.1,t=9.4.2,ur=2.2.3", "User-Agent", "elasticsearch-py/9.5.0 (Python/3.12.1)")
        );
        assertThat(execute(new RestEsqlQueryAction(null), request).directClient(), is(DirectClient.PYTHON));
    }

    public void testAsyncQueryFromGrafana() throws Exception {
        RestRequest request = queryRequest(Map.of(), Map.of("User-Agent", "Grafana/12.3.0 (linux; amd64) elasticsearch/12.9.4"));
        assertThat(execute(new RestEsqlAsyncQueryAction(null), request).directClient(), is(DirectClient.GRAFANA));
    }

    public void testStreamingQueryFromCurl() throws Exception {
        assumeTrue("ES|QL streaming is not available in release builds yet", RestEsqlQueryAction.STREAMING_ENABLED);
        RestRequest request = queryRequest(
            Map.of(RestEsqlQueryAction.STREAMING_OPTION, "true", "format", RestEsqlQueryAction.NDJSON_FORMAT_VALUE),
            Map.of("User-Agent", "curl/8.7.1")
        );
        EsqlQueryRequest esqlRequest = execute(new RestEsqlQueryAction(null), request);
        assertThat(esqlRequest, instanceOf(EsqlStreamQueryRequest.class));
        assertThat(esqlRequest.directClient(), is(DirectClient.CURL));
    }

    public void testQueryFromKibanaIsNotADirectClient() throws Exception {
        RestRequest request = queryRequest(
            Map.of(),
            Map.of(
                "X-elastic-product-origin",
                "kibana",
                "x-elastic-client-meta",
                "es=9.5.1,js=22.11.0,t=9.1.1,hc=22.11.0",
                "User-Agent",
                "Kibana/9.5.1"
            )
        );
        assertThat(execute(new RestEsqlQueryAction(null), request).directClient(), is(DirectClient.ELASTIC_PRODUCT));
    }

    public void testQueryWithoutHeaders() throws Exception {
        RestRequest request = queryRequest(Map.of(), Map.of());
        assertThat(execute(new RestEsqlQueryAction(null), request).directClient(), is(DirectClient.OTHER));
    }

    private EsqlQueryRequest execute(BaseRestHandler action, RestRequest request) throws Exception {
        action.handleRequest(request, new FakeRestChannel(request, false), client);
        assertThat(client.capturedRequest, notNullValue());
        assertThat(client.capturedRequest, instanceOf(EsqlQueryRequest.class));
        request.getHttpChannel().close();
        return (EsqlQueryRequest) client.capturedRequest;
    }

    private static RestRequest queryRequest(Map<String, String> params, Map<String, String> headers) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withParams(params)
            .withHeaders(headers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> List.of(e.getValue()))))
            .withContent(new BytesArray("{\"query\": \"ROW a = 1\"}"), XContentType.JSON)
            .build();
    }

    /**
     * Captures the ES|QL request instead of running it. The query listener is never completed, so no response is built.
     */
    private static class CapturingNodeClient extends NoOpNodeClient {
        private final AtomicLong taskIdGenerator = new AtomicLong();
        private volatile ActionRequest capturedRequest;

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
            capturedRequest = request;
            return new CancellableTask(taskIdGenerator.incrementAndGet(), "transport", action.name(), "", TaskId.EMPTY_TASK_ID, Map.of());
        }
    }
}
