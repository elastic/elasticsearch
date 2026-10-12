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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertSame;

public class PrometheusQueryRestActionTests extends ESTestCase {

    private static final TimeValue NO_TIMEOUT = TimeValue.MINUS_ONE;

    private ThreadPool threadPool;
    private CapturingNodeClient client;

    @Before
    public void initClient() throws Exception {
        threadPool = createThreadPool();
        client = new CapturingNodeClient(threadPool);
    }

    @After
    public void terminateThreadPool() throws Exception {
        terminate(threadPool);
    }

    public void testInstantQueryMissingQueryParamThrows() {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of()).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT).prepareRequest(request, null)
        );
        assertThat(e.getMessage(), equalTo("required parameter \"query\" is missing"));
    }

    public void testInstantQueryDefaultsToMetricsIndexPattern() throws Exception {
        var request = instantQueryRequest(Map.of());

        handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);

        assertThat(capturedIndexPattern(), equalTo(DEFAULT_PROMQL_INDEX_PATTERN));
        request.getHttpChannel().close();
    }

    public void testQueryRangeDefaultsToMetricsIndexPattern() throws Exception {
        var request = rangeQueryRequest(Map.of());

        handle(new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT), request);

        assertThat(capturedIndexPattern(), equalTo(DEFAULT_PROMQL_INDEX_PATTERN));
        request.getHttpChannel().close();
    }

    public void testInstantQueryTimesOut() throws Exception {
        assertQueryTimesOut(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), instantQueryRequest(Map.of("timeout", "10ms")));
    }

    public void testQueryRangeTimesOut() throws Exception {
        assertQueryTimesOut(new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT), rangeQueryRequest(Map.of("timeout", "10ms")));
    }

    public void testQueryTimesOutAfterMaxTimeoutWithoutTimeoutParam() throws Exception {
        assertQueryTimesOut(new PrometheusInstantQueryRestAction(() -> TimeValue.timeValueMillis(10)), instantQueryRequest(Map.of()));
    }

    public void testQueryCompletingBeforeTimeoutIsNotCancelled() throws Exception {
        var request = instantQueryRequest(Map.of("timeout", "1h"));
        FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);

        client.failQuery.accept(new IllegalArgumentException("boom"));

        assertThat(sentResponses(channel), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(client.cancelRequests, empty());
        request.getHttpChannel().close();
    }

    public void testClosingHttpChannelCancelsQuery() throws Exception {
        var request = rangeQueryRequest(Map.of());
        handle(new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT), request);
        assertThat(client.cancelRequests, empty());

        request.getHttpChannel().close();

        assertBusy(() -> assertThat(client.cancelRequests, hasSize(1)));
        assertThat(client.cancelRequests.getFirst().getTargetTaskId(), equalTo(client.taskId()));
    }

    public void testInvalidTimeoutParamThrows() {
        var request = instantQueryRequest(Map.of("timeout", "soon"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT).prepareRequest(request, client)
        );
        assertThat(e.getMessage(), equalTo("invalid parameter \"timeout\": cannot parse \"soon\" to a valid duration"));
    }

    private void assertQueryTimesOut(BaseRestHandler action, RestRequest request) throws Exception {
        FakeRestChannel channel = handle(action, request);

        assertBusy(() -> assertThat(client.cancelRequests, hasSize(1)));
        assertThat(client.cancelRequests.getFirst().getTargetTaskId(), equalTo(client.taskId()));
        // the response is only sent once the cancelled query has completed
        assertThat(sentResponses(channel), equalTo(0));

        client.failQuery.accept(new TaskCancelledException("cancelled"));
        assertThat(sentResponses(channel), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"errorType\":\"timeout\""));
        assertThat(body, containsString("query timed out after"));
        request.getHttpChannel().close();
    }

    private static int sentResponses(FakeRestChannel channel) {
        return channel.responses().get() + channel.errors().get();
    }

    private FakeRestChannel handle(BaseRestHandler action, RestRequest request) throws Exception {
        FakeRestChannel channel = new FakeRestChannel(request, false);
        action.handleRequest(request, channel, client);
        return channel;
    }

    private static RestRequest instantQueryRequest(Map<String, String> extraParams) {
        return request(Map.of("query", "up", "time", "2026-01-01T00:00:00Z"), extraParams);
    }

    private static RestRequest rangeQueryRequest(Map<String, String> extraParams) {
        return request(Map.of("query", "up", "start", "2026-01-01T00:00:00Z", "end", "2026-01-01T01:00:00Z", "step", "1m"), extraParams);
    }

    private static RestRequest request(Map<String, String> params, Map<String, String> extraParams) {
        Map<String, String> allParams = new HashMap<>(params);
        allParams.putAll(extraParams);
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(allParams).build();
    }

    private String capturedIndexPattern() {
        assertNotNull(client.capturedRequest);
        return client.capturedRequest.statement().plan().collect(UnresolvedRelation.class).getFirst().indexPattern().indexPattern();
    }

    /**
     * Captures the ES|QL request instead of running it, and leaves it pending until the test completes it via {@link #failQuery}.
     * Records the task cancellations it receives.
     */
    private static class CapturingNodeClient extends NoOpNodeClient {
        private static final String LOCAL_NODE_ID = "local-node";

        private final AtomicLong taskIdGenerator = new AtomicLong();
        private final List<CancelTasksRequest> cancelRequests = new CopyOnWriteArrayList<>();
        private volatile PreparedEsqlQueryRequest capturedRequest;
        private volatile Consumer<Exception> failQuery;
        private volatile Task task;

        CapturingNodeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertSame(EsqlQueryAction.INSTANCE, action);
            capturedRequest = (PreparedEsqlQueryRequest) request;
            failQuery = listener::onFailure;
            task = new CancellableTask(taskIdGenerator.incrementAndGet(), "transport", action.name(), "", TaskId.EMPTY_TASK_ID, Map.of());
            return task;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof CancelTasksRequest cancelTasksRequest) {
                cancelRequests.add(cancelTasksRequest);
                listener.onResponse(null);
            } else {
                fail("unexpected action [" + action.name() + "]");
            }
        }

        @Override
        public String getLocalNodeId() {
            return LOCAL_NODE_ID;
        }

        TaskId taskId() {
            return new TaskId(LOCAL_NODE_ID, task.getId());
        }
    }
}
