/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
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

    public void testInstantQueryMissingQueryParamReturnsBadData() throws Exception {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of()).build();
        FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);
        assertBadData(channel, "required parameter \"query\" is missing");
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

    /**
     * The query is parsed by the transport action, once it has authorized the request, not by the REST handler: an invalid one
     * reaches the action, which is what refuses it.
     */
    public void testInstantQueryIsParsedByTheTransportAction() throws Exception {
        assertParsedByTheTransportAction(
            new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT),
            instantQueryRequest(Map.of("query", "up{")),
            PrometheusQuery.InstantQuery.class,
            ParsingException.class
        );
    }

    public void testQueryRangeIsParsedByTheTransportAction() throws Exception {
        assertParsedByTheTransportAction(
            new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT),
            rangeQueryRequest(Map.of("query", "up{")),
            PrometheusQuery.RangeQuery.class,
            ParsingException.class
        );
    }

    public void testQueryExemplarsIsParsedByTheTransportAction() throws Exception {
        assertParsedByTheTransportAction(
            new PrometheusQueryExemplarsRestAction(() -> NO_TIMEOUT, new NoneCircuitBreakerService()),
            request(Map.of("query", "up{", "start", "2026-01-01T00:00:00Z", "end", "2026-01-01T01:00:00Z"), Map.of()),
            PrometheusQuery.ExemplarsQuery.class,
            ParsingException.class
        );
    }

    public void testSeriesSelectorIsParsedByTheTransportAction() throws Exception {
        assertParsedByTheTransportAction(
            new PrometheusSeriesRestAction(),
            selectorRequest(),
            PrometheusQuery.SeriesQuery.class,
            IllegalArgumentException.class
        );
    }

    public void testLabelsSelectorIsParsedByTheTransportAction() throws Exception {
        assertParsedByTheTransportAction(
            new PrometheusLabelsRestAction(),
            selectorRequest(),
            PrometheusQuery.LabelsQuery.class,
            IllegalArgumentException.class
        );
    }

    public void testLabelValuesSelectorIsParsedByTheTransportAction() throws Exception {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            new HashMap<>(Map.of("name", "job", "match[]", "{"))
        ).build();
        assertParsedByTheTransportAction(
            new PrometheusLabelValuesRestAction(),
            request,
            PrometheusQuery.LabelValuesQuery.class,
            IllegalArgumentException.class
        );
    }

    public void testMetadataRunsInTheTransportAction() throws Exception {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(new HashMap<>(Map.of("metric", "up"))).build();
        FakeRestChannel channel = handle(new PrometheusMetadataRestAction(), request);
        assertThat(client.capturedRequest.query(), equalTo(new PrometheusQuery.MetadataQuery(DEFAULT_PROMQL_INDEX_PATTERN, "up", 0, 0)));
        assertThat(sentResponses(channel), equalTo(0));
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

    public void testQueryServerErrorIsLoggedAtWarn() throws Exception {
        var request = instantQueryRequest(Map.of());
        FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);

        String logger = PrometheusQueryResponseListener.class.getCanonicalName();
        MockLog.assertThatLogger(
            () -> client.failQuery.accept(new IllegalStateException("boom")),
            PrometheusQueryResponseListener.class,
            new MockLog.SeenEventExpectation("server error", logger, Level.WARN, "*status: 500")
        );
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        request.getHttpChannel().close();
    }

    public void testQueryClientErrorIsNotLoggedAtWarn() throws Exception {
        var request = instantQueryRequest(Map.of());
        FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);

        String logger = PrometheusQueryResponseListener.class.getCanonicalName();
        MockLog.assertThatLogger(
            () -> client.failQuery.accept(new IllegalArgumentException("boom")),
            PrometheusQueryResponseListener.class,
            new MockLog.UnseenEventExpectation("client error", logger, Level.WARN, "*")
        );
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        request.getHttpChannel().close();
    }

    public void testQueryTimeoutIsNotLoggedAtWarn() throws Exception {
        var request = instantQueryRequest(Map.of("timeout", "10ms"));
        String logger = PrometheusQueryResponseListener.class.getCanonicalName();
        try (var mockLog = MockLog.capture(PrometheusQueryResponseListener.class)) {
            mockLog.addExpectation(new MockLog.UnseenEventExpectation("timeout", logger, Level.WARN, "*"));
            FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);
            assertBusy(() -> assertThat(client.cancelRequests, hasSize(1)));
            client.failQuery.accept(new TaskCancelledException("cancelled"));
            assertThat(sentResponses(channel), equalTo(1));
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            mockLog.assertAllExpectationsMatched();
        }
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

    public void testInstantQueryInvalidTimeoutParamReturnsBadData() throws Exception {
        // the timeout is validated before the time parameter is read, which must not be reported as unrecognized instead
        var request = instantQueryRequest(Map.of("timeout", "soon"));
        FakeRestChannel channel = handle(new PrometheusInstantQueryRestAction(() -> NO_TIMEOUT), request);
        assertBadData(channel, "invalid parameter \"timeout\": cannot parse \"soon\" to a valid duration");
        assertThat(client.capturedRequest, nullValue());
    }

    public void testQueryRangeInvalidTimeoutParamReturnsBadData() throws Exception {
        var request = rangeQueryRequest(Map.of("timeout", "soon"));
        FakeRestChannel channel = handle(new PrometheusQueryRangeRestAction(() -> NO_TIMEOUT), request);
        assertBadData(channel, "invalid parameter \"timeout\": cannot parse \"soon\" to a valid duration");
        assertThat(client.capturedRequest, nullValue());
    }

    private static void assertBadData(FakeRestChannel channel, String expectedError) throws IOException {
        assertThat(sentResponses(channel), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        ObjectPath body = ObjectPath.createFromXContent(JsonXContent.jsonXContent, channel.capturedResponse().content());
        assertThat(body.evaluate("status"), equalTo("error"));
        assertThat(body.evaluate("errorType"), equalTo("bad_data"));
        assertThat(body.evaluate("error"), equalTo(expectedError));
    }

    private void assertParsedByTheTransportAction(
        BaseRestHandler action,
        RestRequest request,
        Class<? extends PrometheusQuery> queryType,
        Class<? extends Exception> parseFailure
    ) throws Exception {
        FakeRestChannel channel = handle(action, request);
        assertNotNull("the request reaches the transport action", client.capturedRequest);
        assertThat(sentResponses(channel), equalTo(0));
        assertThat(client.capturedRequest.query(), instanceOf(queryType));
        expectThrows(parseFailure, () -> PrometheusQueryTransportAction.buildStatement(client.capturedRequest.query()));
        request.getHttpChannel().close();
    }

    private static RestRequest selectorRequest() {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(new HashMap<>(Map.of("match[]", "{"))).build();
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
        return client.capturedRequest.query().index();
    }

    /**
     * Captures the request to {@link PrometheusQueryTransportAction} instead of running it, and leaves it pending until the test
     * completes it via {@link #failQuery}. Records the task cancellations it receives.
     */
    private static class CapturingNodeClient extends NoOpNodeClient {
        private static final String LOCAL_NODE_ID = "local-node";

        private final AtomicLong taskIdGenerator = new AtomicLong();
        private final List<CancelTasksRequest> cancelRequests = new CopyOnWriteArrayList<>();
        private volatile PrometheusQueryRequest capturedRequest;
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
            assertSame(PrometheusQueryTransportAction.TYPE, action);
            capturedRequest = (PrometheusQueryRequest) request;
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
            } else if (action == PrometheusQueryTransportAction.TYPE) {
                capturedRequest = (PrometheusQueryRequest) request;
                failQuery = listener::onFailure;
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
