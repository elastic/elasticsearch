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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.ExemplarsQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.InstantQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.LabelValuesQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.LabelsQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.MetadataQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.RangeQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.SeriesQuery;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PrometheusQueryTransportActionTests extends ESTestCase {

    private static final Instant TIME = Instant.parse("2026-01-01T00:00:00Z");

    private ThreadPool threadPool;
    private MockTransport transport;
    private TransportService transportService;
    private CapturingNodeClient client;
    private PrometheusQueryTransportAction action;

    @Before
    public void createAction() {
        threadPool = createThreadPool();
        transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.create("local"),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            Set.of()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        client = new CapturingNodeClient(threadPool);
        action = new PrometheusQueryTransportAction(transportService, new ActionFilters(Set.of()), threadPool, client);
    }

    @After
    public void closeAction() throws Exception {
        transportService.close();
        transport.close();
        terminate(threadPool);
    }

    /**
     * The statement is built on the search thread pool, and runs as an ES|QL query that is a child of the action's task, so that
     * cancelling the task cancels the query.
     */
    public void testRunsTheStatementAsAChildEsqlQuery() throws Exception {
        var request = new PrometheusQueryRequest(new InstantQuery("up", "metrics-*", TIME), "up", "limit", 10);
        CancellableTask task = task(request);
        PlainActionFuture<PrometheusQueryResponse> future = new PlainActionFuture<>();

        action.execute(task, request, future);
        assertBusy(() -> assertThat(client.esqlRequest, notNullValue()));

        PromqlQueryRequest esqlRequest = client.esqlRequest;
        assertThat(client.thread, containsString("[" + ThreadPool.Names.SEARCH + "]"));
        assertThat(esqlRequest.getParentTask(), equalTo(new TaskId(transportService.getLocalNode().getId(), task.getId())));
        assertThat(esqlRequest.getIndex(), equalTo("metrics-*"));
        assertThat(esqlRequest.getType(), equalTo(PromqlQueryRequest.PROMQL_TYPE));
        assertThat(esqlRequest.queryDescription(), equalTo("up"));
        assertThat(esqlRequest.params().get("limit").value(), equalTo(10));
        assertThat(indexPattern(esqlRequest), equalTo("metrics-*"));

        EsqlQueryResponse esqlResponse = emptyEsqlResponse();
        try {
            client.esqlListener.onResponse(esqlResponse);
            PrometheusQueryResponse response = future.get();
            assertThat(response.esqlResponse(), sameInstance(esqlResponse));
            assertThat(response.resultType(), equalTo("vector"));
            assertThat(response.stringValue(), nullValue());
        } finally {
            esqlResponse.decRef();
        }
    }

    public void testStringLiteralIsAnsweredWithItsValue() throws Exception {
        var request = new PrometheusQueryRequest(new InstantQuery("\"a string\"", "metrics-*", TIME), "\"a string\"");
        PlainActionFuture<PrometheusQueryResponse> future = new PlainActionFuture<>();

        action.execute(task(request), request, future);
        assertBusy(() -> assertThat("it still runs as an ES|QL query", client.esqlRequest, notNullValue()));

        EsqlQueryResponse esqlResponse = emptyEsqlResponse();
        try {
            client.esqlListener.onResponse(esqlResponse);
            PrometheusQueryResponse response = future.get();
            assertThat(response.resultType(), equalTo("string"));
            assertThat(response.stringValue(), equalTo("a string"));
        } finally {
            esqlResponse.decRef();
        }
    }

    public void testInvalidQueryFailsWithoutRunningAnEsqlQuery() {
        var request = new PrometheusQueryRequest(new InstantQuery("up{", "metrics-*", TIME), "up{");
        PlainActionFuture<PrometheusQueryResponse> future = new PlainActionFuture<>();

        action.execute(task(request), request, future);

        expectThrows(ParsingException.class, future::actionGet);
        assertThat(client.esqlRequest, nullValue());
    }

    /** A request that waited for a search thread until its client went away isn't parsed. */
    public void testCancelledRequestIsNotParsed() {
        var request = new PrometheusQueryRequest(new InstantQuery("up{", "metrics-*", TIME), "up{");
        CancellableTask task = task(request);
        TaskCancelHelper.cancel(task, "client went away");
        PlainActionFuture<PrometheusQueryResponse> future = new PlainActionFuture<>();

        action.execute(task, request, future);

        expectThrows(TaskCancelledException.class, future::actionGet);
        assertThat(client.esqlRequest, nullValue());
    }

    /** Each endpoint's query reads its own index pattern. */
    public void testBuildsTheStatementOfEachEndpoint() {
        String index = randomAlphaOfLength(8) + "-*";
        Instant end = TIME.plusSeconds(3600);
        List<PrometheusQuery> queries = List.of(
            new InstantQuery("up", index, TIME),
            new RangeQuery("up", index, TIME.toString(), end.toString(), "60", 0),
            new ExemplarsQuery("rate(up[5m])", index, TIME, end),
            new SeriesQuery(index, List.of("up"), TIME, end, 0),
            new LabelsQuery(index, List.of("up"), TIME, end, 0),
            new LabelValuesQuery("job", index, List.of("up"), TIME, end, 0),
            new MetadataQuery(index, "up", 0, 0)
        );
        for (PrometheusQuery query : queries) {
            var statement = PrometheusQueryTransportAction.buildStatement(query).esqlStatement();
            assertThat(
                query.toString(),
                statement.plan().collect(UnresolvedRelation.class).getFirst().indexPattern().indexPattern(),
                equalTo(index)
            );
        }
    }

    private static CancellableTask task(PrometheusQueryRequest request) {
        var task = request.createTask(
            randomNonNegativeLong(),
            "transport",
            PrometheusQueryTransportAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertThat(task, instanceOf(CancellableTask.class));
        return (CancellableTask) task;
    }

    private static String indexPattern(PromqlQueryRequest request) {
        return request.statement().plan().collect(UnresolvedRelation.class).getFirst().indexPattern().indexPattern();
    }

    private static EsqlQueryResponse emptyEsqlResponse() {
        return new EsqlQueryResponse(List.of(), List.of(), 0, 0, null, false, false, ZoneOffset.UTC, 0, 0, null);
    }

    /**
     * Captures the ES|QL query the action runs instead of running it, with the thread it was sent from, and leaves it pending
     * until the test completes its listener.
     */
    private static class CapturingNodeClient extends NoOpNodeClient {
        private volatile PromqlQueryRequest esqlRequest;
        private volatile ActionListener<EsqlQueryResponse> esqlListener;
        private volatile String thread;

        CapturingNodeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked") // the response type of EsqlQueryAction
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, sameInstance(EsqlQueryAction.INSTANCE));
            thread = Thread.currentThread().getName();
            esqlListener = (ActionListener<EsqlQueryResponse>) listener;
            esqlRequest = (PromqlQueryRequest) request;
        }
    }
}
