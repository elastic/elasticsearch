/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalQueryAdmission;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Dataset queries take one slot from the coordinating node's {@link ExternalQueryAdmission} and return it however they
 * end — success, failure, the analysis retry without the request filter, the streaming endpoint, async — and a node
 * whose slots and queue are full answers 429 while index queries are unaffected.
 */
public class ExternalQueryAdmissionIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 5;

    private String dataset;
    private String node;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Before
    public void registerFixture() throws Exception {
        StringBuilder csv = new StringBuilder("id:integer\n");
        for (int i = 0; i < ROWS; i++) {
            csv.append(i).append('\n');
        }
        Path file = createTempDir().resolve("admission.csv");
        Files.writeString(file, csv.toString(), StandardCharsets.UTF_8);
        dataset = registerLocalFileDataset("admission_ds", StoragePath.fileUri(file), Map.of("format", "csv"));
        node = internalCluster().getRandomNodeName();
    }

    private ExternalQueryAdmission gate() {
        return internalCluster().getInstance(PlanExecutor.class, node).datasetQueryAdmission();
    }

    private EsqlQueryResponse query(EsqlQueryRequest request) {
        return client(node).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT);
    }

    private String countQuery() {
        return "FROM " + dataset + " | STATS c = COUNT(*)";
    }

    private void assertCountAnswered(EsqlQueryResponse response) {
        assertThat(getValuesList(response).get(0).get(0), equalTo((long) ROWS));
    }

    /** Takes a slot from the gate directly, standing in for a dataset query already running on the node. */
    private Releasable holdSlot() {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        gate().acquire(() -> false, future);
        return future.actionGet(TIMEOUT);
    }

    private void limitTo(int concurrent, int queued) {
        updateClusterSettings(
            Settings.builder()
                .put(ExternalSourceSettings.ADMISSION_MAX_CONCURRENT_QUERIES.getKey(), concurrent)
                .put(ExternalSourceSettings.ADMISSION_MAX_QUEUED_QUERIES.getKey(), queued)
        );
    }

    private void resetLimits() {
        updateClusterSettings(
            Settings.builder()
                .putNull(ExternalSourceSettings.ADMISSION_MAX_CONCURRENT_QUERIES.getKey())
                .putNull(ExternalSourceSettings.ADMISSION_MAX_QUEUED_QUERIES.getKey())
        );
    }

    public void testDatasetQueryTakesAndReturnsOneSlot() {
        long before = gate().admitted();
        try (EsqlQueryResponse response = query(syncEsqlQueryRequest(countQuery()))) {
            assertCountAnswered(response);
        }
        assertThat(gate().admitted(), equalTo(before + 1));
        assertThat(gate().running(), equalTo(0));
    }

    public void testFailedQueryReturnsItsSlot() {
        long before = gate().admitted();
        expectThrows(VerificationException.class, () -> query(syncEsqlQueryRequest("FROM " + dataset + " | WHERE no_such > 1")));
        assertThat(gate().admitted(), equalTo(before + 1));
        assertThat(gate().running(), equalTo(0));
    }

    /**
     * A request filter makes a failed analysis retry without the filter, which resolves the dataset a second time. The
     * query must still ask the gate once, not once per attempt: a second request could wait in the queue behind a slot
     * the same query already holds.
     */
    @TestLogging(value = "org.elasticsearch.xpack.esql.session.EsqlSession:DEBUG", reason = "asserts the retry without the filter ran")
    public void testAnalysisRetryWithoutTheFilterTakesOneSlot() {
        long before = gate().admitted();
        MockLog.assertThatLogger(
            () -> expectThrows(
                VerificationException.class,
                () -> query(syncEsqlQueryRequest("FROM " + dataset + " | WHERE no_such > 1").filter(QueryBuilders.matchAllQuery()))
            ),
            EsqlSession.class,
            new MockLog.SeenEventExpectation(
                "retry without the filter",
                EsqlSession.class.getCanonicalName(),
                Level.DEBUG,
                "Analyzing the plan (second attempt, without filter)"
            )
        );
        assertThat(gate().admitted(), equalTo(before + 1));
        assertThat(gate().running(), equalTo(0));
    }

    public void testStreamingQueryReturnsItsSlot() throws Exception {
        long before = gate().admitted();
        var subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        StreamQueryTestUtils.executeStreamRequest(client(node), syncEsqlQueryRequest("FROM " + dataset), subscriber);
        assertThat(subscriber.rowCount.get(), equalTo(ROWS));
        assertBusy(() -> assertThat(gate().running(), equalTo(0)));
        assertThat(gate().admitted(), equalTo(before + 1));
    }

    public void testAsyncQueryReturnsItsSlot() {
        long before = gate().admitted();
        EsqlQueryRequest request = asyncEsqlQueryRequest(countQuery());
        request.waitForCompletionTimeout(TIMEOUT);
        request.keepOnCompletion(false);
        try (EsqlQueryResponse response = query(request)) {
            assertCountAnswered(response);
        }
        assertThat(gate().admitted(), equalTo(before + 1));
        assertThat(gate().running(), equalTo(0));
    }

    public void testFullNodeRefusesWithTooManyRequests() {
        limitTo(1, 0);
        try (Releasable held = holdSlot()) {
            Exception e = expectThrows(Exception.class, () -> query(syncEsqlQueryRequest(countQuery())));
            EsRejectedExecutionException rejected = (EsRejectedExecutionException) ExceptionsHelper.unwrap(
                e,
                EsRejectedExecutionException.class
            );
            assertThat(e.toString(), rejected, notNullValue());
            assertThat(ExceptionsHelper.status(rejected), equalTo(RestStatus.TOO_MANY_REQUESTS));
        } finally {
            resetLimits();
        }
        try (EsqlQueryResponse response = query(syncEsqlQueryRequest(countQuery()))) {
            assertCountAnswered(response);
        }
        assertThat(gate().running(), equalTo(0));
    }

    public void testQueuedQueryRunsWhenASlotFrees() throws Exception {
        limitTo(1, 1);
        try {
            ActionFuture<EsqlQueryResponse> waiting;
            try (Releasable held = holdSlot()) {
                waiting = client(node).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(countQuery()));
                assertBusy(() -> assertThat(gate().queued(), equalTo(1)));
                assertFalse(waiting.isDone());
            }
            try (EsqlQueryResponse response = waiting.actionGet(TIMEOUT)) {
                assertCountAnswered(response);
            }
            assertThat(gate().running(), equalTo(0));
        } finally {
            resetLimits();
        }
    }

    /** One dashboard of fifteen panels on a node with 1 GB of heap's worth of slots: every panel renders, in waves. */
    public void testOneDashboardOnASmallNodeRendersEveryPanel() {
        limitTo(4, 16);
        try {
            long admittedBefore = gate().admitted();
            long refusedBefore = gate().refusedQueueFull() + gate().refusedTimeout();
            List<ActionFuture<EsqlQueryResponse>> panels = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                panels.add(client(node).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(countQuery())));
            }
            for (ActionFuture<EsqlQueryResponse> panel : panels) {
                try (EsqlQueryResponse response = panel.actionGet(TIMEOUT)) {
                    assertCountAnswered(response);
                }
            }
            assertThat(gate().admitted(), equalTo(admittedBefore + 15));
            assertThat(gate().refusedQueueFull() + gate().refusedTimeout(), equalTo(refusedBefore));
            assertThat(gate().running(), equalTo(0));
        } finally {
            resetLimits();
        }
    }

    public void testQueryCancelledWhileWaitingLeavesTheQueue() throws Exception {
        limitTo(1, 1);
        try (Releasable held = holdSlot()) {
            ActionFuture<EsqlQueryResponse> waiting = client(node).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(countQuery()));
            assertBusy(() -> assertThat(gate().queued(), equalTo(1)));
            client().admin().cluster().prepareCancelTasks().setActions(EsqlQueryAction.NAME).get();
            Exception e = expectThrows(Exception.class, () -> waiting.actionGet(TIMEOUT));
            assertThat(e.toString(), ExceptionsHelper.unwrap(e, TaskCancelledException.class), notNullValue());
            assertThat(gate().queued(), equalTo(0));
            assertThat("the cancelled query never held a slot", gate().running(), equalTo(1));
        } finally {
            resetLimits();
        }
    }

    public void testIndexQueriesNeverWaitForASlot() {
        assertAcked(client().admin().indices().prepareCreate("admission_idx").setMapping("id", "type=integer"));
        client().prepareIndex("admission_idx").setSource("id", 1).get();
        client().admin().indices().prepareRefresh("admission_idx").get();
        limitTo(1, 0);
        try (Releasable held = holdSlot()) {
            try (EsqlQueryResponse response = query(syncEsqlQueryRequest("FROM admission_idx | STATS c = COUNT(*)"))) {
                assertThat(getValuesList(response).get(0).get(0), equalTo(1L));
            }
        } finally {
            resetLimits();
        }
    }

    public void testZeroTurnsTheGateOff() {
        limitTo(1, 0);
        try (Releasable held = holdSlot()) {
            limitTo(0, 0);
            try (EsqlQueryResponse response = query(syncEsqlQueryRequest(countQuery()))) {
                assertCountAnswered(response);
            }
            assertThat("the slot taken while the gate was on is still the only one counted", gate().running(), equalTo(1));
        } finally {
            resetLimits();
        }
        assertThat(gate().running(), equalTo(0));
    }

    /** A slot handed out on a node must be closed by the test or the query holding it; this catches a leak in any test. */
    @After
    public void assertNoSlotsLeftHeld() {
        for (String n : internalCluster().getNodeNames()) {
            assertThat(
                "dataset query slots left held on " + n,
                internalCluster().getInstance(PlanExecutor.class, n).datasetQueryAdmission().running(),
                equalTo(0)
            );
        }
    }
}
