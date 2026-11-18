/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The memory a CSV query over an external dataset allocates on a node is its query's there, the same figure
 * {@link org.elasticsearch.xpack.esql.plugin.QueryMemoryIT} checks for an index {@code FROM}.
 * <p>
 * Pauses the read with {@link ExternalAsyncStopAndCancelIT.SlowFileDataSourcePlugin} so the charge is
 * visible mid-stream. After the query ends, and after a mid-read cancel, the scheduler holds nothing
 * for that id.
 * <p>
 * <b>M2</b> (explained on #166113, not changed): when a query holds nothing the node forgets it, and a
 * later part starts tracking from zero. A DF producer that outlives its part keeps charging the old
 * {@code QueryCircuitBreaker}; the request breaker still sees those bytes, {@code memoryBytes(queryId)}
 * does not. Do not "fix" {@code QueryCircuitBreaker}. The scheduler contract lives in
 * {@link org.elasticsearch.search.scheduling.QuerySchedulerMemoryTests#testForgottenQueryDoesNotCountLaterChargesOnOldBreaker}.
 * Coordinator planning on the query part is M4, not this suite.
 */
public class ExternalQueryMemoryIT extends AbstractExternalDataSourceIT {

    private static final String SCHEME = "slowfile";
    private static final int LEADING_ROWS = 20;
    private static final int TRICKLE_ROWS = 200;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        // SlowFileDataSourcePlugin is SPI-registered; adding it here would double-bind the scheme.
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .build();
    }

    /**
     * Installs the {@code slowfile://} fixture {@link ExternalAsyncStopAndCancelIT} owns. Those
     * statics are the only handle {@link ExternalAsyncStopAndCancelIT.SlowFileDataSourcePlugin}
     * reads; Gradle runs one IT class per JVM so the two suites do not interleave.
     */
    @Before
    public void setUpSlowFile() throws Exception {
        ExternalAsyncStopAndCancelIT.requestArrived = new CountDownLatch(1);
        ExternalAsyncStopAndCancelIT.releaseHandler = new CountDownLatch(1);
        ExternalAsyncStopAndCancelIT.trickleHold = null;
        ExternalAsyncStopAndCancelIT.planningHold = null;
        ExternalAsyncStopAndCancelIT.planningArrived = null;
        ExternalAsyncStopAndCancelIT.trickleBytesServed.set(0);

        ExternalAsyncStopAndCancelIT.slowFile = createTempFile("external-query-memory-", ".csv");
        StringBuilder body = new StringBuilder("id:integer,value:integer\n");
        for (int i = 0; i < LEADING_ROWS; i++) {
            body.append(i).append(',').append(i * 10).append('\n');
        }
        for (int i = 0; i < TRICKLE_ROWS; i++) {
            int n = LEADING_ROWS + i;
            body.append(n).append(',').append(n * 10).append('\n');
        }
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        Files.write(ExternalAsyncStopAndCancelIT.slowFile, bytes);

        int seenNewlines = 0;
        int leading = bytes.length;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\n') {
                seenNewlines++;
                if (seenNewlines == LEADING_ROWS + 1) {
                    leading = i + 1;
                    break;
                }
            }
        }
        ExternalAsyncStopAndCancelIT.leadingBytes = leading;
    }

    @After
    public void releaseSlowFile() {
        if (ExternalAsyncStopAndCancelIT.releaseHandler != null) {
            ExternalAsyncStopAndCancelIT.releaseHandler.countDown();
        }
        if (ExternalAsyncStopAndCancelIT.trickleHold != null) {
            ExternalAsyncStopAndCancelIT.trickleHold.countDown();
        }
        if (ExternalAsyncStopAndCancelIT.planningHold != null) {
            ExternalAsyncStopAndCancelIT.planningHold.countDown();
        }
    }

    /**
     * A paused CSV read charges the query. After the query ends, the scheduler holds 0 for that id.
     */
    public void testMemoryBytesRisesDuringPausedCsvAndIsZeroAfterEnd() throws Exception {
        assumeExternalCsv();
        ActionFuture<EsqlQueryResponse> response = startPausedCsv("memory_end");
        TaskId queryId = awaitCsvProducing("memory_end");
        long bytes = memoryBytes(queryId);
        assertThat("the query must hold bytes while the CSV read is paused", bytes, greaterThan(0L));
        // Do not countDown releaseHandler: that returns EOF mid-object and the store reports a
        // premature end. Let the trickle reach the real file EOF so the query completes cleanly.
        try (EsqlQueryResponse result = response.actionGet(60, TimeUnit.SECONDS)) {
            assertThat(result.columns().size(), equalTo(2));
        }
        assertQueryForgotten(queryId);
    }

    /**
     * Cancel mid-read: the query is forgotten and holds 0 bytes. Same leak check as ending naturally.
     */
    public void testMemoryBytesZeroAfterMidReadCancel() throws Exception {
        assumeExternalCsv();
        String marker = "memory_cancel";
        ExternalAsyncStopAndCancelIT.trickleHold = new CountDownLatch(1);
        ActionFuture<EsqlQueryResponse> response = startPausedCsv(marker);
        TaskId queryId = awaitCsvProducing(marker);
        try {
            assertThat("the query must hold bytes before cancel", memoryBytes(queryId), greaterThan(0L));

            CancelTasksRequest cancel = new CancelTasksRequest().setTargetTaskId(queryId).setReason("test cancel");
            cancel.setWaitForCompletion(true);
            ActionFuture<?> cancelFuture = client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancel);
            // Producer is parked on trickleHold; cancel cannot complete until the read unblocks.
            ExternalAsyncStopAndCancelIT.trickleHold.countDown();
            cancelFuture.actionGet(60, TimeUnit.SECONDS);

            Exception thrown = expectThrows(Exception.class, () -> {
                try (var ignored = response.actionGet(60, TimeUnit.SECONDS)) {
                    fail("cancel must hard-fail the external dataset query");
                }
            });
            assertThat(ExceptionsHelper.unwrap(thrown, TaskCancelledException.class), notNullValue());
        } finally {
            ExternalAsyncStopAndCancelIT.trickleHold.countDown();
            ExternalAsyncStopAndCancelIT.releaseHandler.countDown();
        }
        assertQueryForgotten(queryId);
    }

    /**
     * Shrinking the request breaker while CSV is mid-read must fail the query with a circuit break
     * and complete the future. That can be the producer/parser or the driver; this test does not
     * pin the thread. Do not wait for the node request breaker to drain: leftover bytes on the old
     * {@code QueryCircuitBreaker} after forget is M2, not a leak of {@code memoryBytes(queryId)}.
     */
    public void testBreakerTripOnProducerFailsQueryWithoutHang() throws Exception {
        assumeExternalCsv();
        ExternalAsyncStopAndCancelIT.trickleHold = new CountDownLatch(1);
        ActionFuture<EsqlQueryResponse> response = startPausedCsv("memory_trip");
        TaskId queryId = awaitCsvProducing("memory_trip");
        try {
            assertThat(memoryBytes(queryId), greaterThan(0L));
            // Below what the paused query already holds, so the next charge trips.
            setRequestCircuitBreakerLimit(ByteSizeValue.ofBytes(256));
            ExternalAsyncStopAndCancelIT.trickleHold.countDown();
            Exception thrown = expectThrows(Exception.class, () -> {
                try (var ignored = response.actionGet(60, TimeUnit.SECONDS)) {
                    fail("a trip must fail the query, not hang or return rows");
                }
            });
            Throwable cbe = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
            assertThat("the query must fail with a circuit break, not hang or return rows", cbe, notNullValue());
            assertThat(cbe.getMessage(), containsString("Data too large"));
        } finally {
            ExternalAsyncStopAndCancelIT.trickleHold.countDown();
            ExternalAsyncStopAndCancelIT.releaseHandler.countDown();
            setRequestCircuitBreakerLimit(null);
        }
        assertQueryForgotten(queryId);
    }

    private static void assumeExternalCsv() {
        assumeTrue("external_parsing_parallelism / page_size pragmas are snapshot-only", Build.current().isSnapshot());
    }

    private ActionFuture<EsqlQueryResponse> startPausedCsv(String marker) {
        String uri = SCHEME + "://" + StoragePath.fileUri(ExternalAsyncStopAndCancelIT.slowFile).substring("file://".length());
        String query = "// " + marker + "\nFROM " + registerDataset("query_memory", uri, Map.of()) + " | LIMIT 1000000";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(
                Settings.builder().put(QueryPragmas.PARSING_PARALLELISM.getKey(), 1).put(QueryPragmas.PAGE_SIZE.getKey(), 5).build()
            )
        );
        return client().execute(EsqlQueryAction.INSTANCE, request);
    }

    private static TaskId awaitCsvProducing(String marker) throws Exception {
        assertTrue(
            "storage fixture must receive the dataset read request",
            ExternalAsyncStopAndCancelIT.requestArrived.await(30, TimeUnit.SECONDS)
        );
        // Prefix rows are served at full speed; trickle is the pause. Same gate as
        // ExternalAsyncStopAndCancelIT, so cancel/trip land mid-read, not during the prefix.
        assertBusy(
            () -> assertThat(
                "producer must be in the trickle phase",
                ExternalAsyncStopAndCancelIT.trickleBytesServed.get(),
                greaterThan(0)
            ),
            30,
            TimeUnit.SECONDS
        );
        assertBusy(() -> {
            TaskId queryId = queryId(marker);
            // Driver local-breaker over-reserve also charges this figure; trickle proves CSV is
            // in the read, and WP-1/WP-2a put those pages on the same query breaker.
            assertThat("the query must hold bytes while the CSV read is paused", memoryBytes(queryId), greaterThan(0L));
        }, 30, TimeUnit.SECONDS);
        return queryId(marker);
    }

    private static TaskId queryId(String marker) {
        ListTasksResponse tasks = clusterAdmin().prepareListTasks().setActions(EsqlQueryAction.NAME).setDetailed(true).get();
        for (TaskInfo info : tasks.getTasks()) {
            String description = info.description();
            if (description != null && description.contains(marker)) {
                return info.taskId();
            }
        }
        throw new AssertionError("no EsqlQueryAction task for marker [" + marker + "] in " + tasks.getTasks());
    }

    private static long memoryBytes(TaskId queryId) {
        long bytes = 0;
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            bytes += searchService.queryScheduler().memoryBytes(queryId);
        }
        return bytes;
    }

    private static void assertQueryForgotten(TaskId queryId) throws Exception {
        assertBusy(() -> assertThat("the query must hold no bytes after it ends", memoryBytes(queryId), equalTo(0L)));
    }
}
