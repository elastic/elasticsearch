/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Locks the cancellation contract for a query whose {@code delay()} blocks the <em>coordinator's</em> final
 * driver (an {@code EVAL delay(...)} after {@code LIMIT} runs on the coordinator, downstream of the exchange
 * source and terminating in an {@code OutputOperator}). Every cancellation entry point must wind that driver
 * down promptly instead of waiting out the whole sleep:
 * <ul>
 *   <li><b>DELETE</b> (async hard cancel) rides {@code waitForCompletion=true}, so it genuinely blocks until the
 *       coordinator driver finishes. Cancellation is cooperative — {@code Driver.cancel} only sets a flag and
 *       never interrupts {@link Thread#sleep} — so {@code delay()} must poll
 *       {@code DriverContext.checkForEarlyTermination()} between sleep slices for DELETE to return quickly.</li>
 *   <li><b>STOP</b> (graceful) closes the exchange source and fires the per-task stop hooks, but neither sets
 *       the coordinator driver's cancel/early-finished flag; {@code delay()} registers a stop hook so a sleeping
 *       delay aborts and STOP returns partial results.</li>
 *   <li><b>{@code _tasks/{id}/_cancel}</b> (hard cancel, the path a client disconnect also takes) on the sync
 *       query task, on the async task, or directly on the coordinator {@code delay()} driver — all must fail the
 *       query with {@link TaskCancelledException} promptly and tear the tasks down.</li>
 * </ul>
 * These mirror the cancellation patterns in {@code EsqlActionTaskIT} (task/driver cancel) and
 * {@code AsyncEsqlQueryActionIT} (async cancel), but the block is a coordinator-side {@code delay()} rather than a
 * paused script. Without the {@code delay()} fix every entry point blocks for the full remaining delay; the
 * {@link #PROMPT} bound (well below {@link #DELAY}) is what turns a regression into a fast, clear failure.
 */
public class DelayCancellationIT extends AbstractEsqlIntegTestCase {

    /** Delay long enough that "waited the whole sleep" (a regression) is unmistakably distinct from a prompt cancel. */
    private static final TimeValue DELAY = timeValueSeconds(30);
    /** Upper bound a correctly-wired cancel must beat. Generous vs. the observed ~0.5s so CI slowness never flakes it,
     *  yet far below {@link #DELAY} so a regressed cancel (which blocks ~{@link #DELAY}) trips it. */
    private static final TimeValue PROMPT = timeValueSeconds(15);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // Wires up the async ES|QL actions (submit/get/stop/delete) used below.
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(InternalExchangePlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Before
    public void setupIndex() {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping("foo", "type=long").get();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5; i++) {
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        bulk.get();
    }

    /**
     * DELETE on a query blocked in a coordinator-side {@code delay()} must return well within {@link #PROMPT}
     * (not after the full {@link #DELAY}), tear the task down, and leave nothing retrievable.
     */
    public void testDeleteCancelsBlockingDelayPromptly() throws Exception {
        final String asyncId = submitBlockingDelayQuery();
        awaitDelayDriverSleeping();

        long start = System.nanoTime();
        ActionFuture<AcknowledgedResponse> deleteFuture = client().execute(
            TransportDeleteAsyncResultAction.TYPE,
            new DeleteAsyncResultRequest(asyncId)
        );
        AcknowledgedResponse deleteResponse;
        try {
            deleteResponse = deleteFuture.actionGet(PROMPT);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError(
                "DELETE did not return within "
                    + PROMPT
                    + " for a delay("
                    + DELAY
                    + ") query — the coordinator delay driver "
                    + "blocked cancellation instead of observing it between sleep slices (fix regressed)",
                e
            );
        }
        long elapsedMillis = TimeValue.nsecToMSec(System.nanoTime() - start);
        assertTrue(deleteResponse.isAcknowledged());
        logger.info("DELETE returned in {} ms for delay({})", elapsedMillis, DELAY);

        // The async task must be gone after DELETE torn it down.
        TaskId asyncTaskId = AsyncExecutionId.decode(asyncId).getTaskId();
        assertBusy(() -> assertFalse("async task should be gone after DELETE", isTaskRunning(asyncTaskId)));
        // And nothing should remain retrievable.
        Exception thrown = expectThrows(Exception.class, () -> { getAsyncResponse(client(), asyncId).close(); });
        assertThat(
            "GET after DELETE must surface ResourceNotFoundException, got: " + thrown,
            ExceptionsHelper.unwrap(thrown, ResourceNotFoundException.class),
            notNullValue()
        );
    }

    /**
     * STOP on a query blocked in a coordinator-side {@code delay()} must return well within {@link #PROMPT} with a
     * finished, partial response — the stop hook stops the sleep so the in-flight row drains through and the pipeline
     * completes naturally, rather than the whole {@link #DELAY} being slept out.
     */
    public void testStopCancelsBlockingDelayPromptly() throws Exception {
        final String asyncId = submitBlockingDelayQuery();
        awaitDelayDriverSleeping();

        long start = System.nanoTime();
        ActionFuture<EsqlQueryResponse> stopFuture = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(asyncId));
        EsqlQueryResponse response;
        try {
            response = stopFuture.actionGet(PROMPT);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError(
                "STOP did not return within "
                    + PROMPT
                    + " for a delay("
                    + DELAY
                    + ") query — the coordinator delay driver "
                    + "did not observe the stop hook and slept out the full delay (fix regressed)",
                e
            );
        }
        try {
            long elapsedMillis = TimeValue.nsecToMSec(System.nanoTime() - start);
            logger.info("STOP returned in {} ms for delay({})", elapsedMillis, DELAY);
            assertThat("STOP response must be finished", response.isRunning(), is(false));
            assertThat(
                "STOP must flag is_partial — the delay stop hook reported it cut a still-running unit of work, which "
                    + "TransportEsqlAsyncStopAction reports as partial",
                response.isPartial(),
                is(true)
            );
            assertThat(
                "STOP stops the delay sleeping and lets the in-flight LIMIT 1 row drain through to the output",
                countRows(response),
                equalTo(1)
            );
        } finally {
            response.close();
            EsqlAsyncTestUtils.deleteAsyncId(client(), asyncId);
        }
    }

    /**
     * Sync hard cancel via the generic {@code _tasks/{id}/_cancel} API (the same path a client disconnect takes) must
     * fail the query with {@link TaskCancelledException} well within {@link #PROMPT} and leave no tasks behind. Mirrors
     * {@code EsqlActionTaskIT#testCancelEsqlTask}, but the block is a coordinator-side {@code delay()} rather than a
     * paused script.
     */
    public void testSyncTaskCancelCancelsBlockingDelayPromptly() throws Exception {
        String marker = "delay_sync_cancel_" + System.nanoTime();
        ActionFuture<EsqlQueryResponse> queryFuture = submitSyncBlockingDelayQuery(marker);
        TaskId taskId = findEsqlQueryTask(marker);
        assertThat("the EsqlQueryAction task must be present while delay() is sleeping", taskId, notNullValue());
        awaitDelayDriverSleeping();

        long elapsedMillis = cancelAndWait(taskId, "sync task");
        logger.info("sync task cancel returned in {} ms for delay({})", elapsedMillis, DELAY);

        Exception thrown = expectThrows(Exception.class, () -> {
            try (var ignored = queryFuture.actionGet(PROMPT)) {
                fail("cancel must hard-fail the delay() query, not return a response body");
            }
        });
        assertThat(
            "sync cancel must surface as TaskCancelledException, got: " + thrown,
            ExceptionsHelper.unwrap(thrown, TaskCancelledException.class),
            notNullValue()
        );
        assertNoEsqlTasksRunning();
    }

    /**
     * Cancelling the specific coordinator driver that runs {@code delay()} (its {@link DriverStatus} description is
     * {@code "final"}) must chain up to fail the whole query with {@link TaskCancelledException} within {@link #PROMPT}.
     * Mirrors {@code EsqlActionTaskIT#testCancelMerge}, which cancels the coordinator "final" driver directly.
     */
    public void testCancelCoordinatorDelayDriverPromptly() throws Exception {
        String marker = "delay_driver_cancel_" + System.nanoTime();
        ActionFuture<EsqlQueryResponse> queryFuture = submitSyncBlockingDelayQuery(marker);
        assertThat("the EsqlQueryAction task must be present", findEsqlQueryTask(marker), notNullValue());
        TaskId driverTaskId = awaitCoordinatorDelayDriver();

        long elapsedMillis = cancelAndWait(driverTaskId, "coordinator driver");
        logger.info("coordinator driver cancel returned in {} ms for delay({})", elapsedMillis, DELAY);

        Exception thrown = expectThrows(Exception.class, () -> {
            try (var ignored = queryFuture.actionGet(PROMPT)) {
                fail("cancelling the delay() driver must hard-fail the query");
            }
        });
        assertThat(
            "cancelling the coordinator delay driver must surface as TaskCancelledException, got: " + thrown,
            ExceptionsHelper.unwrap(thrown, TaskCancelledException.class),
            notNullValue()
        );
        assertNoEsqlTasksRunning();
    }

    /**
     * Async hard cancel via {@code _tasks/{id}/_cancel} on the async task (not DELETE) must wind the blocked delay
     * driver down within {@link #PROMPT}, after which the stored result surfaces a cancel-side failure. Mirrors
     * {@code AsyncEsqlQueryActionIT#testGetAsyncWhileQueryTaskIsBeingCancelled} and
     * {@code ExternalAsyncStopAndCancelIT#testAsyncTaskCancelHardFailsWithNoRows}.
     */
    public void testAsyncTaskCancelCancelsBlockingDelayPromptly() throws Exception {
        final String asyncId = submitBlockingDelayQuery();
        awaitDelayDriverSleeping();
        TaskId asyncTaskId = AsyncExecutionId.decode(asyncId).getTaskId();
        assertTrue("the async task must be alive before cancel", isTaskRunning(asyncTaskId));

        long elapsedMillis = cancelAndWait(asyncTaskId, "async task");
        logger.info("async task cancel returned in {} ms for delay({})", elapsedMillis, DELAY);

        try {
            // After cancel (waitForCompletion=true), the stored async result surfaces a cancel-side failure: either
            // TaskCancelledException reached the stored response, or the cancel cascade wiped the entry before the GET.
            Exception thrown = expectThrows(Exception.class, () -> { getAsyncResponse(client(), asyncId).close(); });
            boolean cancelled = ExceptionsHelper.unwrap(thrown, TaskCancelledException.class) != null;
            boolean notFound = ExceptionsHelper.unwrap(thrown, ResourceNotFoundException.class) != null;
            assertTrue(
                "async cancel must surface as TaskCancelledException or ResourceNotFoundException, got: " + thrown,
                cancelled || notFound
            );
        } finally {
            // best-effort cleanup if the cancel cascade did not already wipe the stored entry
            try {
                EsqlAsyncTestUtils.deleteAsyncId(client(), asyncId);
            } catch (Exception ignored) {}
        }
    }

    private String submitBlockingDelayQuery() {
        EsqlQueryRequest request = asyncEsqlQueryRequest("FROM test | LIMIT 1 | EVAL d = delay(" + DELAY.seconds() + "s)")
            .waitForCompletionTimeout(timeValueMillis(100))
            .keepOnCompletion(true)
            .keepAlive(timeValueMinutes(10));
        try (EsqlQueryResponse resp = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)) {
            assertTrue("query should still be running", resp.isRunning());
            assertTrue(resp.asyncExecutionId().isPresent());
            return resp.asyncExecutionId().get();
        }
    }

    private ActionFuture<EsqlQueryResponse> submitSyncBlockingDelayQuery(String marker) {
        // The marker rides in a line comment so it shows up in the task description, letting findEsqlQueryTask latch
        // onto this exact task even if other ES|QL traffic is present (same trick as ExternalAsyncStopAndCancelIT).
        String query = "// " + marker + "\nFROM test | LIMIT 1 | EVAL d = delay(" + DELAY.seconds() + "s)";
        return client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query));
    }

    /**
     * Waits until a compute driver is running, then gives {@code delay()} a moment to enter {@link Thread#sleep}, so the
     * cancel below lands while the coordinator driver is actually blocked (the case the fix targets).
     */
    private void awaitDelayDriverSleeping() throws Exception {
        assertBusy(() -> assertThat(listDriverTasks().size(), greaterThanOrEqualTo(1)));
        safeSleep(1000);
    }

    /**
     * Resolves the coordinator driver that runs {@code delay()} — its {@link DriverStatus} description is
     * {@code "final"} — then gives {@code delay()} a moment to enter {@link Thread#sleep} so the cancel lands mid-sleep.
     */
    private TaskId awaitCoordinatorDelayDriver() throws Exception {
        List<TaskId> found = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> finalDrivers = listDriverTasks().stream()
                .filter(t -> t.status() instanceof DriverStatus ds && ds.description().equals("final"))
                .toList();
            assertThat("the coordinator 'final' driver running delay() must be present", finalDrivers, not(empty()));
            found.clear();
            found.add(finalDrivers.get(0).taskId());
        });
        safeSleep(1000);
        return found.get(0);
    }

    /**
     * Resolves the {@link EsqlQueryAction} task carrying {@code marker} in its description (the query text). Retries
     * via {@code assertBusy} because the task appears a beat after the request is dispatched.
     */
    private TaskId findEsqlQueryTask(String marker) throws Exception {
        List<TaskId> found = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> matched = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.description() != null && t.description().contains(marker))
                .toList();
            assertThat(matched, not(empty()));
            found.clear();
            found.add(matched.get(0).taskId());
        });
        return found.get(0);
    }

    /**
     * Cancels {@code taskId} with {@code waitForCompletion=true} (so the call genuinely blocks until the driver winds
     * down) and returns how long that took. Converts the {@link #PROMPT} timeout into a clear regression failure.
     */
    private long cancelAndWait(TaskId taskId, String what) {
        long start = System.nanoTime();
        CancelTasksRequest cancel = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        cancel.setWaitForCompletion(true);
        try {
            client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancel).actionGet(PROMPT);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError(
                what
                    + " cancel did not return within "
                    + PROMPT
                    + " for a delay("
                    + DELAY
                    + ") query — the coordinator delay driver blocked cancellation instead of observing it between "
                    + "sleep slices (fix regressed)",
                e
            );
        }
        return TimeValue.nsecToMSec(System.nanoTime() - start);
    }

    private List<TaskInfo> listDriverTasks() {
        return client().admin().cluster().prepareListTasks().setActions(DriverTaskRunner.ACTION_NAME).setDetailed(true).get().getTasks();
    }

    private void assertNoEsqlTasksRunning() throws Exception {
        assertBusy(
            () -> assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks(),
                emptyIterable()
            )
        );
    }

    private boolean isTaskRunning(TaskId taskId) {
        return client().admin().cluster().prepareListTasks().setTargetTaskId(taskId).get().getTasks().isEmpty() == false;
    }

    private static int countRows(EsqlQueryResponse response) {
        int rows = 0;
        Iterator<Iterator<Object>> values = response.values();
        while (values.hasNext()) {
            Iterator<Object> row = values.next();
            while (row.hasNext()) {
                row.next();
            }
            rows++;
        }
        return rows;
    }
}
