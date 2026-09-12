/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.plugin.EsqlQueryStatus;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.core.IsEqual;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Individual tests for specific aspects of the async query API.
 */
public class AsyncEsqlQueryActionIT extends AbstractPausableIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> actions = new ArrayList<>(super.nodePlugins());
        actions.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        actions.add(InternalExchangePlugin.class);
        return Collections.unmodifiableList(actions);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    public void testBasicAsyncExecution() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();

            if (randomBoolean()) {
                // let's timeout first
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueMillis(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var responseWithTimeout = future.get()) {
                    assertThat(initialResponse.asyncExecutionId(), isPresent());
                    assertThat(responseWithTimeout.asyncExecutionId().get(), equalTo(id));
                    assertThat(responseWithTimeout.isRunning(), is(true));
                }
            }

            // Now we wait
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            getResultsRequest.setKeepAlive(randomKeepAlive());
            var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);

            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());

            try (var finalResponse = future.get()) {
                assertThat(finalResponse, notNullValue());
                assertThat(finalResponse.isRunning(), is(false));
                assertThat(finalResponse.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                assertThat(getValuesList(finalResponse).size(), equalTo(1));
            }

            // Get the stored result (again)
            var again = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
            try (var finalResponse = again.get()) {
                assertThat(finalResponse, notNullValue());
                assertThat(finalResponse.isRunning(), is(false));
                assertThat(finalResponse.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                assertThat(getValuesList(finalResponse).size(), equalTo(1));
            }

            AcknowledgedResponse deleteResponse = deleteAsyncId(id);
            assertThat(deleteResponse.isAcknowledged(), equalTo(true));
            // the stored response should no longer be retrievable
            var e = expectThrows(ResourceNotFoundException.class, () -> deleteAsyncId(id));
            assertThat(e.getMessage(), IsEqual.equalTo(id));
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testGetAsyncWhileQueryTaskIsBeingCancelled() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();
            // ensure we have started Lucene operators
            assertBusy(() -> {
                var tasks = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.description().contains("_LuceneSourceOperator"))
                    .toList();
                assertThat(tasks.size(), greaterThanOrEqualTo(1));
            });
            client().admin()
                .cluster()
                .prepareCancelTasks()
                .setActions(EsqlQueryAction.NAME + AsyncTaskManagementService.ASYNC_ACTION_SUFFIX)
                .get();
            assertBusy(() -> {
                List<TaskInfo> tasks = getEsqlQueryTasks().stream().filter(TaskInfo::cancelled).toList();
                assertThat(tasks, not(empty()));
            });
            // get the result while the query is being cancelled
            {
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueMillis(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var resp = future.get()) {
                    assertThat(initialResponse.asyncExecutionId(), isPresent());
                    assertThat(resp.asyncExecutionId().get(), equalTo(id));
                    assertThat(resp.isRunning(), is(true));
                }
            }
            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());
            // get the result after the cancellation is done
            {
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                TaskCancelledException error = expectThrows(TaskCancelledException.class, future::actionGet);
                assertThat(error.getMessage(), equalTo("by user request"));
            }
            assertTrue(deleteAsyncId(id).isAcknowledged());
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testAsyncCancellation() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();

            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            var future = client().execute(TransportDeleteAsyncResultAction.TYPE, request);

            // there should be just one task
            List<TaskInfo> tasks = getEsqlQueryTasks();
            assertThat(tasks.size(), is(1));

            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());

            var deleteResponse = future.actionGet(timeValueSeconds(60));
            assertThat(deleteResponse.isAcknowledged(), equalTo(true));

            // there should be no tasks after delete
            tasks = getEsqlQueryTasks();
            assertThat(tasks.size(), is(0));

            // the stored response should no longer be retrievable
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setKeepAlive(timeValueMinutes(10));
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            var e = expectThrows(
                ResourceNotFoundException.class,
                () -> client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo(id));
        } finally {
            scriptPermits.drainPermits();
        }
    }

    /**
     * Verifies that async stop works while nested subqueries are still queued: with {@code branch_parallel_degree=1} only the first
     * leaf subquery runs (paused on the script) and the nested branches have not started yet, so the stop must both halt the running
     * leaf and drain the queued ones, returning a partial, non-running result.
     * <p>
     * {@code branch_parallel_degree=1} is the only pragma this and the following nested-subquery termination tests need: it
     * deterministically forces the nested branches into the queued state regardless of the pragma's default. Unlike
     * {@link #testAsyncStopNestedSubqueryHonorsGlobalParallelismWithBranchParallelDegreeTwo}, these tests do not count paused
     * drivers — they only need to know the query reached the pausable script (a single {@code scriptWaits} acquire), which holds
     * no matter how many slices a leaf is split into, so {@code data_partitioning} need not be forced to {@code shard}. Script
     * permits are consumed per document rather than per page, so {@code page_size} is irrelevant as well.
     */
    public void testStopNestedSubqueryWithBranchParallelDegreeOne() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM test | STATS total = SUM(pause_me) ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas).waitForCompletionTimeout(TimeValue.timeValueNanos(1)).keepOnCompletion(true).keepAlive(randomKeepAlive());

        try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertTrue("a nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            var stopFuture = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(initialResponse.asyncExecutionId().get()));
            scriptPermits.release(numberOfDocs() * 3);
            try (var stoppedResponse = stopFuture.actionGet(60, TimeUnit.SECONDS)) {
                assertThat(stoppedResponse.isRunning(), is(false));
                assertThat(stoppedResponse.isPartial(), is(true));
            }
        } finally {
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    /**
     * Verifies that cancelling the async query task through the cancel tasks API terminates a query whose nested subqueries are still
     * queued behind {@code branch_parallel_degree=1}: the cancellation must propagate to the running leaf and the queued branches, and
     * fetching the async result afterwards must throw {@link TaskCancelledException}.
     * See {@link #testStopNestedSubqueryWithBranchParallelDegreeOne} for why {@code branch_parallel_degree=1} is the only pragma these
     * tests set.
     */
    public void testCancelNestedSubqueryWithBranchParallelDegreeOne() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM test | STATS total = SUM(pause_me) ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas).waitForCompletionTimeout(TimeValue.timeValueNanos(1)).keepOnCompletion(true).keepAlive(randomKeepAlive());

        try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            String id = initialResponse.asyncExecutionId().get();
            assertTrue("a nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            List<TaskInfo> queryTasks = getEsqlQueryTasks();
            assertThat(queryTasks, hasSize(1));
            client().admin()
                .cluster()
                .execute(
                    TransportCancelTasksAction.TYPE,
                    new CancelTasksRequest().setTargetTaskId(queryTasks.get(0).taskId()).setReason("test cancel")
                )
                .actionGet();
            scriptPermits.release(numberOfDocs() * 3);

            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            getResultsRequest.setKeepAlive(randomKeepAlive());
            expectThrows(
                TaskCancelledException.class,
                () -> client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet()
            );
        } finally {
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    /**
     * Verifies that deleting the async result while one leaf is paused and the nested subqueries are queued behind
     * {@code branch_parallel_degree=1} cancels the whole query: the delete must be acknowledged, the queued branches drained so no
     * ES|QL tasks linger, and the stored response removed so a later get throws {@link ResourceNotFoundException}. See
     * {@link #testStopNestedSubqueryWithBranchParallelDegreeOne} for why {@code branch_parallel_degree=1} is the only pragma these tests
     * set.
     */
    public void testDeleteNestedSubqueryWithBranchParallelDegreeOne() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM test | STATS total = SUM(pause_me) ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas).waitForCompletionTimeout(TimeValue.timeValueNanos(1)).keepOnCompletion(true).keepAlive(randomKeepAlive());

        try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            String id = initialResponse.asyncExecutionId().get();
            assertTrue("a nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            var deleteFuture = client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id));
            scriptPermits.release(numberOfDocs() * 3);

            assertThat(deleteFuture.actionGet(timeValueSeconds(60)).isAcknowledged(), equalTo(true));

            // no tasks should remain after deletion
            assertBusy(() -> assertThat(getEsqlQueryTasks(), hasSize(0)));

            // the stored result must be gone
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setKeepAlive(timeValueMinutes(10));
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            var e = expectThrows(
                ResourceNotFoundException.class,
                () -> client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo(id));
        } finally {
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    /**
     * Verifies that keep-alive expiry cancels a query with queued nested subqueries: while one leaf is paused behind
     * {@code branch_parallel_degree=1}, the keep-alive is shortened to a few milliseconds so the async reaper kicks in. All started
     * drivers and the async query task itself must be cancelled, and a subsequent get of the result must fail with
     * "keep_alive expired". See {@link #testStopNestedSubqueryWithBranchParallelDegreeOne} for why {@code branch_parallel_degree=1} is
     * the only pragma these tests set.
     */
    public void testKeepAliveExpiryNestedSubqueryWithBranchParallelDegreeOne() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM test | STATS total = SUM(pause_me) ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas)
            .waitForCompletionTimeout(TimeValue.timeValueNanos(1))
            .keepOnCompletion(randomBoolean())
            .allowPartialResults(false)
            .keepAlive(TimeValue.timeValueMinutes(between(1, 5)));
        final String asyncId;
        try {
            try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
                assertThat(initialResponse.isRunning(), is(true));
                assertThat(initialResponse.asyncExecutionId(), isPresent());
                asyncId = initialResponse.asyncExecutionId().get();
            }
            assertTrue("a nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));
            // Shorten the keepAlive to a tiny value so the reaper cancels the query quickly
            var getRequest = new GetAsyncResultRequest(asyncId).setWaitForCompletionTimeout(timeValueMillis(between(1, 10)))
                .setKeepAlive(timeValueMillis(randomIntBetween(1, 100)));
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertTrue(resp.isRunning());
            }
            // all started drivers are cancelled once the keepAlive expires
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks();
                for (TaskInfo task : tasks) {
                    assertTrue(task.cancelled());
                }
            });
            // the async task itself is cancelled
            assertBusy(() -> {
                List<TaskInfo> queryTasks = getEsqlQueryTasks();
                assertThat(queryTasks, hasSize(1));
                assertTrue(queryTasks.get(0).cancelled());
            });
        } finally {
            scriptPermits.release(numberOfDocs() * 3);
            scriptWaits.drainPermits();
        }
        TaskCancelledException error = expectThrows(TaskCancelledException.class, () -> {
            var getRequest = new GetAsyncResultRequest(asyncId).setWaitForCompletionTimeout(timeValueSeconds(10))
                .setKeepAlive(timeValueSeconds(30));
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
        });
        assertThat(error.getMessage(), containsString("keep_alive expired"));
    }

    /**
     * Verifies that {@code branch_parallel_degree} is enforced globally across nesting levels rather than per level. The query has four
     * leaf subqueries spread over two nested FROM branches; with {@code branch_parallel_degree=2} exactly two leaves may start executing
     * (observed via the pausable script) while the rest stay queued. It then issues an async stop while the leaves are paused and expects
     * a partial, non-running result.
     */
    public void testAsyncStopNestedSubqueryHonorsGlobalParallelismWithBranchParallelDegreeTwo() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(
            Settings.builder()
                // Force shard partitioning so each leaf subquery runs as exactly one driver (the index has a single shard). The
                // semaphore accounting below counts one scriptWaits permit per paused driver, so finer partitioning that splits a
                // leaf into several slices/drivers would break the "exactly two waits" logic.
                .put("data_partitioning", "shard")
                // Small page size for consistency with the other tests in this file; the pause script blocks per document, so this
                // test does not depend on it.
                .put("page_size", pageSize())
                // Matches the current default, but every assertion below depends on the value being exactly 2, so pin it explicitly
                // in case the default changes.
                .put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 2)
                .build()
        );
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas).waitForCompletionTimeout(TimeValue.timeValueNanos(1)).keepOnCompletion(true).keepAlive(randomKeepAlive());

        try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertTrue("the first nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));
            assertTrue("the second nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));
            assertFalse(
                "branch_parallel_degree=2 must keep the other nested leaves queued",
                scriptWaits.tryAcquire(200, TimeUnit.MILLISECONDS)
            );

            var stopFuture = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(initialResponse.asyncExecutionId().get()));
            scriptPermits.release(numberOfDocs() * 4);
            try (var stoppedResponse = stopFuture.actionGet(60, TimeUnit.SECONDS)) {
                assertThat(stoppedResponse.isRunning(), is(false));
                assertThat(stoppedResponse.isPartial(), is(true));
                assertThat(stoppedResponse.columns(), equalTo(List.of(new ColumnInfoImpl("total", "long", null))));
                // The stop races with the released leaves, so anywhere from none to all four branches may contribute a row. A
                // branch that is stopped mid-aggregation may emit a partial sum (or null if it processed nothing); a branch that
                // completed sums pause_me=1 over all docs, so no value can exceed numberOfDocs().
                List<List<Object>> rows = getValuesList(stoppedResponse);
                assertThat(rows.size(), lessThanOrEqualTo(4));
                for (List<Object> row : rows) {
                    assertThat(row, hasSize(1));
                    if (row.get(0) != null) {
                        assertThat((long) row.get(0), greaterThanOrEqualTo(0L));
                        assertThat((long) row.get(0), lessThanOrEqualTo((long) numberOfDocs()));
                    }
                }
            }
        } finally {
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    /**
     * Verifies that cancelling an async query (via delete) also drains nested subqueries that are still queued behind the parallelism
     * limit. With {@code branch_parallel_degree=1} only one leaf subquery runs while the nested ones wait in the queue; deleting the async
     * result while the running leaf is paused must be acknowledged and leave no lingering ES|QL tasks, i.e. the queued nested branches
     * must not start or leak.
     */
    public void testAsyncCancellationDrainsQueuedNestedSubqueriesWithBranchParallelDegreeOne() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        var pragmas = new QueryPragmas(
            Settings.builder()
                // Force shard partitioning so each leaf subquery runs as exactly one driver (the index has a single shard),
                // keeping the scriptWaits accounting deterministic.
                .put("data_partitioning", "shard")
                // Small page size for consistency with the other tests in this file; the pause script blocks per document, so this
                // test does not depend on it.
                .put("page_size", pageSize())
                // Only one leaf may run at a time, so the delete below must drain the nested subqueries still waiting in the queue.
                .put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1)
                .build()
        );
        var request = asyncEsqlQueryRequest("""
            FROM
               ( FROM test | STATS total = SUM(pause_me) ),
               ( FROM
                    ( FROM test | STATS total = SUM(pause_me) ),
                    ( FROM test | STATS total = SUM(pause_me) )
               )
            """).pragmas(pragmas).waitForCompletionTimeout(TimeValue.timeValueNanos(1)).keepOnCompletion(true).keepAlive(randomKeepAlive());

        try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertTrue("a nested leaf must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            String id = initialResponse.asyncExecutionId().get();
            var deleteFuture = client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id));
            scriptPermits.release(numberOfDocs() * 3);
            assertThat(deleteFuture.actionGet(timeValueSeconds(60)).isAcknowledged(), equalTo(true));
            assertThat(getEsqlQueryTasks(), empty());

            // Deleting cancels the query and discards its results, so no query output can be validated: the stored response must
            // no longer be retrievable.
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            var e = expectThrows(
                ResourceNotFoundException.class,
                () -> client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo(id));
        } finally {
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    public void testFinishingBeforeTimeoutKeep() {
        testFinishingBeforeTimeout(true);
    }

    public void testFinishingBeforeTimeoutDoNotKeep() {
        testFinishingBeforeTimeout(false);
    }

    private void testFinishingBeforeTimeout(boolean keepOnCompletion) {
        // don't block the query execution at all
        scriptPermits.drainPermits();
        assert scriptPermits.availablePermits() == 0;

        scriptPermits.release(numberOfDocs());

        var request = asyncEsqlQueryRequest("from test | stats sum(pause_me)").pragmas(queryPragmas())
            .waitForCompletionTimeout(TimeValue.timeValueSeconds(60))
            .keepOnCompletion(keepOnCompletion)
            .keepAlive(randomKeepAlive());

        try (var response = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(response.isRunning(), is(false));
            assertThat(response.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
            assertThat(getValuesList(response).size(), equalTo(1));

            if (keepOnCompletion) {
                assertThat(response.asyncExecutionId(), isPresent());
                // we should be able to retrieve the response by id, since it has been kept
                String id = response.asyncExecutionId().get();
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var resp = future.actionGet(60, TimeUnit.SECONDS)) {
                    assertThat(resp.asyncExecutionId().get(), equalTo(id));
                    assertThat(resp.isRunning(), is(false));
                    assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                    assertThat(getValuesList(resp).size(), equalTo(1));
                }
            } else {
                assertThat(response.asyncExecutionId(), isEmpty());
            }
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testUpdateKeepAlive() throws Exception {
        long nowInMillis = System.currentTimeMillis();
        TimeValue keepAlive = timeValueSeconds(between(30, 60));
        var request = asyncEsqlQueryRequest("from test | stats sum(pause_me)").pragmas(queryPragmas())
            .waitForCompletionTimeout(TimeValue.timeValueMillis(between(1, 10)))
            .keepOnCompletion(randomBoolean())
            .keepAlive(keepAlive);
        final String asyncId;
        long currentExpiration;
        scriptPermits.drainPermits();
        try {
            try (EsqlQueryResponse initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
                assertThat(initialResponse.isRunning(), is(true));
                assertTrue(initialResponse.asyncExecutionId().isPresent());
                asyncId = initialResponse.asyncExecutionId().get();
            }
            currentExpiration = getExpirationFromTask(asyncId);
            assertThat(currentExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
            assertThat(getTaskKeepAlive(asyncId), equalTo(keepAlive.getStringRep()));
            // update the expiration while the task is still running
            int iters = iterations(1, 5);
            for (int i = 0; i < iters; i++) {
                long extraKeepAlive = randomIntBetween(30, 60);
                keepAlive = TimeValue.timeValueSeconds(keepAlive.seconds() + extraKeepAlive);
                GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId).setKeepAlive(keepAlive);
                try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                    assertThat(resp.asyncExecutionId(), isPresent());
                    assertThat(resp.asyncExecutionId().get(), equalTo(asyncId));
                    assertTrue(resp.isRunning());
                }
                long updatedExpiration = getExpirationFromTask(asyncId);
                assertThat(updatedExpiration, greaterThanOrEqualTo(currentExpiration + extraKeepAlive));
                assertThat(updatedExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
                assertThat(getTaskKeepAlive(asyncId), equalTo(keepAlive.getStringRep()));
                currentExpiration = updatedExpiration;
            }
        } finally {
            scriptPermits.release(numberOfDocs());
        }
        // allow the query to complete, then update the expiration with the result is being stored in the async index
        assertBusy(() -> {
            GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId);
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
        });
        assertThat(getExpirationFromDoc(asyncId), greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
        // update the keepAlive after the query has completed
        int iters = between(1, 5);
        for (int i = 0; i < iters; i++) {
            long extraKeepAlive = randomIntBetween(30, 60);
            keepAlive = TimeValue.timeValueSeconds(keepAlive.seconds() + extraKeepAlive);
            GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId).setKeepAlive(keepAlive);
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
            long updatedExpiration = getExpirationFromDoc(asyncId);
            assertThat(updatedExpiration, greaterThanOrEqualTo(currentExpiration + extraKeepAlive));
            assertThat(updatedExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
            currentExpiration = updatedExpiration;
        }
    }

    public void testCancelOnExpiry() throws Exception {
        var request = asyncEsqlQueryRequest("from test | stats sum(pause_me)").pragmas(queryPragmas())
            // small interval so that we can return quickly on submission
            .waitForCompletionTimeout(TimeValue.timeValueMillis(between(1, 10)))
            .keepOnCompletion(randomBoolean())
            .allowPartialResults(false)
            // large interval so that the tasks won't be cancelled until it has started
            .keepAlive(TimeValue.timeValueMinutes(between(1, 5)));
        final String asyncId;
        scriptPermits.drainPermits();
        try {
            try (EsqlQueryResponse initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
                assertThat(initialResponse.isRunning(), is(true));
                assertTrue(initialResponse.asyncExecutionId().isPresent());
                asyncId = initialResponse.asyncExecutionId().get();
            }
            // make sure at least one data node driver has started
            assertBusy(() -> {
                List<TaskInfo> driverTasks = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .stream()
                    .filter(d -> d.status().toString().contains("Lucene"))
                    .toList();
                assertThat(driverTasks, not(empty()));
                for (TaskInfo driveTask : driverTasks) {
                    assertFalse(driveTask.cancelled());
                }
            });
            var getRequest = new GetAsyncResultRequest(asyncId).setWaitForCompletionTimeout(TimeValue.timeValueMillis(between(1, 10)))
                .setKeepAlive(timeValueMillis(randomIntBetween(1, 100)));
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertTrue(resp.isRunning());
            }
            // all the started drivers were canceled
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks();
                for (TaskInfo task : tasks) {
                    assertTrue(task.cancelled());
                }
            });
            // the async task was canceled
            assertBusy(() -> {
                List<TaskInfo> queryTasks = getEsqlQueryTasks();
                assertThat(queryTasks, hasSize(1));
                assertTrue(queryTasks.get(0).cancelled());
            });
        } finally {
            scriptPermits.release(numberOfDocs());
        }
        TaskCancelledException error = expectThrows(TaskCancelledException.class, () -> {
            var getRequest = new GetAsyncResultRequest(asyncId).setWaitForCompletionTimeout(timeValueSeconds(10))
                .setKeepAlive(timeValueSeconds(30));
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
        });
        assertThat(error.getMessage(), containsString("keep_alive expired"));
    }

    private static long getExpirationFromTask(String asyncId) {
        List<EsqlQueryTask> tasks = new ArrayList<>();
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            for (CancellableTask task : ts.getTaskManager().getCancellableTasks().values()) {
                if (task instanceof EsqlQueryTask queryTask) {
                    EsqlQueryResponse result = queryTask.getCurrentResult();
                    if (result.isAsync() && result.asyncExecutionId().get().equals(asyncId)) {
                        tasks.add(queryTask);
                    }
                }
            }
        }
        assertThat(tasks, hasSize(1));
        return tasks.getFirst().getExpirationTimeMillis();
    }

    private String getTaskKeepAlive(String asyncId) throws Exception {
        List<TaskInfo> tasks = getEsqlQueryTasks();
        assertThat(tasks, hasSize(1));
        EsqlQueryStatus status = (EsqlQueryStatus) tasks.getFirst().status();
        assertThat(status.id().getEncoded(), equalTo(asyncId));
        return status.keepAlive().getStringRep();
    }

    private static long getExpirationFromDoc(String asyncId) {
        String docId = AsyncExecutionId.decode(asyncId).getDocId();
        GetResponse doc = client().prepareGet().setIndex(XPackPlugin.ASYNC_RESULTS_INDEX).setId(docId).setRealtime(true).get();
        assertTrue(doc.isExists());
        return ((Number) doc.getSource().get(AsyncTaskIndexService.EXPIRATION_TIME_FIELD)).longValue();
    }

    private List<TaskInfo> getEsqlQueryTasks() throws Exception {
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME + "[a]")
                .setDetailed(true)
                .get()
                .getTasks();
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    private EsqlQueryResponse sendAsyncQuery() {
        scriptPermits.drainPermits();
        assert scriptPermits.availablePermits() == 0;

        scriptPermits.release(between(1, 5));
        var pragmas = queryPragmas();
        return client().execute(
            EsqlQueryAction.INSTANCE,
            asyncEsqlQueryRequest("from test | stats sum(pause_me)").pragmas(pragmas)
                // deliberately small timeout, to frequently trigger incomplete response
                .waitForCompletionTimeout(TimeValue.timeValueNanos(randomIntBetween(1, 20)))
                .keepOnCompletion(randomBoolean())
                .keepAlive(randomKeepAlive())
        ).actionGet(60, TimeUnit.SECONDS);
    }

    private QueryPragmas queryPragmas() {
        return new QueryPragmas(
            Settings.builder()
                // Force shard partitioning because that's all the tests know how to match. It is easier to reason about too.
                .put("data_partitioning", "shard")
                // Limit the page size to something small so we do more than one page worth of work, so we get more status updates.
                .put("page_size", pageSize())
                .build()
        );
    }

    private AcknowledgedResponse deleteAsyncId(String id) {
        DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
        return client().execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(timeValueSeconds(60));
    }

    TimeValue randomKeepAlive() {
        return randomTimeValue(1, 5, TimeUnit.DAYS);
    }

    public static class LocalStateEsqlAsync extends LocalStateCompositeXPackPlugin {
        public LocalStateEsqlAsync(final Settings settings, final Path configPath) {
            super(settings, configPath);
        }
    }
}
