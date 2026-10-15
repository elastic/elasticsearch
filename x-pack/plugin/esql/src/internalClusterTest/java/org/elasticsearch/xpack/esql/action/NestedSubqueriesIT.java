/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Cluster tests for nested {@code FROM} subqueries under {@code org.elasticsearch.xpack.esql.plugin.SubPlansExecutor}. {@link SubqueryIT}
 * checks that nested unions return the correct results; this suite checks that later branches stay undispatched when the query no longer
 * needs them.
 * <p>
 * Each test uses {@code branch_parallel_degree = 1} (or {@code 2} for the reaper case) so one outer leaf starts while nested leaves wait
 * for a permit. Branches that must not run read the pausable {@code pause_me} field; {@code scriptWaits} is the proof they never started.
 * LIMIT and STOP finish the root {@code LocalExchange} without cancelling the root task, so the next depth-first search visit must skip
 * those leaves as success. Cancel must fail the query instead. The inactive-sink reaper must not drop a leaf that has not been dispatched
 * yet: those leaves have no {@code ExchangeSinkHandler}, only a dummy sink on the parent {@code LocalExchange}.
 */
public class NestedSubqueriesIT extends AbstractPausableIntegTestCase {

    private static final TimeValue INACTIVE_SINK_INTERVAL = TimeValue.timeValueSeconds(1);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // InternalExchangePlugin registers esql.exchange.sink_inactive_interval as a node setting.
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalExchangePlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, INACTIVE_SINK_INTERVAL)
            .build();
    }

    @Override
    protected int numberOfDocs() {
        // Every document evaluation of pause_me consumes a permit, so keep the index small enough to release them all cheaply.
        return 10;
    }

    @Before
    public void requirePragma() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
    }

    @Before
    public void resetPausePermits() {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
    }

    /**
     * A branch waiting for a runner permit must still contribute its rows once it finally runs.
     * <p>
     * {@link ExchangeService} reaps any {@code ExchangeSinkHandler} that has no producer attached and has not been touched
     * for {@code esql.exchange.sink_inactive_interval}. A leaf that has not been dispatched must not register such a handler:
     * {@code SubPlansExecutor} only opens a dummy sink on the parent {@code LocalExchange} at tree-build time, and the data-node sink is
     * created later inside {@code executePlan}. Reaping a handler that was registered too early finishes the buffer, the parked fetch is
     * answered with "finished, no pages", and the branch contributes nothing — no failure, no partial-results flag.
     * <p>
     * The query below is a nested union under one {@code SubPlansExecutor}. With {@code branch_parallel_degree = 2} the outer branch and
     * the first inner branch take both permits and block on {@code pause_me}; the second inner branch reads only {@code foo} and is left
     * undispatched, for as long as this test cares to hold it. All three branches count the same 10 documents, so a branch that lost its
     * sink would show up as a count of 20 rather than 30.
     */
    public void testQueuedBranchOutlivesInactiveSinkReaper() throws Exception {
        String query = """
            FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo),
                 (FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo),
                       (FROM test | KEEP foo))
            | STATS count = COUNT(*)
            """;
        var request = syncEsqlQueryRequest(query).pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", 2).build()));

        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);
        try {
            // Wait until two branches are actually blocked inside pause_me, which means both runner permits are held and the third
            // branch is still waiting for a permit.
            assertBusy(() -> assertThat(scriptWaits.availablePermits(), greaterThanOrEqualTo(2)), 30, TimeUnit.SECONDS);

            safeSleep(INACTIVE_SINK_INTERVAL.millis() * 4);

            // Let everything through. Each document evaluation of pause_me needs one permit, across all branches.
            scriptPermits.release(numberOfDocs() * 2);

            try (EsqlQueryResponse response = future.actionGet(60, TimeUnit.SECONDS)) {
                // Three branches over the same index: the undispatched branch must still have contributed its rows.
                assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs() * 3))));
            }
        } finally {
            scriptPermits.release(numberOfDocs() * 10);
            if (future.isDone() == false) {
                future.cancel(true);
            }
        }
    }

    /**
     * A nested merge branch that has not started yet must be skipped once the query already has enough rows.
     * <p>
     * When the root {@code LimitExec} has enough pages it finishes the root {@code LocalExchange}. The next {@code tryExecuteLeaves} visit
     * sees that the parent exchange is finished and completes the nested merge without {@code runCompute}, then skips its leaves without
     * {@code executePlan}. Without that check, an unstarted nested union would start a coordinator merge driver and run its own branches
     * to produce rows that nobody will read.
     * <p>
     * {@code branch_parallel_degree = 1} starts only the first outer branch, and that branch reads just {@code foo}, so {@code LIMIT 1} is
     * satisfied while the nested union is still waiting for the single permit. The nested branches do read {@code pause_me} and no permits
     * have been released at that point, so if the merge were started they would block in the pause script and raise {@code scriptWaits}.
     * <p>
     * Each branch tags its rows with its own name so that the one row {@code LIMIT 1} keeps says which branch produced it. {@code foo} is
     * the document id, so its value is whichever document the outer branch happened to emit first.
     */
    public void testLimitSkipsUnstartedNestedMergeAndQueuedSiblingLeaves() {
        String query = """
            FROM (FROM test | KEEP foo | EVAL branch = "outer"),
                 (FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo | EVAL branch = "nested-paused"),
                       (FROM test | KEEP foo | EVAL branch = "nested-plain"))
            | LIMIT 1
            | KEEP branch, foo
            """;
        var request = syncEsqlQueryRequest(query).pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()));

        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);
        try (EsqlQueryResponse response = future.actionGet(30, TimeUnit.SECONDS)) {
            assertColumnNames(response.columns(), List.of("branch", "foo"));
            assertColumnTypes(response.columns(), List.of("keyword", "long"));
            // Skipping branches is not a partial result: the query ran to completion, it just did not need every branch.
            assertFalse(response.isPartial());

            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            // Only the first outer branch ever ran, so the surviving row has to be one of its documents.
            assertThat(values.get(0).get(0), equalTo("outer"));
            assertThat((Long) values.get(0).get(1), allOf(greaterThanOrEqualTo(0L), lessThan((long) numberOfDocs())));

            // The nested branches never ran: reaching pause_me would have raised scriptWaits.
            assertThat(scriptWaits.availablePermits(), equalTo(0));
        } finally {
            // Unblock anything that did reach pause_me, so a failing run tears the cluster down instead of hanging.
            scriptPermits.release(numberOfDocs() * 10);
            if (future.isDone() == false) {
                future.cancel(true);
            }
        }
    }

    /**
     * Cancelling the query through the tasks API while a nested merge is still unstarted must fail the whole query and wind every task
     * down — the sync counterpart of {@code AsyncEsqlQueryActionIT}'s delete test. Cancellation reaches the branch machinery through the
     * root {@code CancellableTask}: {@code start()} / {@code startLeaf} see {@code rootTask.isCancelled()} and fail without
     * {@code runCompute} / {@code executePlan}. If the nested merge started anyway, its paused branch would evaluate {@code pause_me}
     * and raise {@code scriptWaits} past what the single outer branch can produce.
     * <p>
     * {@code branch_parallel_degree = 1}: the first outer branch takes the only permit and blocks on {@code pause_me}; the nested union
     * waits unstarted. The cancel lands while the query is in that state, so the failure is recorded before the outer branch completes
     * and before the nested merge is ever considered.
     */
    public void testSyncCancellationSkipsUnstartedNestedMerge() throws Exception {
        String query = """
            FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo),
                 (FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo),
                       (FROM test | KEEP foo))
            | STATS count = COUNT(*)
            """;
        var request = syncEsqlQueryRequest(query).pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()));

        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);
        try {
            // The outer branch is inside the pause script, holding the only permit; the nested union is unstarted.
            assertTrue("the first outer branch must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.INSTANCE.name())
                .get()
                .getTasks();
            assertThat(tasks, hasSize(1));
            CancelTasksRequest cancelRequest = new CancelTasksRequest().setTargetTaskId(tasks.getFirst().taskId())
                .setReason("test sync cancel");
            cancelRequest.setWaitForCompletion(false);
            client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancelRequest).actionGet();

            // Let the blocked script return so the cancelled driver can observe the cancellation and unwind.
            scriptPermits.release(numberOfDocs() * 10);

            Exception e = expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
            assertNotNull("expected a task cancellation in the cause chain", ExceptionsHelper.unwrap(e, TaskCancelledException.class));

            // The nested branches never ran: only the outer branch's documents can have reached pause_me.
            assertThat(scriptWaits.availablePermits(), lessThanOrEqualTo(numberOfDocs()));

            // Nothing lingers: the query task and all driver tasks ("indices:data/read/esql*") wind down.
            assertBusy(() -> {
                List<TaskInfo> remaining = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.INSTANCE.name() + "*")
                    .get()
                    .getTasks();
                assertThat(remaining, empty());
            });
        } finally {
            scriptPermits.release(numberOfDocs() * 10);
            if (future.isDone() == false) {
                future.cancel(true);
            }
        }
    }

    /**
     * Async STOP while the outer leaf is blocked on {@code pause_me} and the nested leaves are still waiting for a permit. STOP marks
     * {@code EsqlExecutionInfo} stopped and finishes the root {@code LocalExchange}; the next depth-first search visit must skip those
     * nested leaves.
     */
    public void testStopSkipsQueuedNestedLeaves() throws Exception {
        String query = """
            FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo | EVAL branch = "outer"),
                 (FROM (FROM test | WHERE pause_me IS NOT NULL | KEEP foo | EVAL branch = "nested-paused"),
                       (FROM test | KEEP foo | EVAL branch = "nested-plain"))
            | KEEP branch, foo
            """;
        var request = asyncEsqlQueryRequest(query).pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()))
            .waitForCompletionTimeout(TimeValue.timeValueNanos(1))
            .keepOnCompletion(true)
            .keepAlive(TimeValue.timeValueMinutes(5));

        try (EsqlQueryResponse initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initialResponse.isRunning(), is(true));
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertTrue("the first outer branch must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));

            var stopFuture = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(initialResponse.asyncExecutionId().get()));
            scriptPermits.release(numberOfDocs() * 10);
            try (EsqlQueryResponse stoppedResponse = stopFuture.actionGet(60, TimeUnit.SECONDS)) {
                assertThat(stoppedResponse.isRunning(), is(false));
                assertThat(stoppedResponse.isPartial(), is(true));

                for (List<Object> row : getValuesList(stoppedResponse)) {
                    assertThat(row.get(0), equalTo("outer"));
                }
                // The nested pause_me leaf never ran: only the outer branch's documents can have reached the script.
                assertThat(scriptWaits.availablePermits(), lessThanOrEqualTo(numberOfDocs()));
            }

            assertBusy(() -> {
                List<TaskInfo> remaining = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.INSTANCE.name() + "*")
                    .get()
                    .getTasks();
                assertThat(remaining, empty());
            });
        } finally {
            scriptPermits.release(numberOfDocs() * 10);
        }
    }
}
