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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * How a nested union schedules its branches when some of them are slow, which is where the coordinator-side branch
 * bookkeeping is easiest to get wrong.
 * <p>
 * A nested union has one {@code MergeLevelExecutor} per nesting level, but all of them submit leaves into a single
 * query-wide {@code SubPlanTaskRunner}, so the window of concurrently running branches spans levels: an inner branch can
 * be waiting for a permit that an outer branch is holding. The tests here pin branches in a particular state by making
 * them read the {@code pause_me} runtime field, then assert what the coordinator did with the branches that were
 * <em>not</em> running - a queued leaf in {@link #testQueuedBranchOutlivesInactiveSinkReaper}, an unstarted nested merge
 * in {@link #testLimitSkipsUnstartedNestedMerge}. Both failure modes are silent: the query succeeds, it just returns the
 * wrong rows or does work nobody reads.
 * <p>
 * Correctness of nested-union results at various parallel degrees lives in {@code SubqueryIT}; this class only covers the
 * cases that need a branch to block, and it is configured for them: the inactive-sink reaper interval is turned down to
 * one second so a test can outwait it, and the index is kept small because every document evaluation of {@code pause_me}
 * costs a permit.
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
    public void checkNestedSubquerySupport() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    /**
     * {@code scriptPermits} and {@code scriptWaits} are static on {@link AbstractPausableIntegTestCase} and the cluster is
     * shared across the methods of this class, so permits released by one test would otherwise leak into the next in
     * whatever order the runner picks. A leak here does not fail a test, it defeats it: with spare permits available no
     * branch ever blocks, and a test that depends on a branch staying queued would pass without exercising anything.
     */
    @Before
    public void resetPausePermits() {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
    }

    /**
     * A branch waiting for a runner permit must still contribute its rows once it finally runs.
     * <p>
     * {@link ExchangeService} reaps any sink that has no producer attached and has not been touched for
     * {@code esql.exchange.sink_inactive_interval}, and a sink belonging to a branch that has not started looks exactly
     * like that: the coordinator's fetch parks on an empty, unfinished buffer, so nothing refreshes the sink's timestamp.
     * Reaping finishes the buffer, the parked fetch is answered with "finished, no pages", and the branch contributes
     * nothing - no failure, no partial-results flag. {@code MergeLevelExecutor} avoids this by opening a leaf's sink from
     * {@code SubPlan.execute}, after the runner has dispatched it, rather than when it is submitted.
     * <p>
     * The query below is a nested union, so the outer and inner executors share one runner. With
     * {@code branch_parallel_degree = 2} the outer branch and the first inner branch take both permits and block on
     * {@code pause_me}; the second inner branch reads only {@code foo} and is the one left queued, for as long as this
     * test cares to hold it. All three branches count the same 10 documents, so a branch that lost its sink would show up
     * as a count of 20 rather than 30.
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
            // branch is sitting in the queue.
            assertBusy(() -> assertThat(scriptWaits.availablePermits(), greaterThanOrEqualTo(2)), 30, TimeUnit.SECONDS);

            // Hold that state well past the reaper interval. The reaper runs every half interval, so this gives it several passes
            // over the queued branch.
            safeSleep(INACTIVE_SINK_INTERVAL.millis() * 4);

            // Let everything through. Each document evaluation of pause_me needs one permit, across all branches.
            scriptPermits.release(numberOfDocs() * 10);

            try (EsqlQueryResponse response = future.actionGet(60, TimeUnit.SECONDS)) {
                // Three branches over the same index: the queued branch must still have contributed its rows.
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
     * When the main plan satisfies its {@code LIMIT} it calls {@code SubPlanTaskRunner.finish()}, which skips every leaf
     * still queued. Merge branches never enter that queue - {@code MergeLevelExecutor} expands them itself, on the thread
     * that finished the previous branch - so they need the separate {@code finished()} check in
     * {@code tryExecuteNextSubPlan}. Without it, an unstarted nested union still registers an exchange source, starts a
     * coordinator merge driver and runs its own branches to produce rows that nobody will read.
     * <p>
     * {@code branch_parallel_degree = 1} starts only the first outer branch, and that branch reads just {@code foo}, so
     * {@code LIMIT 1} is satisfied and {@code finish()} runs while the nested union is still waiting for the single
     * permit. The nested branches do read {@code pause_me} and no permits have been released at that point, so if the
     * merge were expanded they would block in the pause script and raise {@code scriptWaits}.
     * <p>
     * Each branch tags its rows with its own name so that the one row {@code LIMIT 1} keeps says which branch produced
     * it. {@code foo} is the document id, so its value is whichever document the outer branch happened to emit first.
     */
    public void testLimitSkipsUnstartedNestedMerge() {
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
     * Cancelling the query through the tasks API while a nested merge is still unstarted must fail the whole query and
     * wind every task down - the sync counterpart of {@code AsyncEsqlQueryActionIT}'s delete test. Cancellation reaches
     * the branch machinery through the {@code CancellableTask} listener that {@code ComputeService.execute} registers,
     * which calls {@code SubPlanTaskRunner.fail}; an unstarted merge then hits the failure check in
     * {@code MergeLevelExecutor.tryExecuteNextSubPlan} instead of expanding. If it expanded anyway, its paused branch
     * would evaluate {@code pause_me} and raise {@code scriptWaits} past what the single outer branch can produce.
     * <p>
     * {@code branch_parallel_degree = 1}: the first outer branch takes the only permit and blocks on {@code pause_me};
     * the nested union waits unstarted. The cancel lands while the query is in that state, so the failure is recorded
     * before the outer branch completes and before the nested merge is ever considered.
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
}
