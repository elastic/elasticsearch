/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSourceExec;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.Stage;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.StagePlacement;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.StagedPlan;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.containsString;

/** Exercises scheduling callbacks on real stage graphs without depending on transport timing. */
public class StageSchedulerTests extends ESTestCase {
    private final ThreadContext context = new ThreadContext(Settings.EMPTY);

    public void testChainedJoinBuildsCompleteBeforeProbeProducersStart() {
        for (String strategy : List.of("partitioned", "broadcast")) {
            StagedPlan plan = joinPlan(strategy, false);
            Stage receiver = joinStage(plan);
            DistributedHashJoinExec outer = receiver.plan().collect(DistributedHashJoinExec.class).getFirst();
            DistributedHashJoinExec inner = outer.left().collect(DistributedHashJoinExec.class).getFirst();
            int outerBuild = producer(plan, outer.right().collect(StageSourceExec.class).getFirst());
            int innerBuild = producer(plan, inner.right().collect(StageSourceExec.class).getFirst());
            Map<Integer, ActionListener<Void>> running = new LinkedHashMap<>();
            PlainActionFuture<Void> finished = new PlainActionFuture<>();
            try (var restoreContext = context.stashContext()) {
                context.putHeader("request-id", "shuffle");
                StageScheduler.scheduleStages(plan, task(), EsExecutors.DIRECT_EXECUTOR_SERVICE, context, (stage, listener) -> {
                    assertEquals("shuffle", context.getHeader("request-id"));
                    assertNull(running.put(stage.id(), listener));
                }, finished);

                assertTrue(running.containsKey(receiver.id()));
                assertTrue(running.containsKey(outerBuild));
                assertFalse(running.containsKey(innerBuild));
                assertEquals(2, running.size());
                try (var ignored = context.stashContext()) {
                    running.remove(outerBuild).onResponse(null);
                }
                assertTrue(running.containsKey(innerBuild));
                running.remove(innerBuild).onResponse(null);
                if (strategy.equals("partitioned")) {
                    int probe = producer(plan, inner.left().collect(StageSourceExec.class).getFirst());
                    assertTrue(running.containsKey(probe));
                    running.remove(probe).onResponse(null);
                }
                assertFalse(finished.isDone());
                running.remove(receiver.id()).onResponse(null);
                assertTrue(running.isEmpty());
                finished.actionGet();
            }
        }
    }

    public void testUnionBuildWaitsForEveryProducer() {
        StagedPlan plan = joinPlan("partitioned", true);
        Stage receiver = joinStage(plan);
        DistributedHashJoinExec outer = receiver.plan().collect(DistributedHashJoinExec.class).getFirst();
        DistributedHashJoinExec inner = outer.left().collect(DistributedHashJoinExec.class).getFirst();
        List<Integer> builds = plan.edge(outer.right().collect(StageSourceExec.class).getFirst().edgeKey()).producerStages();
        int innerBuild = producer(plan, inner.right().collect(StageSourceExec.class).getFirst());
        int probe = producer(plan, inner.left().collect(StageSourceExec.class).getFirst());
        Map<Integer, ActionListener<Void>> running = new LinkedHashMap<>();
        PlainActionFuture<Void> finished = new PlainActionFuture<>();
        try (var restoreContext = context.stashContext()) {
            StageScheduler.scheduleStages(
                plan,
                task(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                context,
                (stage, listener) -> assertNull(running.put(stage.id(), listener)),
                finished
            );
            assertEquals(2, builds.size());
            running.remove(builds.getFirst()).onResponse(null);
            assertFalse(running.containsKey(innerBuild));
            running.remove(builds.getLast()).onResponse(null);
            running.remove(innerBuild).onResponse(null);
            running.remove(probe).onResponse(null);
            running.remove(receiver.id()).onResponse(null);
            finished.actionGet();
        }
    }

    public void testBuildFailureDoesNotStartWaitingProducers() {
        StagedPlan plan = joinPlan("partitioned", false);
        Stage receiver = joinStage(plan);
        DistributedHashJoinExec outer = receiver.plan().collect(DistributedHashJoinExec.class).getFirst();
        int build = producer(plan, outer.right().collect(StageSourceExec.class).getFirst());
        Map<Integer, ActionListener<Void>> running = new LinkedHashMap<>();
        PlainActionFuture<Void> finished = new PlainActionFuture<>();
        try (var restoreContext = context.stashContext()) {
            StageScheduler.scheduleStages(
                plan,
                task(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                context,
                (stage, listener) -> assertNull(running.put(stage.id(), listener)),
                finished
            );
            IllegalStateException failure = new IllegalStateException("build failed");
            running.remove(build).onFailure(failure);
            assertEquals(List.of(receiver.id()), List.copyOf(running.keySet()));
            assertFalse(finished.isDone());
            running.remove(receiver.id()).onFailure(failure);
            assertSame(failure, expectThrows(IllegalStateException.class, finished::actionGet));
        }
    }

    public void testBufferBudgetReducesAllHashStagesTogether() {
        StagedPlan plan = joinPlan("partitioned", false);
        Map<Integer, Integer> nodes = nodeCounts(plan);
        assertEquals(4, StageScheduler.budgetedPartitions(plan, nodes, 2, 4, 1, 50));
        assertEquals(2, StageScheduler.budgetedPartitions(plan, nodes, 2, 4, 1, 26));
        assertEquals(1, StageScheduler.budgetedPartitions(plan, nodes, 2, Integer.MAX_VALUE, 1, 14));
        assertEquals(1, StageScheduler.budgetedPartitions(plan, nodes, 2, 1, 1, 50));
        IllegalArgumentException failure = expectThrows(
            IllegalArgumentException.class,
            () -> StageScheduler.budgetedPartitions(plan, nodes, 2, 4, 1, 13)
        );
        assertThat(failure.getMessage(), containsString("[shuffle_max_buffered_pages]=[13]"));
    }

    public void testCancellationBeforeQueuedStartsDoesNotDispatchStages() {
        StagedPlan plan = joinPlan("partitioned", false);
        CancellableTask task = task();
        ArrayDeque<Runnable> queued = new ArrayDeque<>();
        PlainActionFuture<Void> finished = new PlainActionFuture<>();
        StageScheduler.scheduleStages(
            plan,
            task,
            queued::add,
            context,
            (stage, listener) -> fail("cancelled stage dispatched: " + stage.id()),
            finished
        );
        assertFalse(finished.isDone());
        TaskCancelHelper.cancel(task, "cancel before dispatch");
        while (queued.isEmpty() == false) {
            queued.remove().run();
        }
        assertThat(expectThrows(TaskCancelledException.class, finished::actionGet).getMessage(), containsString("cancel before dispatch"));
    }

    public void testBufferBudgetCountsEveryUnionProducer() {
        StagedPlan plan = joinPlan("partitioned", true);
        assertEquals(1, StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 33));
        assertEquals(2, StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 34));
    }

    public void testBroadcastBudgetCannotBeReducedByLoweringHashPartitions() {
        StagedPlan plan = joinPlan("broadcast", false);
        assertEquals(4, StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 10));
        expectThrows(IllegalArgumentException.class, () -> StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 9));
        expectThrows(
            IllegalArgumentException.class,
            () -> StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE)
        );
    }

    public void testWorkerBroadcastBudgetCountsNodesRatherThanLanes() {
        StagedPlan plan = plan("""
            FROM test
            | STATS salary = SUM(salary) BY emp_no
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, salary | RENAME salary AS s1) ON emp_no
            | STATS total = SUM(salary + s1)
            | LIMIT 1000
            """, "broadcast", true);
        assertEquals(4, StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 22));
        assertEquals(3, StageScheduler.budgetedPartitions(plan, nodeCounts(plan), 2, 4, 1, 21));
    }

    private static CancellableTask task() {
        return new CancellableTask(1, "transport", "shuffle", "shuffle", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static Stage joinStage(StagedPlan plan) {
        return plan.stages()
            .stream()
            .filter(stage -> stage.plan().collect(DistributedHashJoinExec.class).size() == 2)
            .findFirst()
            .orElseThrow();
    }

    private static int producer(StagedPlan plan, StageSourceExec source) {
        return plan.edge(source.edgeKey()).producerStages().getFirst();
    }

    private static Map<Integer, Integer> nodeCounts(StagedPlan plan) {
        Map<Integer, Integer> result = new HashMap<>();
        for (Stage stage : plan.stages()) {
            result.put(stage.id(), stage.placement() == StagePlacement.COORDINATOR || stage.placement() == StagePlacement.SINGLE ? 1 : 2);
        }
        return result;
    }

    private static StagedPlan joinPlan(String strategy, boolean unionBuild) {
        String query = """
            FROM test
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, salary | RENAME salary AS s1) ON emp_no
            | LEFT OUTER JOIN (%s | KEEP emp_no, salary | RENAME salary AS s2) ON emp_no
            | STATS s1 = SUM(s1), s2 = SUM(s2) BY emp_no
            | LIMIT 1000
            """.formatted(unionBuild ? "FROM test, (FROM test | WHERE salary > 0)" : "FROM test");
        return plan(query, strategy, false);
    }

    private static StagedPlan plan(String query, String strategy, boolean shuffleAgg) {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 4)
                .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
                .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), strategy)
                .put(QueryPragmas.SHUFFLE_AGG.getKey(), shuffleAgg)
                .build()
        );
        var configuration = configuration(pragmas, query);
        var analyzer = analyzer().minimumTransportVersion(TransportVersion.current())
            .configuration(configuration)
            .addIndex("test", "mapping-basic.json")
            .addNoFieldsIndex()
            .buildAnalyzer();
        return StagedPlanner.split(new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query));
    }
}
