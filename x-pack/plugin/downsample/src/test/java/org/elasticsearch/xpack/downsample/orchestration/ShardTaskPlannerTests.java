/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newShardTask;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardTaskPlannerTests extends ESTestCase {

    private static final int MAX_ATTEMPTS = 3;
    private static final Set<String> NODES = Set.of("node-1", "node-2");

    public void testLeavesUnassignedTaskToTheNodes() {
        // nodes claim unassigned tasks themselves, the master never picks a node
        ShardTaskDoc doc = randomBoolean() ? newShardTask("target", randomUUID()) : running("node-gone").unassign();
        assertThat(ShardTaskPlanner.plan(doc, NODES, MAX_ATTEMPTS), nullValue());
    }

    public void testLeavesTaskOnPresentNode() {
        assertThat(ShardTaskPlanner.plan(running("node-1"), NODES, MAX_ATTEMPTS), nullValue());
    }

    public void testReleasesTaskOfDepartedNodeWithoutCountingAnAttempt() {
        ShardTaskDoc task = running("node-gone");
        ShardTaskDoc next = ShardTaskPlanner.plan(task, NODES, MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.UNASSIGNED));
        assertThat(next.assignedNode(), nullValue());
        assertThat(next.epoch(), equalTo(task.epoch() + 1));
        assertThat(next.attempt(), equalTo(task.attempt()));
        StateTransitions.validateMasterShardTaskWrite(task, next);
    }

    public void testRetriesRetriableFailure() {
        ShardTaskDoc failed = running("node-1").failedOnNode("bulk failure", true, Map.of());
        ShardTaskDoc next = ShardTaskPlanner.plan(failed, NODES, MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.UNASSIGNED));
        assertThat(next.assignedNode(), nullValue());
        assertThat(next.attempt(), equalTo(1));
        StateTransitions.validateMasterShardTaskWrite(failed, next);
    }

    public void testFailsForGoodWhenAttemptsAreExhausted() {
        ShardTaskDoc task = newShardTask("target", randomUUID());
        for (int i = 1; i < MAX_ATTEMPTS; i++) {
            task = task.claimBy("node-1", randomUUID(), Map.of()).failedOnNode("bulk failure", true, Map.of()).retry();
        }
        ShardTaskDoc failed = task.claimBy("node-1", randomUUID(), Map.of()).failedOnNode("bulk failure", true, Map.of());
        ShardTaskDoc next = ShardTaskPlanner.plan(failed, NODES, MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.FAILED));
        assertThat(next.retriable(), equalTo(false));
        assertThat(next.lastError(), containsString("bulk failure"));
        StateTransitions.validateMasterShardTaskWrite(failed, next);
    }

    public void testLeavesFinishedTasks() {
        ShardTaskDoc running = running("node-1");
        assertThat(ShardTaskPlanner.plan(running.completed(Map.of()), NODES, MAX_ATTEMPTS), nullValue());
        assertThat(ShardTaskPlanner.plan(running.failedOnNode("boom", false, Map.of()), NODES, MAX_ATTEMPTS), nullValue());
    }

    public void testOutcome() {
        ShardTaskDoc running = running("node-1");
        ShardTaskDoc completed = running.completed(Map.of());
        assertThat(ShardTaskPlanner.outcome(List.of(completed, completed)), equalTo(ShardTaskPlanner.Outcome.ALL_COMPLETED));
        assertThat(ShardTaskPlanner.outcome(List.of(completed, running)), equalTo(ShardTaskPlanner.Outcome.IN_PROGRESS));
        assertThat(
            ShardTaskPlanner.outcome(List.of(completed, running.failedOnNode("boom", true, Map.of()))),
            equalTo(ShardTaskPlanner.Outcome.IN_PROGRESS)
        );
        assertThat(
            ShardTaskPlanner.outcome(List.of(running, running.failedOnNode("boom", false, Map.of()))),
            equalTo(ShardTaskPlanner.Outcome.FAILED)
        );
    }

    private static ShardTaskDoc running(String node) {
        return newShardTask("target", randomUUID()).claimBy(node, randomUUID(), Map.of());
    }
}
