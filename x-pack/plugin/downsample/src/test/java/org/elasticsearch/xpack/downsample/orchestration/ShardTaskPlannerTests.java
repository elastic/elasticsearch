/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newShardTask;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardTaskPlannerTests extends ESTestCase {

    private static final int MAX_ATTEMPTS = 3;
    private static final Set<String> NODES = Set.of("node-1", "node-2");

    public void testAssignsUnassignedTask() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        ShardTaskDoc next = ShardTaskPlanner.plan(doc, NODES, List.of("node-2", "node-1"), MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.ASSIGNED));
        assertThat(next.assignedNode(), equalTo("node-2"));
        assertThat(next.epoch(), equalTo(doc.epoch() + 1));
        assertThat(next.attempt(), equalTo(0));
        StateTransitions.validateMasterShardTaskWrite(doc, next);
    }

    public void testWaitsWithoutCandidates() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        assertThat(ShardTaskPlanner.plan(doc, NODES, List.of(), MAX_ATTEMPTS), nullValue());
    }

    public void testLeavesTaskOnPresentNode() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo("node-1", false);
        ShardTaskDoc task = randomBoolean() ? assigned : assigned.running(randomUUID(), Map.of());
        assertThat(ShardTaskPlanner.plan(task, NODES, List.of("node-2"), MAX_ATTEMPTS), nullValue());
    }

    public void testReassignsTaskOfDepartedNodeWithoutCountingAnAttempt() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo("node-gone", false);
        ShardTaskDoc task = randomBoolean() ? assigned : assigned.running(randomUUID(), Map.of());
        ShardTaskDoc next = ShardTaskPlanner.plan(task, NODES, List.of("node-1"), MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.ASSIGNED));
        assertThat(next.assignedNode(), equalTo("node-1"));
        assertThat(next.epoch(), equalTo(task.epoch() + 1));
        assertThat(next.attempt(), equalTo(task.attempt()));
        StateTransitions.validateMasterShardTaskWrite(task, next);
    }

    public void testUnassignsTaskOfDepartedNodeWithoutCandidates() {
        ShardTaskDoc task = newShardTask("target", randomUUID()).assignTo("node-gone", false);
        ShardTaskDoc next = ShardTaskPlanner.plan(task, NODES, List.of(), MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.UNASSIGNED));
        assertThat(next.assignedNode(), nullValue());
        StateTransitions.validateMasterShardTaskWrite(task, next);
    }

    public void testRetriesRetriableFailure() {
        ShardTaskDoc failed = newShardTask("target", randomUUID()).assignTo("node-1", false)
            .running(randomUUID(), Map.of())
            .failedOnNode("bulk failure", true, Map.of());
        ShardTaskDoc next = ShardTaskPlanner.plan(failed, NODES, List.of("node-2"), MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.ASSIGNED));
        assertThat(next.assignedNode(), equalTo("node-2"));
        assertThat(next.attempt(), equalTo(1));
        StateTransitions.validateMasterShardTaskWrite(failed, next);
    }

    public void testFailsForGoodWhenAttemptsAreExhausted() {
        ShardTaskDoc task = newShardTask("target", randomUUID()).assignTo("node-1", false);
        for (int i = 1; i < MAX_ATTEMPTS; i++) {
            task = task.assignTo("node-1", true);
        }
        ShardTaskDoc failed = task.running(randomUUID(), Map.of()).failedOnNode("bulk failure", true, Map.of());
        ShardTaskDoc next = ShardTaskPlanner.plan(failed, NODES, List.of("node-1"), MAX_ATTEMPTS);
        assertThat(next.status(), equalTo(ShardTaskStatus.FAILED));
        assertThat(next.retriable(), equalTo(false));
        assertThat(next.lastError(), containsString("bulk failure"));
        StateTransitions.validateMasterShardTaskWrite(failed, next);
    }

    public void testLeavesFinishedTasks() {
        ShardTaskDoc running = newShardTask("target", randomUUID()).assignTo("node-1", false).running(randomUUID(), Map.of());
        assertThat(ShardTaskPlanner.plan(running.completed(Map.of()), NODES, List.of("node-1"), MAX_ATTEMPTS), nullValue());
        assertThat(
            ShardTaskPlanner.plan(running.failedOnNode("boom", false, Map.of()), NODES, List.of("node-1"), MAX_ATTEMPTS),
            nullValue()
        );
    }

    public void testOutcome() {
        ShardTaskDoc running = newShardTask("target", randomUUID()).assignTo("node-1", false).running(randomUUID(), Map.of());
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

    public void testCandidatesAreStartedCopiesLeastLoadedFirst() {
        ShardId shardId = new ShardId(new Index("source", randomUUID()), 0);
        IndexShardRoutingTable routing = new IndexShardRoutingTable.Builder(shardId).addShard(
            TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED)
        )
            .addShard(TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(shardId, "node-3", false, ShardRoutingState.INITIALIZING))
            .build();
        assertThat(ShardTaskPlanner.candidateNodes(routing, Map.of()), contains("node-1", "node-2"));
        assertThat(ShardTaskPlanner.candidateNodes(routing, Map.of("node-1", 2, "node-2", 1)), contains("node-2", "node-1"));
        assertThat(ShardTaskPlanner.candidateNodes(null, Map.of()), empty());
    }

    public void testLoadCountsOwnedTasks() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo("node-1", false);
        ShardTaskDoc running = newShardTask("target", randomUUID()).assignTo("node-1", false).running(randomUUID(), Map.of());
        ShardTaskDoc completed = newShardTask("target", randomUUID()).assignTo("node-2", false)
            .running(randomUUID(), Map.of())
            .completed(Map.of());
        assertThat(ShardTaskPlanner.load(List.of(assigned, running, completed)), equalTo(Map.of("node-1", 2)));
    }
}
