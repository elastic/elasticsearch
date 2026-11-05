/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newIndexTask;
import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newShardTask;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateMasterIndexTaskWrite;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateMasterShardTaskWrite;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateNodeShardTaskWrite;
import static org.hamcrest.Matchers.containsString;

public class StateTransitionsTests extends ESTestCase {

    private static final String NODE = "node-1";
    private static final String OTHER_NODE = "node-2";

    // ---- index task, master ----

    public void testNewIndexTask() {
        IndexTaskDoc doc = newIndexTask("target");
        validateMasterIndexTaskWrite(null, doc);
        rejected(() -> validateMasterIndexTaskWrite(null, doc.targetCreated(randomUUID())), "must start at [CREATE_TARGET]");
        rejected(() -> validateMasterIndexTaskWrite(null, doc.withRetry("boom", 1)), "must have no attempts");
    }

    public void testTerminalIndexTaskCanBeReplaced() {
        IndexTaskDoc finished = randomBoolean()
            ? newIndexTask("target").targetCreated(randomUUID()).withPhase(IndexTaskPhase.DONE)
            : newIndexTask("target").failed("boom");
        validateMasterIndexTaskWrite(finished, newIndexTask("target"));
    }

    public void testIndexTaskPhasesMoveForward() {
        IndexTaskDoc doc = newIndexTask("target");
        IndexTaskDoc running = doc.targetCreated(randomUUID());
        validateMasterIndexTaskWrite(doc, running);
        validateMasterIndexTaskWrite(running, running.withPhase(IndexTaskPhase.FINALIZE_SETTINGS));
        validateMasterIndexTaskWrite(running, running.withRetry("boom", 1));
        IndexTaskDoc finalizing = running.withPhase(IndexTaskPhase.FINALIZE_SETTINGS);
        rejected(() -> validateMasterIndexTaskWrite(finalizing, finalizing.withPhase(IndexTaskPhase.RUN_SHARDS)), "only move forward");
    }

    public void testAnyActiveIndexTaskCanFail() {
        IndexTaskDoc doc = newIndexTask("target");
        if (randomBoolean()) {
            doc = doc.targetCreated(randomUUID()).withPhase(randomFrom(IndexTaskPhase.RUN_SHARDS, IndexTaskPhase.REFRESH));
        }
        validateMasterIndexTaskWrite(doc, doc.failed("boom"));
    }

    public void testIndexTaskNeedsTargetUuidAfterCreation() {
        IndexTaskDoc doc = newIndexTask("target");
        rejected(() -> validateMasterIndexTaskWrite(doc, doc.withPhase(IndexTaskPhase.RUN_SHARDS)), "requires the target index uuid");
    }

    public void testIndexTaskTargetUuidIsImmutable() {
        IndexTaskDoc running = newIndexTask("target").targetCreated(randomUUID());
        IndexTaskDoc otherUuid = new IndexTaskDoc(
            running.projectId(),
            running.targetIndex(),
            randomUUID(),
            running.sourceIndex(),
            running.sourceIndexUuid(),
            running.config(),
            running.phase(),
            running.attempt(),
            running.nextRetryAt(),
            running.lastError(),
            running.errorHistory(),
            running.createdAt(),
            running.deadline()
        );
        rejected(() -> validateMasterIndexTaskWrite(running, otherUuid), "uuid cannot change once recorded");
    }

    public void testIndexTaskIdentityIsImmutable() {
        IndexTaskDoc doc = newIndexTask("target");
        IndexTaskDoc otherSource = new IndexTaskDoc(
            doc.projectId(),
            doc.targetIndex(),
            doc.targetIndexUuid(),
            doc.sourceIndex(),
            randomUUID(),
            doc.config(),
            doc.phase(),
            doc.attempt(),
            doc.nextRetryAt(),
            doc.lastError(),
            doc.errorHistory(),
            doc.createdAt(),
            doc.deadline()
        );
        rejected(() -> validateMasterIndexTaskWrite(doc, otherSource), "source index uuid cannot change");
        IndexTaskDoc otherConfig = new IndexTaskDoc(
            doc.projectId(),
            doc.targetIndex(),
            doc.targetIndexUuid(),
            doc.sourceIndex(),
            doc.sourceIndexUuid(),
            randomValueOtherThan(doc.config(), OrchestrationTestUtils::randomConfig),
            doc.phase(),
            doc.attempt(),
            doc.nextRetryAt(),
            doc.lastError(),
            doc.errorHistory(),
            doc.createdAt(),
            doc.deadline()
        );
        rejected(() -> validateMasterIndexTaskWrite(doc, otherConfig), "config cannot change");
    }

    public void testProjectIsImmutable() {
        IndexTaskDoc doc = newIndexTask("target");
        IndexTaskDoc otherProject = IndexTaskDoc.newTask(
            randomValueOtherThan(doc.projectId(), ESTestCase::randomUniqueProjectId),
            doc.targetIndex(),
            doc.sourceIndex(),
            doc.sourceIndexUuid(),
            doc.config(),
            doc.createdAt(),
            doc.deadline()
        );
        rejected(() -> validateMasterIndexTaskWrite(doc, otherProject), "the project cannot change");

        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        ShardTaskDoc moved = ShardTaskDoc.newTask(
            randomValueOtherThan(assigned.projectId(), ESTestCase::randomUniqueProjectId),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params()
        ).assignTo(OTHER_NODE, false).assignTo(OTHER_NODE, false);
        rejected(() -> validateMasterShardTaskWrite(assigned, moved), "the project cannot change");
        rejected(() -> validateNodeShardTaskWrite(NODE, assigned.epoch(), assigned, moved), "the project cannot change");
    }

    // ---- shard task, master ----

    public void testNewShardTask() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        validateMasterShardTaskWrite(null, doc);
        rejected(() -> validateMasterShardTaskWrite(null, doc.assignTo(NODE, false)), "must be [UNASSIGNED]");
    }

    public void testMasterAssignsReassignsAndUnassigns() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        ShardTaskDoc assigned = doc.assignTo(NODE, false);
        validateMasterShardTaskWrite(doc, assigned);
        ShardTaskDoc running = assigned.running(randomUUID(), Map.of());
        validateMasterShardTaskWrite(running, running.assignTo(OTHER_NODE, true));
        validateMasterShardTaskWrite(running, running.unassign());
        validateMasterShardTaskWrite(running, running.failedByMaster("gave up"));
    }

    public void testMasterMustIncrementEpoch() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        ShardTaskDoc skippedEpoch = assigned.assignTo(OTHER_NODE, false).assignTo(OTHER_NODE, false);
        rejected(() -> validateMasterShardTaskWrite(assigned, skippedEpoch), "must increment the epoch");
        rejected(() -> validateMasterShardTaskWrite(assigned, assigned.withProgress(Map.of())), "must increment the epoch");
    }

    public void testMasterCannotActAsNode() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        // build a doc that only differs by status and an incremented epoch, so the status is the only reason to reject
        ShardTaskDoc completed = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params(),
            randomFrom(ShardTaskStatus.RUNNING, ShardTaskStatus.COMPLETED),
            NODE,
            assigned.epoch() + 1,
            assigned.attempt(),
            null,
            false,
            randomUUID(),
            Map.of()
        );
        rejected(() -> validateMasterShardTaskWrite(assigned, completed), "only the assigned node");
    }

    public void testFinishedShardTaskIsFinal() {
        ShardTaskDoc running = newShardTask("target", randomUUID()).assignTo(NODE, false).running(randomUUID(), Map.of());
        ShardTaskDoc completed = running.completed(Map.of());
        rejected(() -> validateMasterShardTaskWrite(completed, completed.assignTo(OTHER_NODE, true)), "completed shard task");
        ShardTaskDoc failedForGood = running.failedOnNode("boom", false, Map.of());
        rejected(() -> validateMasterShardTaskWrite(failedForGood, failedForGood.assignTo(OTHER_NODE, true)), "not retriable");
        ShardTaskDoc retriable = running.failedOnNode("boom", true, Map.of());
        validateMasterShardTaskWrite(retriable, retriable.assignTo(OTHER_NODE, true));
    }

    public void testMasterCannotChangeShardTaskIdentity() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        ShardTaskDoc otherTarget = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            randomUUID(),
            assigned.params(),
            ShardTaskStatus.ASSIGNED,
            OTHER_NODE,
            assigned.epoch() + 1,
            assigned.attempt(),
            null,
            false,
            null,
            null
        );
        rejected(() -> validateMasterShardTaskWrite(assigned, otherTarget), "target index uuid cannot change");
    }

    // ---- shard task, node ----

    public void testNodeRunsAndFinishesItsTask() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        long epoch = assigned.epoch();
        ShardTaskDoc running = assigned.running(randomUUID(), Map.of());
        validateNodeShardTaskWrite(NODE, epoch, assigned, running);
        validateNodeShardTaskWrite(NODE, epoch, running, running.withProgress(Map.of("in_num_docs_received", 1)));
        validateNodeShardTaskWrite(NODE, epoch, running, running.completed(Map.of()));
        validateNodeShardTaskWrite(NODE, epoch, running, running.failedOnNode("boom", randomBoolean(), Map.of()));
        // a node can fail a task before marking it running, e.g. when the source shard is already gone
        validateNodeShardTaskWrite(NODE, epoch, assigned, assigned.failedOnNode("boom", randomBoolean(), Map.of()));
    }

    public void testNodeCompletesOnlyARunningTask() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        rejected(
            () -> validateNodeShardTaskWrite(NODE, assigned.epoch(), assigned, assigned.completed(Map.of())),
            "only a running shard task can complete"
        );
    }

    public void testNodeCannotWriteAnotherNodesTask() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(OTHER_NODE, false);
        rejected(
            () -> validateNodeShardTaskWrite(NODE, assigned.epoch(), assigned, assigned.running(randomUUID(), Map.of())),
            "is not assigned to node [" + NODE + "]"
        );
    }

    public void testNodeIsFencedByEpoch() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        long heldEpoch = assigned.epoch();
        // the master took the task away and gave it back to the same node: the old run is still fenced
        ShardTaskDoc reassigned = assigned.unassign().assignTo(NODE, true);
        rejected(() -> validateNodeShardTaskWrite(NODE, heldEpoch, reassigned, reassigned.running(randomUUID(), Map.of())), "holds epoch");
    }

    public void testNodeCannotWriteUnownedStates() {
        ShardTaskDoc running = newShardTask("target", randomUUID()).assignTo(NODE, false).running(randomUUID(), Map.of());
        ShardTaskDoc completed = running.completed(Map.of());
        rejected(
            () -> validateNodeShardTaskWrite(NODE, running.epoch(), completed, completed.withProgress(Map.of())),
            "a node cannot write"
        );
    }

    public void testNodeCannotActAsMaster() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        long epoch = assigned.epoch();
        rejected(
            () -> validateNodeShardTaskWrite(NODE, epoch, assigned, assigned.assignTo(OTHER_NODE, false)),
            "cannot change the assigned"
        );
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, assigned, assigned.unassign()), "cannot change the assigned");
        ShardTaskDoc bumped = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params(),
            ShardTaskStatus.RUNNING,
            NODE,
            epoch + 1,
            assigned.attempt(),
            null,
            false,
            randomUUID(),
            null
        );
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, assigned, bumped), "cannot change the epoch");
        ShardTaskDoc moreAttempts = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params(),
            ShardTaskStatus.RUNNING,
            NODE,
            epoch,
            assigned.attempt() + 1,
            null,
            false,
            randomUUID(),
            null
        );
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, assigned, moreAttempts), "cannot change the attempt");
        ShardTaskDoc backToAssigned = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params(),
            ShardTaskStatus.ASSIGNED,
            NODE,
            epoch,
            assigned.attempt(),
            null,
            false,
            null,
            null
        );
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, assigned, backToAssigned), "only the master");
    }

    public void testRunningNeedsRunId() {
        ShardTaskDoc assigned = newShardTask("target", randomUUID()).assignTo(NODE, false);
        ShardTaskDoc noRunId = new ShardTaskDoc(
            assigned.projectId(),
            assigned.targetIndex(),
            assigned.targetIndexUuid(),
            assigned.params(),
            ShardTaskStatus.RUNNING,
            NODE,
            assigned.epoch(),
            assigned.attempt(),
            null,
            false,
            null,
            null
        );
        rejected(() -> validateNodeShardTaskWrite(NODE, assigned.epoch(), assigned, noRunId), "needs a run id");
    }

    private static void rejected(Runnable write, String expectedMessage) {
        IllegalStateException e = expectThrows(IllegalStateException.class, write::run);
        assertThat(e.getMessage(), containsString(expectedMessage));
    }
}
