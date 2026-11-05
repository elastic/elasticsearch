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
import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.withOwnership;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateMasterIndexTaskWrite;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateMasterShardTaskWrite;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateNodeClaim;
import static org.elasticsearch.xpack.downsample.orchestration.StateTransitions.validateNodeShardTaskWrite;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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

        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc moved = ShardTaskDoc.newTask(
            randomValueOtherThan(running.projectId(), ESTestCase::randomUniqueProjectId),
            running.targetIndex(),
            running.targetIndexUuid(),
            running.params(),
            running.createdAt()
        ).claimBy(NODE, randomUUID(), Map.of()).unassign();
        rejected(() -> validateMasterShardTaskWrite(running, moved), "the project cannot change");
        rejected(() -> validateNodeShardTaskWrite(NODE, running.epoch(), running, moved), "the project cannot change");
    }

    // ---- shard task, master ----

    public void testNewShardTask() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        validateMasterShardTaskWrite(null, doc);
        rejected(() -> validateMasterShardTaskWrite(null, doc.claimBy(NODE, randomUUID(), Map.of())), "must be [UNASSIGNED]");
    }

    public void testMasterReleasesRetriesAndFails() {
        ShardTaskDoc running = claimed(NODE);
        validateMasterShardTaskWrite(running, running.unassign());
        validateMasterShardTaskWrite(running, running.failedByMaster("gave up"));
        ShardTaskDoc retriable = running.failedOnNode("boom", true, Map.of());
        ShardTaskDoc retried = retriable.retry();
        validateMasterShardTaskWrite(retriable, retried);
        assertThat(retried.status(), equalTo(ShardTaskStatus.UNASSIGNED));
        assertThat(retried.attempt(), equalTo(retriable.attempt() + 1));
        assertThat(retried.assignedNode(), nullValue());
    }

    public void testMasterCannotGiveATaskToANode() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        ShardTaskDoc given = withOwnership(doc, ShardTaskStatus.UNASSIGNED, NODE, doc.epoch() + 1, doc.attempt(), null);
        rejected(() -> validateMasterShardTaskWrite(doc, given), "cannot have a node");
    }

    public void testMasterMustIncrementEpoch() {
        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc skippedEpoch = running.unassign().claimBy(OTHER_NODE, randomUUID(), Map.of()).unassign();
        rejected(() -> validateMasterShardTaskWrite(running, skippedEpoch), "must increment the epoch");
        rejected(() -> validateMasterShardTaskWrite(running, running.withProgress(Map.of())), "must increment the epoch");
    }

    public void testMasterCannotActAsNode() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        // a doc that only differs by status and an incremented epoch, so the status is the only reason to reject
        ShardTaskDoc completed = withOwnership(
            doc,
            randomFrom(ShardTaskStatus.RUNNING, ShardTaskStatus.COMPLETED),
            NODE,
            doc.epoch() + 1,
            doc.attempt(),
            randomUUID()
        );
        rejected(() -> validateMasterShardTaskWrite(doc, completed), "only the owning node");
    }

    public void testFinishedShardTaskIsFinal() {
        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc completed = running.completed(Map.of());
        rejected(() -> validateMasterShardTaskWrite(completed, completed.retry()), "completed shard task");
        ShardTaskDoc failedForGood = running.failedOnNode("boom", false, Map.of());
        rejected(() -> validateMasterShardTaskWrite(failedForGood, failedForGood.retry()), "not retriable");
    }

    public void testMasterCannotChangeShardTaskIdentity() {
        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc otherTarget = new ShardTaskDoc(
            running.projectId(),
            running.targetIndex(),
            randomUUID(),
            running.params(),
            running.createdAt(),
            ShardTaskStatus.UNASSIGNED,
            null,
            running.epoch() + 1,
            running.attempt(),
            null,
            false,
            null,
            null
        );
        rejected(() -> validateMasterShardTaskWrite(running, otherTarget), "target index uuid cannot change");
        ShardTaskDoc otherCreation = new ShardTaskDoc(
            running.projectId(),
            running.targetIndex(),
            running.targetIndexUuid(),
            running.params(),
            running.createdAt() + 1,
            ShardTaskStatus.UNASSIGNED,
            null,
            running.epoch() + 1,
            running.attempt(),
            null,
            false,
            null,
            null
        );
        rejected(() -> validateMasterShardTaskWrite(running, otherCreation), "creation time cannot change");
    }

    // ---- shard task, node claims ----

    public void testNodeClaimsAnUnassignedTask() {
        ShardTaskDoc doc = randomBoolean() ? newShardTask("target", randomUUID()) : claimed(OTHER_NODE).unassign();
        ShardTaskDoc running = doc.claimBy(NODE, randomUUID(), Map.of());
        validateNodeClaim(NODE, doc, running);
        assertThat(running.status(), equalTo(ShardTaskStatus.RUNNING));
        assertThat(running.epoch(), equalTo(doc.epoch() + 1));
        assertThat(running.attempt(), equalTo(doc.attempt()));
    }

    public void testNodeClaimsOnlyForItself() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        rejected(() -> validateNodeClaim(NODE, doc, doc.claimBy(OTHER_NODE, randomUUID(), Map.of())), "only claim");
    }

    public void testNodeClaimsOnlyUnassignedTasks() {
        ShardTaskDoc running = claimed(OTHER_NODE);
        ShardTaskDoc taken = randomFrom(running, running.completed(Map.of()), running.failedOnNode("boom", randomBoolean(), Map.of()));
        rejected(() -> validateNodeClaim(NODE, taken, taken.claimBy(NODE, randomUUID(), Map.of())), "only an unassigned shard task");
    }

    public void testClaimMustIncrementEpochAndKeepAttempts() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        ShardTaskDoc sameEpoch = withOwnership(doc, ShardTaskStatus.RUNNING, NODE, doc.epoch(), doc.attempt(), randomUUID());
        rejected(() -> validateNodeClaim(NODE, doc, sameEpoch), "must increment the epoch");
        ShardTaskDoc moreAttempts = withOwnership(doc, ShardTaskStatus.RUNNING, NODE, doc.epoch() + 1, doc.attempt() + 1, randomUUID());
        rejected(() -> validateNodeClaim(NODE, doc, moreAttempts), "cannot change the attempt");
        ShardTaskDoc completed = withOwnership(doc, ShardTaskStatus.COMPLETED, NODE, doc.epoch() + 1, doc.attempt(), randomUUID());
        rejected(() -> validateNodeClaim(NODE, doc, completed), "must be [RUNNING]");
        ShardTaskDoc noRunId = withOwnership(doc, ShardTaskStatus.RUNNING, NODE, doc.epoch() + 1, doc.attempt(), null);
        rejected(() -> validateNodeClaim(NODE, doc, noRunId), "needs a run id");
    }

    // ---- shard task, owning node ----

    public void testNodeRunsAndFinishesItsTask() {
        ShardTaskDoc running = claimed(NODE);
        long epoch = running.epoch();
        // a new run after a restart
        validateNodeShardTaskWrite(NODE, epoch, running, running.running(randomUUID(), Map.of()));
        validateNodeShardTaskWrite(NODE, epoch, running, running.withProgress(Map.of("in_num_docs_received", 1)));
        validateNodeShardTaskWrite(NODE, epoch, running, running.completed(Map.of()));
        validateNodeShardTaskWrite(NODE, epoch, running, running.failedOnNode("boom", randomBoolean(), Map.of()));
    }

    public void testNodeCannotWriteAnotherNodesTask() {
        ShardTaskDoc running = claimed(OTHER_NODE);
        rejected(
            () -> validateNodeShardTaskWrite(NODE, running.epoch(), running, running.withProgress(Map.of())),
            "is not owned by node [" + NODE + "]"
        );
    }

    public void testNodeIsFencedByEpoch() {
        ShardTaskDoc running = claimed(NODE);
        long heldEpoch = running.epoch();
        // the master released the task and the same node claimed it again: the old run is still fenced
        ShardTaskDoc reclaimed = running.unassign().claimBy(NODE, randomUUID(), Map.of());
        rejected(() -> validateNodeShardTaskWrite(NODE, heldEpoch, reclaimed, reclaimed.withProgress(Map.of())), "holds epoch");
    }

    public void testNodeCannotWriteUnownedStates() {
        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc completed = running.completed(Map.of());
        rejected(
            () -> validateNodeShardTaskWrite(NODE, running.epoch(), completed, completed.withProgress(Map.of())),
            "a node cannot write"
        );
        ShardTaskDoc released = running.unassign();
        rejected(
            () -> validateNodeShardTaskWrite(NODE, running.epoch(), released, released.withProgress(Map.of())),
            "is not owned by node"
        );
    }

    public void testNodeCannotActAsMaster() {
        ShardTaskDoc running = claimed(NODE);
        long epoch = running.epoch();
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, running, running.unassign()), "cannot change the assigned");
        ShardTaskDoc handedOver = withOwnership(running, ShardTaskStatus.RUNNING, OTHER_NODE, epoch, running.attempt(), randomUUID());
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, running, handedOver), "cannot change the assigned");
        ShardTaskDoc bumped = withOwnership(running, ShardTaskStatus.RUNNING, NODE, epoch + 1, running.attempt(), randomUUID());
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, running, bumped), "cannot change the epoch");
        ShardTaskDoc moreAttempts = withOwnership(running, ShardTaskStatus.RUNNING, NODE, epoch, running.attempt() + 1, randomUUID());
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, running, moreAttempts), "cannot change the attempt");
        ShardTaskDoc unassigned = withOwnership(running, ShardTaskStatus.UNASSIGNED, NODE, epoch, running.attempt(), null);
        rejected(() -> validateNodeShardTaskWrite(NODE, epoch, running, unassigned), "only the master");
    }

    public void testRunningNeedsRunId() {
        ShardTaskDoc running = claimed(NODE);
        ShardTaskDoc noRunId = withOwnership(running, ShardTaskStatus.RUNNING, NODE, running.epoch(), running.attempt(), null);
        rejected(() -> validateNodeShardTaskWrite(NODE, running.epoch(), running, noRunId), "needs a run id");
    }

    private static ShardTaskDoc claimed(String node) {
        return newShardTask("target", randomUUID()).claimBy(node, randomUUID(), Map.of());
    }

    private static void rejected(Runnable write, String expectedMessage) {
        IllegalStateException e = expectThrows(IllegalStateException.class, write::run);
        assertThat(e.getMessage(), containsString(expectedMessage));
    }
}
