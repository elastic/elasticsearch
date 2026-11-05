/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import java.util.Objects;

/**
 * The rules for who may change what in the downsample state documents. {@link DownsampleStateStore} checks every write
 * against them before it is sent, so a caller cannot write a transition its role does not allow. The compare-and-set on
 * {@code _seq_no} / {@code _primary_term} makes the check and the write atomic: if the document changed after the caller
 * read it, the write fails and the caller has to re-read and re-decide.
 * <p>
 * Summary:
 * <ul>
 *     <li>The master owns the index task document and all its phase transitions.</li>
 *     <li>The master creates shard task documents, and releases, retries or fails them; each of these increments the
 *     epoch, which fences the node that held the task. The master never gives a task to a node.</li>
 *     <li>A node may claim an {@code UNASSIGNED} shard task for itself only, which moves it to {@code RUNNING} at the next
 *     epoch. Whether the node is eligible (holds a copy of the source shard, is not shutting down) is checked by the store
 *     against the cluster state, since it is not part of the document.</li>
 *     <li>A node may only write a shard task document it owns at the epoch it holds, may only move it to {@code RUNNING},
 *     {@code COMPLETED} or {@code FAILED}, and may not change its identity, owner, epoch or attempt.</li>
 * </ul>
 */
public final class StateTransitions {

    private StateTransitions() {}

    /**
     * The master writes an index task document. {@code current} is {@code null} when there is no document yet.
     */
    public static void validateMasterIndexTaskWrite(@Nullable IndexTaskDoc current, IndexTaskDoc next) {
        if (current == null || current.phase().isTerminal()) {
            // a new operation, or a new operation replacing a finished one with the same target name
            check(next.phase() == IndexTaskPhase.CREATE_TARGET, "a new index task must start at [CREATE_TARGET], got [%s]", next.phase());
            check(next.targetIndexUuid() == null, "a new index task cannot have a target index uuid");
            check(next.attempt() == 0, "a new index task must have no attempts");
            return;
        }
        check(current.projectId().equals(next.projectId()), "the project cannot change");
        check(current.targetIndex().equals(next.targetIndex()), "the target index cannot change");
        check(current.sourceIndex().equals(next.sourceIndex()), "the source index cannot change");
        check(current.sourceIndexUuid().equals(next.sourceIndexUuid()), "the source index uuid cannot change");
        check(current.config().equals(next.config()), "the downsample config cannot change");
        check(current.createdAt() == next.createdAt(), "the creation time cannot change");
        if (current.targetIndexUuid() != null) {
            check(current.targetIndexUuid().equals(next.targetIndexUuid()), "the target index uuid cannot change once recorded");
        }
        if (next.phase() != IndexTaskPhase.FAILED) {
            check(
                next.phase().ordinal() >= current.phase().ordinal(),
                "phase can only move forward, from [%s] to [%s]",
                current.phase(),
                next.phase()
            );
            check(
                next.phase() == IndexTaskPhase.CREATE_TARGET || next.targetIndexUuid() != null,
                "phase [%s] requires the target index uuid",
                next.phase()
            );
        }
    }

    /**
     * The master writes a shard task document. {@code current} is {@code null} when the document is being created.
     */
    public static void validateMasterShardTaskWrite(@Nullable ShardTaskDoc current, ShardTaskDoc next) {
        if (current == null) {
            check(next.status() == ShardTaskStatus.UNASSIGNED, "a new shard task must be [UNASSIGNED], got [%s]", next.status());
            check(next.assignedNode() == null, "a new shard task cannot be assigned");
            check(next.epoch() == 0 && next.attempt() == 0, "a new shard task must start at epoch 0 with no attempts");
            return;
        }
        checkSameTask(current, next);
        check(current.status() != ShardTaskStatus.COMPLETED, "a completed shard task cannot change");
        check(
            current.status() != ShardTaskStatus.FAILED || current.retriable(),
            "a shard task that failed and is not retriable cannot change"
        );
        check(
            next.epoch() == current.epoch() + 1,
            "the master must increment the epoch, from [%s] got [%s]",
            current.epoch(),
            next.epoch()
        );
        check(next.attempt() >= current.attempt(), "the attempt count cannot decrease");
        switch (next.status()) {
            case UNASSIGNED -> check(next.assignedNode() == null, "an unassigned shard task cannot have a node");
            case FAILED -> check(next.retriable() == false, "the master can only fail a shard task for good");
            case RUNNING, COMPLETED -> throw new IllegalStateException(
                "only the owning node can move a shard task to [" + next.status() + "]"
            );
        }
    }

    /**
     * A node claims an unassigned shard task: it becomes its owner and starts running it, at the next epoch.
     *
     * @param nodeId the id of the node that claims, the only node it may claim for
     */
    public static void validateNodeClaim(String nodeId, ShardTaskDoc current, ShardTaskDoc next) {
        Objects.requireNonNull(current, "a node cannot create a shard task");
        check(
            current.status() == ShardTaskStatus.UNASSIGNED,
            "shard task [%s] is [%s], only an unassigned shard task can be claimed",
            current.id(),
            current.status()
        );
        checkSameTask(current, next);
        check(nodeId.equals(next.assignedNode()), "node [%s] can only claim shard task [%s] for itself", nodeId, current.id());
        check(next.status() == ShardTaskStatus.RUNNING, "a claimed shard task must be [RUNNING], got [%s]", next.status());
        check(next.runId() != null, "a running shard task needs a run id");
        check(next.epoch() == current.epoch() + 1, "a claim must increment the epoch, from [%s] got [%s]", current.epoch(), next.epoch());
        check(current.attempt() == next.attempt(), "a node cannot change the attempt count");
    }

    /**
     * A node writes a shard task document.
     *
     * @param nodeId    the id of the node that writes
     * @param heldEpoch the epoch at which this node claimed the task, i.e. the one its local run was started with
     */
    public static void validateNodeShardTaskWrite(String nodeId, long heldEpoch, ShardTaskDoc current, ShardTaskDoc next) {
        Objects.requireNonNull(current, "a node cannot create a shard task");
        check(nodeId.equals(current.assignedNode()), "shard task [%s] is not owned by node [%s]", current.id(), nodeId);
        check(
            current.epoch() == heldEpoch,
            "shard task [%s] is at epoch [%s], the node holds epoch [%s]",
            current.id(),
            current.epoch(),
            heldEpoch
        );
        check(current.status().isOwnedByNode(), "shard task [%s] is [%s], a node cannot write it", current.id(), current.status());
        checkSameTask(current, next);
        check(Objects.equals(current.assignedNode(), next.assignedNode()), "a node cannot change the assigned node");
        check(current.epoch() == next.epoch(), "a node cannot change the epoch");
        check(current.attempt() == next.attempt(), "a node cannot change the attempt count");
        switch (next.status()) {
            case RUNNING -> check(next.runId() != null, "a running shard task needs a run id");
            // a node owns only running tasks (it claims a task by marking it running), so it completes or fails from there
            case COMPLETED, FAILED -> {
            }
            case UNASSIGNED -> throw new IllegalStateException("only the master can move a shard task to [" + next.status() + "]");
        }
    }

    private static void checkSameTask(ShardTaskDoc current, ShardTaskDoc next) {
        check(current.projectId().equals(next.projectId()), "the project cannot change");
        check(current.targetIndex().equals(next.targetIndex()), "the target index cannot change");
        check(current.targetIndexUuid().equals(next.targetIndexUuid()), "the target index uuid cannot change");
        check(current.params().equals(next.params()), "the shard task params cannot change");
        check(current.createdAt() == next.createdAt(), "the creation time cannot change");
        check(
            current.sourceShardId().getIndex().getUUID().equals(next.sourceShardId().getIndex().getUUID()),
            "the source index uuid cannot change"
        );
    }

    private static void check(boolean condition, String message, Object... args) {
        if (condition == false) {
            throw new IllegalStateException(args.length == 0 ? message : Strings.format(message, args));
        }
    }
}
