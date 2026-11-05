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
 *     <li>The master creates shard task documents, and (re)assigns, unassigns or fails them; each of these increments the
 *     epoch, which fences the node that held the task.</li>
 *     <li>A node may only write a shard task document assigned to it at the epoch it holds, may only move it to
 *     {@code RUNNING}, {@code COMPLETED} (from {@code RUNNING} only) or {@code FAILED}, and may not change its identity,
 *     owner, epoch or attempt.</li>
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
            case ASSIGNED -> check(next.assignedNode() != null, "an assigned shard task needs a node");
            case UNASSIGNED -> check(next.assignedNode() == null, "an unassigned shard task cannot have a node");
            case FAILED -> check(next.retriable() == false, "the master can only fail a shard task for good");
            case RUNNING, COMPLETED -> throw new IllegalStateException(
                "only the assigned node can move a shard task to [" + next.status() + "]"
            );
        }
    }

    /**
     * A node writes a shard task document.
     *
     * @param nodeId    the id of the node that writes
     * @param heldEpoch the epoch at which this node was given the task, i.e. the one its local run was started with
     */
    public static void validateNodeShardTaskWrite(String nodeId, long heldEpoch, ShardTaskDoc current, ShardTaskDoc next) {
        Objects.requireNonNull(current, "a node cannot create a shard task");
        check(nodeId.equals(current.assignedNode()), "shard task [%s] is not assigned to node [%s]", current.id(), nodeId);
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
            // the node marks the task running before it starts the indexer, so only a running task can complete
            case COMPLETED -> check(
                current.status() == ShardTaskStatus.RUNNING,
                "shard task [%s] is [%s], only a running shard task can complete",
                current.id(),
                current.status()
            );
            // a node can fail a task it has not marked running yet, e.g. when the source shard is already gone
            case FAILED -> {
            }
            case UNASSIGNED, ASSIGNED -> throw new IllegalStateException(
                "only the master can move a shard task to [" + next.status() + "]"
            );
        }
    }

    private static void checkSameTask(ShardTaskDoc current, ShardTaskDoc next) {
        check(current.projectId().equals(next.projectId()), "the project cannot change");
        check(current.targetIndex().equals(next.targetIndex()), "the target index cannot change");
        check(current.targetIndexUuid().equals(next.targetIndexUuid()), "the target index uuid cannot change");
        check(current.params().equals(next.params()), "the shard task params cannot change");
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
