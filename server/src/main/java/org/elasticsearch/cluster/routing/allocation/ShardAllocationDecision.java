/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents the decision taken for the allocation of a single shard.  If
 * the shard is unassigned, {@link #getAllocateDecision()} will return an
 * object containing the decision and its explanation, and {@link #getMoveDecision()}
 * will return an object for which {@link MoveDecision#isDecisionTaken()} returns
 * {@code false}.  If the shard is in the started state, then {@link #getMoveDecision()}
 * will return an object containing the decision to move/rebalance the shard, and
 * {@link #getAllocateDecision()} will return an object for which
 * {@link AllocateUnassignedDecision#isDecisionTaken()} returns {@code false}.  If
 * the shard is neither unassigned nor started (i.e. it is initializing or relocating),
 * then both {@link #getAllocateDecision()} and {@link #getMoveDecision()} will return
 * objects whose {@code isDecisionTaken()} method returns {@code false}.
 */
public final class ShardAllocationDecision implements ToXContentFragment, Writeable {
    public static final ShardAllocationDecision NOT_TAKEN = new ShardAllocationDecision(
        AllocateUnassignedDecision.NOT_TAKEN,
        MoveDecision.NOT_TAKEN
    );

    private final AllocateUnassignedDecision allocateDecision;
    private final MoveDecision moveDecision;

    public ShardAllocationDecision(AllocateUnassignedDecision allocateDecision, MoveDecision moveDecision) {
        this.allocateDecision = allocateDecision;
        this.moveDecision = moveDecision;
    }

    public ShardAllocationDecision(StreamInput in) throws IOException {
        allocateDecision = new AllocateUnassignedDecision(in);
        moveDecision = new MoveDecision(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        allocateDecision.writeTo(out);
        moveDecision.writeTo(out);
    }

    /**
     * Returns {@code true} if either an allocation decision or a move decision was taken
     * for the shard.  If no decision was taken, as in the case of initializing or relocating
     * shards, then this method returns {@code false}.
     */
    public boolean isDecisionTaken() {
        return allocateDecision.isDecisionTaken() || moveDecision.isDecisionTaken();
    }

    /**
     * Gets the unassigned allocation decision for the shard.  If the shard was not in the unassigned state,
     * the instance of {@link AllocateUnassignedDecision} that is returned will have {@link AllocateUnassignedDecision#isDecisionTaken()}
     * return {@code false}.
     */
    public AllocateUnassignedDecision getAllocateDecision() {
        return allocateDecision;
    }

    /**
     * Gets the move decision for the shard.  If the shard was not in the started state,
     * the instance of {@link MoveDecision} that is returned will have {@link MoveDecision#isDecisionTaken()}
     * return {@code false}.
     */
    public MoveDecision getMoveDecision() {
        return moveDecision;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (allocateDecision.isDecisionTaken()) {
            allocateDecision.toXContent(builder, params);
        }
        if (moveDecision.isDecisionTaken()) {
            moveDecision.toXContent(builder, params);
        }
        return builder;
    }

}
