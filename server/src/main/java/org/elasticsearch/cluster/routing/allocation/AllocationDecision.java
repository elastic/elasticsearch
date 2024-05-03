/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * An enum which represents the various decision types that can be taken by the
 * allocators and deciders for allocating a shard to a node.
 */
public enum AllocationDecision implements Writeable {
    /**
     * The shard can be allocated to a node.
     */
    YES((byte) 0),
    /**
     * The allocation attempt was throttled for the shard.
     */
    THROTTLED((byte) 1),
    /**
     * The shard cannot be allocated, which can happen for any number of reasons,
     * including the allocation deciders gave a NO decision for allocating.
     */
    NO((byte) 2),
    /**
     * The shard could not be rebalanced to another node despite rebalancing
     * being allowed, because moving the shard to the other node would not form
     * a better cluster balance.
     */
    WORSE_BALANCE((byte) 3),
    /**
     * Waiting on getting shard data from all nodes before making a decision
     * about where to allocate the shard.
     */
    AWAITING_INFO((byte) 4),
    /**
     * The allocation decision has been delayed waiting for a replica with a shard copy
     * that left the cluster to rejoin.
     */
    ALLOCATION_DELAYED((byte) 5),
    /**
     * The shard was denied allocation because there were no valid shard copies
     * found for it amongst the nodes in the cluster.
     */
    NO_VALID_SHARD_COPY((byte) 6),
    /**
     * No attempt was made to allocate the shard
     */
    NO_ATTEMPT((byte) 7);

    private final byte id;

    AllocationDecision(byte id) {
        this.id = id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }

    public static AllocationDecision readFrom(StreamInput in) throws IOException {
        byte id = in.readByte();
        return switch (id) {
            case 0 -> YES;
            case 1 -> THROTTLED;
            case 2 -> NO;
            case 3 -> WORSE_BALANCE;
            case 4 -> AWAITING_INFO;
            case 5 -> ALLOCATION_DELAYED;
            case 6 -> NO_VALID_SHARD_COPY;
            case 7 -> NO_ATTEMPT;
            default -> throw new IllegalArgumentException("Unknown value [" + id + "]");
        };
    }

    /**
     * Gets an {@link AllocationDecision} from a {@link AllocationStatus}.
     */
    public static AllocationDecision fromAllocationStatus(AllocationStatus allocationStatus) {
        if (allocationStatus == null) {
            return YES;
        } else {
            return switch (allocationStatus) {
                case DECIDERS_THROTTLED -> THROTTLED;
                case FETCHING_SHARD_DATA -> AWAITING_INFO;
                case DELAYED_ALLOCATION -> ALLOCATION_DELAYED;
                case NO_VALID_SHARD_COPY -> NO_VALID_SHARD_COPY;
                case NO_ATTEMPT -> NO_ATTEMPT;
                case DECIDERS_NO -> NO;
            };
        }
    }

    /**
     * Gets an {@link AllocationDecision} from a {@link Decision.Type}
     */
    public static AllocationDecision fromDecisionType(Decision.Type type) {
        return switch (type) {
            case YES -> YES;
            case THROTTLE -> THROTTLED;
            case NO -> NO;
        };
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

}
