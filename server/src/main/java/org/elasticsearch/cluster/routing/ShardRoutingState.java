/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

/**
 * Tracks the current assignment status of a particular shard copy ({@link ShardRouting}).
 */
public enum ShardRoutingState {
    /**
     * The shard is not assigned to any node; any data which it contains is unavailable in the cluster.
     */
    UNASSIGNED((byte) 1),
    /**
     * The shard is assigned to a node and the recovery process has begun. The shard data is not yet available on the node initializing the
     * shard.
     */
    INITIALIZING((byte) 2),
    /**
     * The shard is assigned to a specific data node and ready to accept indexing and search requests.
     */
    STARTED((byte) 3),
    /**
     * The shard is being reassigned away from one node to another node. This is the state of the shard on a source node when the shard is
     * being moved away to a target node. The target node will be initializing the shard and running recovery ({@link #INITIALIZING}).
     */
    RELOCATING((byte) 4);

    private final byte value;

    ShardRoutingState(byte value) {
        this.value = value;
    }

    /**
     * Byte value of this {@link ShardRoutingState}
     * @return Byte value of this {@link ShardRoutingState}
     */
    public byte value() {
        return this.value;
    }

    public static ShardRoutingState fromValue(byte value) {
        return switch (value) {
            case 1 -> UNASSIGNED;
            case 2 -> INITIALIZING;
            case 3 -> STARTED;
            case 4 -> RELOCATING;
            default -> throw new IllegalStateException("No routing state mapped for [" + value + "]");
        };
    }
}
