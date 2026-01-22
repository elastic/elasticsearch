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
     * The shard is not assigned to any node; any data which it contains is unavailable to the cluster.
     * <p>
     * A shard transitions from {@link #UNASSIGNED} to {@link #INITIALIZING} when the master wants an assigned data node to create or start
     * recovering that shard copy. A shard may also transition back to {@link #UNASSIGNED} in case of any failure, during initialization or
     * later.
     */
    UNASSIGNED((byte) 1),

    /**
     * The shard is assigned to a node and the recovery process has begun. The shard data is not yet available on the node initializing the
     * shard (though there is a small window of time when the data is available, after recovery completes and before the cluster state is
     * updated to mark the shard {@link #STARTED}).
     * <p>
     * A shard transitions from {@link #INITIALIZING} -> {@link #STARTED} when recovery is complete and the data node informs the master
     * that it is ready to serve requests.
     */
    INITIALIZING((byte) 2),

    /**
     * The shard is assigned to a specific data node and ready to accept indexing and search requests.
     * <p>
     * A shard transitions from {@link #STARTED} -> {@link #RELOCATING} when the master wants to initialize the shard elsewhere.
     */
    STARTED((byte) 3),

    /**
     * The shard is being reassigned away from one node to another node. This is the state of the shard on a source node when the shard is
     * being moved away to a new target node. The target shard copy will be {@link #INITIALIZING}.
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
