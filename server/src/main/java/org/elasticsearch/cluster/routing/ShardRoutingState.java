/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

/**
 * Represents the current state of a {@link ShardRouting} as defined by the
 * cluster.
 */
public enum ShardRoutingState {
    /**
     * The shard is not assigned to any node.
     */
    UNASSIGNED((byte) 1),
    /**
     * The shard is initializing (probably recovering from either a peer shard
     * or gateway).
     */
    INITIALIZING((byte) 2),
    /**
     * The shard is started.
     */
    STARTED((byte) 3),
    /**
     * The shard is in the process being relocated.
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
