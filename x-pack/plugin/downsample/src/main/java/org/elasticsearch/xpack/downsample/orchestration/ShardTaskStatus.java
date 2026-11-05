/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

/**
 * The status of the downsampling of one source shard. {@code UNASSIGNED} and {@code ASSIGNED} are set by the master,
 * {@code RUNNING} and {@code COMPLETED} only by the assigned node, and {@code FAILED} by either.
 */
public enum ShardTaskStatus {
    UNASSIGNED,
    ASSIGNED,
    RUNNING,
    COMPLETED,
    FAILED;

    /**
     * Whether a node may currently own the shard task: these are the states a node looks for and may write from.
     */
    public boolean isOwnedByNode() {
        return switch (this) {
            case ASSIGNED, RUNNING -> true;
            case UNASSIGNED, COMPLETED, FAILED -> false;
        };
    }
}
