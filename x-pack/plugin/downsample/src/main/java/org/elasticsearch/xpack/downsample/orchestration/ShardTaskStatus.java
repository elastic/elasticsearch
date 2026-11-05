/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

/**
 * The status of the downsampling of one source shard. {@code UNASSIGNED} is set by the master and means any eligible node may
 * claim the task, {@code RUNNING} is set by the node that claims it, {@code COMPLETED} only by that node, and {@code FAILED} by
 * either.
 */
public enum ShardTaskStatus {
    UNASSIGNED,
    RUNNING,
    COMPLETED,
    FAILED;

    /**
     * Whether a node currently owns the shard task: the state a node resumes and may write from.
     */
    public boolean isOwnedByNode() {
        return switch (this) {
            case RUNNING -> true;
            case UNASSIGNED, COMPLETED, FAILED -> false;
        };
    }
}
