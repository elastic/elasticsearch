/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;

/**
 * Cluster state update request that allows to close one or more indices
 */
public class CloseIndexClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<CloseIndexClusterStateUpdateRequest> {

    private long taskId;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public CloseIndexClusterStateUpdateRequest(final long taskId) {
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }

    public CloseIndexClusterStateUpdateRequest taskId(final long taskId) {
        this.taskId = taskId;
        return this;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    public CloseIndexClusterStateUpdateRequest waitForActiveShards(final ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }
}
