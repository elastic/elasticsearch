/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.index.Index;

/**
 * Cluster state update request that allows re-sharding an index
 * At the moment, we only have the ability to increment the number of shards
 * of an index (by a multiplicative factor).
 * We do not support removing shards from an index.
 */
public class ReshardIndexClusterStateUpdateRequest {
    private final Index index;
    private final ProjectId projectId;
    private final int newShardCount;

    public ReshardIndexClusterStateUpdateRequest(ProjectId projectId, Index index, int newShardCount) {
        this.projectId = projectId;
        this.index = index;
        this.newShardCount = newShardCount;
    }

    public ProjectId projectId() {
        return projectId;
    }

    public Index index() {
        return index;
    }

    public int getNewShardCount() {
        return newShardCount;
    }

    @Override
    public String toString() {
        return "ReshardIndexClusterStateUpdateRequest{" + "index=" + index + ", projectId=" + projectId + '}';
    }
}
