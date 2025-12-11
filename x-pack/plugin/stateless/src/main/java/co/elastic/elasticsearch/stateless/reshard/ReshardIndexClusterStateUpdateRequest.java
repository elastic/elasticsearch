/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

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
