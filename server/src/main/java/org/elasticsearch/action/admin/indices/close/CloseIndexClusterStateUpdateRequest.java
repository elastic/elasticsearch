/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
