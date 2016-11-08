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

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;

/**
 * A simple immutable container class that comprises a cluster state and cluster state status. Used by {@link ClusterService}
 * to provide a snapshot view on which cluster state is currently being applied / already applied.
 */
public class ClusterServiceState {
    private final ClusterState clusterState;
    private final ClusterStateStatus clusterStateStatus;

    public ClusterServiceState(ClusterState clusterState, ClusterStateStatus clusterStateStatus) {
        this.clusterState = clusterState;
        this.clusterStateStatus = clusterStateStatus;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public ClusterStateStatus getClusterStateStatus() {
        return clusterStateStatus;
    }

    @Override
    public String toString() {
        return "version [" + clusterState.version() + "], status [" + clusterStateStatus + "]";
    }
}
