/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.action.admin.cluster.state;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.internal.InternalClusterAdminClient;

/**
 * @author kimchy (shay.banon)
 */
public class ClusterStateRequestBuilder {

    private final InternalClusterAdminClient clusterClient;

    private final ClusterStateRequest request;

    public ClusterStateRequestBuilder(InternalClusterAdminClient clusterClient) {
        this.clusterClient = clusterClient;
        this.request = new ClusterStateRequest();
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.metadata.MetaData}. Defaults
     * to <tt>false</tt>.
     */
    public ClusterStateRequestBuilder setFilterMetaData(boolean filter) {
        request.filterMetaData(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.node.DiscoveryNodes}. Defaults
     * to <tt>false</tt>.
     */
    public ClusterStateRequestBuilder setFilterNodes(boolean filter) {
        request.filterNodes(filter);
        return this;
    }

    /**
     * Should the cluster state result include teh {@link org.elasticsearch.cluster.routing.RoutingTable}. Defaults
     * to <tt>false</tt>.
     */
    public ClusterStateRequestBuilder setFilterRoutingTable(boolean filter) {
        request.filterRoutingTable(filter);
        return this;
    }

    /**
     * When {@link #setFilterMetaData(boolean)} is not set, which indices to return the {@link org.elasticsearch.cluster.metadata.IndexMetaData}
     * for. Defaults to all indices.
     */
    public ClusterStateRequestBuilder setFilterIndices(String... indices) {
        request.filteredIndices(indices);
        return this;
    }

    /**
     * Executes the operation asynchronously and returns a future.
     */
    public ListenableActionFuture<ClusterStateResponse> execute() {
        PlainListenableActionFuture<ClusterStateResponse> future = new PlainListenableActionFuture<ClusterStateResponse>(request.listenerThreaded(), clusterClient.threadPool());
        clusterClient.state(request, future);
        return future;
    }

    /**
     * Executes the operation asynchronously with the provided listener.
     */
    public void execute(ActionListener<ClusterStateResponse> listener) {
        clusterClient.state(request, listener);
    }

}
