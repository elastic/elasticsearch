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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 *
 */
public class ClusterStateRequestBuilder extends MasterNodeReadOperationRequestBuilder<ClusterStateRequest, ClusterStateResponse, ClusterStateRequestBuilder, ClusterAdminClient> {

    public ClusterStateRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new ClusterStateRequest());
    }

    /**
     * Include all data
     */
    public ClusterStateRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Do not include any data
     */
    public ClusterStateRequestBuilder clear() {
        request.clear();
        return this;
    }

    public ClusterStateRequestBuilder setBlocks(boolean filter) {
        request.blocks(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.metadata.MetaData}. Defaults
     * to <tt>true</tt>.
     */
    public ClusterStateRequestBuilder setMetaData(boolean filter) {
        request.metaData(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.node.DiscoveryNodes}. Defaults
     * to <tt>true</tt>.
     */
    public ClusterStateRequestBuilder setNodes(boolean filter) {
        request.nodes(filter);
        return this;
    }

    /**
     * Should the cluster state result include teh {@link org.elasticsearch.cluster.routing.RoutingTable}. Defaults
     * to <tt>true</tt>.
     */
    public ClusterStateRequestBuilder setRoutingTable(boolean filter) {
        request.routingTable(filter);
        return this;
    }

    /**
     * When {@link #setMetaData(boolean)} is set, which indices to return the {@link org.elasticsearch.cluster.metadata.IndexMetaData}
     * for. Defaults to all indices.
     */
    public ClusterStateRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ClusterStateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<ClusterStateResponse> listener) {
        client.state(request, listener);
    }
}
