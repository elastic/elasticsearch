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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;

public class ClusterStateRequestBuilder extends MasterNodeReadOperationRequestBuilder<ClusterStateRequest,
        ClusterStateResponse, ClusterStateRequestBuilder> {

    public ClusterStateRequestBuilder(ElasticsearchClient client, ClusterStateAction action) {
        super(client, action, new ClusterStateRequest());
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
     * to {@code true}.
     */
    public ClusterStateRequestBuilder setMetaData(boolean filter) {
        request.metaData(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.node.DiscoveryNodes}. Defaults
     * to {@code true}.
     */
    public ClusterStateRequestBuilder setNodes(boolean filter) {
        request.nodes(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.ClusterState.Custom}. Defaults
     * to {@code true}.
     */
    public ClusterStateRequestBuilder setCustoms(boolean filter) {
        request.customs(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.elasticsearch.cluster.routing.RoutingTable}. Defaults
     * to {@code true}.
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

    /**
     * Causes the request to wait for the metadata version to advance to at least the given version.
     * @param waitForMetaDataVersion The metadata version for which to wait
     */
    public ClusterStateRequestBuilder setWaitForMetaDataVersion(long waitForMetaDataVersion) {
        request.waitForMetaDataVersion(waitForMetaDataVersion);
        return this;
    }

    /**
     * If {@link ClusterStateRequest#waitForMetaDataVersion()} is set then this determines how long to wait
     */
    public ClusterStateRequestBuilder setWaitForTimeOut(TimeValue waitForTimeout) {
        request.waitForTimeout(waitForTimeout);
        return this;
    }
}
