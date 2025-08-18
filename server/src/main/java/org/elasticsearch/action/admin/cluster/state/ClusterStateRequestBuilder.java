/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;

public class ClusterStateRequestBuilder extends ActionRequestBuilder<ClusterStateRequest, ClusterStateResponse> {

    public ClusterStateRequestBuilder(ElasticsearchClient client, TimeValue masterNodeTimeout) {
        super(client, ClusterStateAction.INSTANCE, new ClusterStateRequest(masterNodeTimeout));
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
     * Should the cluster state result include the {@link Metadata}. Defaults
     * to {@code true}.
     */
    public ClusterStateRequestBuilder setMetadata(boolean filter) {
        request.metadata(filter);
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
     * When {@link #setMetadata(boolean)} is set, which indices to return the {@link org.elasticsearch.cluster.metadata.IndexMetadata}
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
     * @param waitForMetadataVersion The metadata version for which to wait
     */
    public ClusterStateRequestBuilder setWaitForMetadataVersion(long waitForMetadataVersion) {
        request.waitForMetadataVersion(waitForMetadataVersion);
        return this;
    }

    /**
     * If {@link ClusterStateRequest#waitForMetadataVersion()} is set then this determines how long to wait
     */
    public ClusterStateRequestBuilder setWaitForTimeOut(TimeValue waitForTimeout) {
        request.waitForTimeout(waitForTimeout);
        return this;
    }
}
