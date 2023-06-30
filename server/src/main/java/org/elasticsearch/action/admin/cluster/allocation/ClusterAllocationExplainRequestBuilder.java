/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Builder for requests to explain the allocation of a shard in the cluster
 */
public class ClusterAllocationExplainRequestBuilder extends MasterNodeOperationRequestBuilder<
    ClusterAllocationExplainRequest,
    ClusterAllocationExplainResponse,
    ClusterAllocationExplainRequestBuilder> {

    public ClusterAllocationExplainRequestBuilder(ElasticsearchClient client, ClusterAllocationExplainAction action) {
        super(client, action, new ClusterAllocationExplainRequest());
    }

    /** The index name to use when finding the shard to explain */
    public ClusterAllocationExplainRequestBuilder setIndex(String index) {
        request.setIndex(index);
        return this;
    }

    /** The shard number to use when finding the shard to explain */
    public ClusterAllocationExplainRequestBuilder setShard(int shard) {
        request.setShard(shard);
        return this;
    }

    /** Whether the primary or replica should be explained */
    public ClusterAllocationExplainRequestBuilder setPrimary(boolean primary) {
        request.setPrimary(primary);
        return this;
    }

    /** Whether to include "YES" decider decisions in the response instead of only "NO" decisions */
    public ClusterAllocationExplainRequestBuilder setIncludeYesDecisions(boolean includeYesDecisions) {
        request.includeYesDecisions(includeYesDecisions);
        return this;
    }

    /** Whether to include information about the gathered disk information of nodes in the cluster */
    public ClusterAllocationExplainRequestBuilder setIncludeDiskInfo(boolean includeDiskInfo) {
        request.includeDiskInfo(includeDiskInfo);
        return this;
    }

    /**
     * Requests the explain API to explain an already assigned replica shard currently allocated to
     * the given node.
     */
    public ClusterAllocationExplainRequestBuilder setCurrentNode(String currentNode) {
        request.setCurrentNode(currentNode);
        return this;
    }

}
