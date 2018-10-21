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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for requests to explain the allocation of a shard in the cluster
 */
public class ClusterAllocationExplainRequestBuilder
        extends MasterNodeOperationRequestBuilder<ClusterAllocationExplainRequest,
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

    /**
     * Signal that the first unassigned shard should be used
     */
    public ClusterAllocationExplainRequestBuilder useAnyUnassignedShard() {
        request.setIndex(null);
        request.setShard(null);
        request.setPrimary(null);
        return this;
    }

}
