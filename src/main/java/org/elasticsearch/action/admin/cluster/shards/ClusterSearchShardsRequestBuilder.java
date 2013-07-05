/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.internal.InternalClusterAdminClient;

/**
 */
public class ClusterSearchShardsRequestBuilder extends MasterNodeOperationRequestBuilder<ClusterSearchShardsRequest, ClusterSearchShardsResponse, ClusterSearchShardsRequestBuilder> {

    public ClusterSearchShardsRequestBuilder(ClusterAdminClient clusterClient) {
        super((InternalClusterAdminClient) clusterClient, new ClusterSearchShardsRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public ClusterSearchShardsRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public ClusterSearchShardsRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore. For example indices that don't exist.
     */
    public ClusterSearchShardsRequestBuilder setIgnoreIndices(IgnoreIndices ignoreIndices) {
        request().ignoreIndices(ignoreIndices);
        return this;
    }

    /**
     * Specifies if request should be executed on local node rather than on master.
     */
    public ClusterSearchShardsRequestBuilder setLocal(boolean local) {
        request().local(local);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<ClusterSearchShardsResponse> listener) {
        ((ClusterAdminClient) client).searchShards(request, listener);
    }

}
