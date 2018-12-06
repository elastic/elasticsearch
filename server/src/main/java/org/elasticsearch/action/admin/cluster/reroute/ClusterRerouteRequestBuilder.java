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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;

/**
 * Builder for a cluster reroute request
 */
public class ClusterRerouteRequestBuilder
        extends AcknowledgedRequestBuilder<ClusterRerouteRequest, ClusterRerouteResponse, ClusterRerouteRequestBuilder> {
    public ClusterRerouteRequestBuilder(ElasticsearchClient client, ClusterRerouteAction action) {
        super(client, action, new ClusterRerouteRequest());
    }

    /**
     * Adds allocation commands to be applied to the cluster. Note, can be empty, in which case
     * will simply run a simple "reroute".
     */
    public ClusterRerouteRequestBuilder add(AllocationCommand... commands) {
        request.add(commands);
        return this;
    }

    /**
     * Sets a dry run flag (defaults to {@code false}) allowing to run the commands without
     * actually applying them to the cluster state, and getting the resulting cluster state back.
     */
    public ClusterRerouteRequestBuilder setDryRun(boolean dryRun) {
        request.dryRun(dryRun);
        return this;
    }

    /**
     * Sets the explain flag (defaults to {@code false}). If true, the
     * request will include an explanation in addition to the cluster state.
     */
    public ClusterRerouteRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    /**
     * Sets the retry failed flag (defaults to {@code false}). If true, the
     * request will retry allocating shards that can't currently be allocated due to too many allocation failures.
     */
    public ClusterRerouteRequestBuilder setRetryFailed(boolean retryFailed) {
        request.setRetryFailed(retryFailed);
        return this;
    }
}