/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;

/**
 * Builder for a cluster reroute request
 */
public class ClusterRerouteRequestBuilder extends AcknowledgedRequestBuilder<
    ClusterRerouteRequest,
    ClusterRerouteResponse,
    ClusterRerouteRequestBuilder> {
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
