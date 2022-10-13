/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodesUsageRequest extends BaseNodesRequest<NodesUsageRequest> {

    private boolean restActions;
    private boolean aggregations;

    public NodesUsageRequest(StreamInput in) throws IOException {
        super(in);
        this.restActions = in.readBoolean();
        this.aggregations = in.readBoolean();
    }

    /**
     * Get usage from nodes based on the nodes ids specified. If none are
     * passed, usage for all nodes will be returned.
     */
    public NodesUsageRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Sets all the request flags.
     */
    public NodesUsageRequest all() {
        this.restActions = true;
        this.aggregations = true;
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesUsageRequest clear() {
        this.restActions = false;
        return this;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public boolean restActions() {
        return this.restActions;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public NodesUsageRequest restActions(boolean restActions) {
        this.restActions = restActions;
        return this;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public boolean aggregations() {
        return this.aggregations;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public NodesUsageRequest aggregations(boolean aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(restActions);
        out.writeBoolean(aggregations);
    }
}
