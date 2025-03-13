/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public class NodesUsageRequest extends BaseNodesRequest {

    private boolean restActions;
    private boolean aggregations;

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
}
