/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Priority;

/**
 * Asynchronously performs a cluster reroute, updating any shard states and rebalancing the cluster if appropriate.
 */
@FunctionalInterface
public interface RerouteService {

    /**
     * Schedule a cluster reroute.
     * @param priority the (minimum) priority at which to run this reroute. If there is already a pending reroute at a higher priority then
     *                 this reroute is batched with the pending one; if there is already a pending reroute at a lower priority then
     *                 the priority of the pending batch is raised to the given priority.
     */
    void reroute(String reason, Priority priority, ActionListener<ClusterState> listener);
}
