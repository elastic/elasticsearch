/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GatewayAllocatorObserver implements RoutingChangesObserver {

    @Nullable // if not tracking
    private Map<ShardRouting, String> gatewayAllocations = null;

    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        if (gatewayAllocations == null) {
            return;
        }
        final var previousNode = gatewayAllocations.put(unassignedShard, initializedShard.currentNodeId());
        assert previousNode == null : "double-initializing " + unassignedShard + " to " + previousNode + " and " + initializedShard;
    }

    public void startTracking() {
        assert gatewayAllocations == null;
        gatewayAllocations = new HashMap<>();
    }

    public Map<ShardRouting, String> stopTracking() {
        assert gatewayAllocations != null;
        final var gatewayAllocations = this.gatewayAllocations;
        this.gatewayAllocations = null;
        return Collections.unmodifiableMap(gatewayAllocations);
    }
}
