/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;

public class StatelessShardRoutingRoleStrategy implements ShardRoutingRoleStrategy {

    @Override
    public ShardRouting.Role newEmptyRole(int copyIndex) {
        return copyIndex == 0 ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
    }

    @Override
    public ShardRouting.Role newReplicaRole() {
        return ShardRouting.Role.SEARCH_ONLY;
    }
}
