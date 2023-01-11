/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;

public class TestShardRoutingRoleStrategies {

    public static final ShardRoutingRoleStrategy DEFAULT_ROLE_ONLY = new ShardRoutingRoleStrategy() {
        @Override
        public ShardRouting.Role newReplicaRole() {
            return ShardRouting.Role.DEFAULT;
        }

        @Override
        public ShardRouting.Role newEmptyRole(int copyIndex) {
            return ShardRouting.Role.DEFAULT;
        }
    };

    private TestShardRoutingRoleStrategies() {
        // no instances
    }
}
