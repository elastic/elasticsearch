/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.function.Supplier;

public class ShardRoutingRoleStrategyHelper {

    public static ShardRoutingRoleStrategy createTestShardRoutingRoleStrategy(Supplier<Integer> numIndexingCopies) {
        return new ShardRoutingRoleStrategy() {
            @Override
            public ShardRouting.Role newReplicaRole(ShardRouting.Role primaryRole) {
                return primaryRole == ShardRouting.Role.INDEX_ONLY ? ShardRouting.Role.SEARCH_ONLY : ShardRouting.Role.GETS_ONLY;
            }

            @Override
            public ShardRouting.Role newEmptyRole(int copyIndex, IndexMetadata indexMetadata) {
                assert 0 < numIndexingCopies.get();
                return copyIndex < numIndexingCopies.get()
                    ? (copyIndex % 2 == 0 ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.INDEX_SEARCH)
                    : (copyIndex % 2 == 0 ? ShardRouting.Role.SEARCH_ONLY : ShardRouting.Role.GETS_ONLY);
            }
        };
    }

}
