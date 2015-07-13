/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.routing;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 */
public class TestShardRouting {

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, null, null, primary, state, version, null, true);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, null, primary, state, version, null, true);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version, null, true);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId,
                                               String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version,
                                               UnassignedInfo unassignedInfo) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version, unassignedInfo, true);
    }
}
