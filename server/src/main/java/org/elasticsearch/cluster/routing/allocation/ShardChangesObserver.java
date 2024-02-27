/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;

public class ShardChangesObserver implements RoutingChangesObserver {

    private static final Logger logger = LogManager.getLogger(ShardChangesObserver.class);

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        logger.info("{} started on node {}", shardIdentifier(startedShard), startedShard.currentNodeId());
    }

    @Override
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {
        // TODO distinguish between move and rebalance
        logger.info("{} is relocating from {} to {}", shardIdentifier(startedShard), startedShard.currentNodeId(), targetRelocatingShard.currentNodeId());
    }

    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        logger.info("{} has failed on {}: {}", shardIdentifier(failedShard), failedShard.currentNodeId(), unassignedInfo.getReason());
    }

    @Override
    public void replicaPromoted(ShardRouting replicaShard) {
        logger.info("{} is promoted to primary on {}", shardIdentifier(replicaShard), replicaShard.currentNodeId());
    }

    private static String shardIdentifier(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + '[' + (shardRouting.primary() ? 'P' : 'R') + ']';
    }
}
