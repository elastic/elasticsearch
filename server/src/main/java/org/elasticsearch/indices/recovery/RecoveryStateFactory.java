/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

/// Factory that creates a new [RecoveryState] instance per shard.
///
/// The default implementation is `RecoveryState::new`. Plugins can supply alternative implementations via
/// [org.elasticsearch.plugins.IndexStorePlugin#getRecoveryStateFactories].
@FunctionalInterface
public interface RecoveryStateFactory {
    /// Creates a new [RecoveryState] for the given shard. Called once per shard at creation time.
    ///
    /// @param shardRouting the routing entry for the shard being created
    /// @param localNode    the node on which the shard is being created
    /// @param sourceNode   the node from which the shard is being recovered, or `null` for non-peer recoveries
    RecoveryState newRecoveryState(ShardRouting shardRouting, DiscoveryNode localNode, @Nullable DiscoveryNode sourceNode);
}
