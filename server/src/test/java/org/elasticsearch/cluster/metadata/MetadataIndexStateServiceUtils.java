/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.index.Index;

import java.util.Map;

public class MetadataIndexStateServiceUtils {

    private MetadataIndexStateServiceUtils() {}

    /**
     * Allows to call {@link MetadataIndexStateService#addIndexClosedBlocks(Index[], Map, ClusterState)} which is a protected method.
     */
    public static ClusterState addIndexClosedBlocks(
        final Index[] indices,
        final Map<Index, ClusterBlock> blockedIndices,
        final ClusterState state
    ) {
        return MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
    }

    /**
     * Allows to call {@link MetadataIndexStateService#closeRoutingTable} which is a protected method.
     */
    public static ClusterState closeRoutingTable(
        final ClusterState state,
        final Map<Index, ClusterBlock> blockedIndices,
        final Map<Index, CloseIndexResponse.IndexResult> results
    ) {
        return MetadataIndexStateService.closeRoutingTable(state, blockedIndices, results, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .v1();
    }
}
