/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

public class DesiredBalanceSimulationTests extends ESAllocationTestCase {

    public void testDesiredBalanceFromDesiredBalanceOutput() {

        var allocator = createDesiredBalanceShardsAllocator(Settings.EMPTY);

        var allocation = new RoutingAllocation(
            randomAllocationDeciders(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings()),
            parseMinimizedClusterState(),
            parseClusterInfo(),
            SnapshotShardSizeInfo.EMPTY,
            0L
        );

        allocator.allocate(allocation, ActionListener.noop());

        logger.info("Result routing nodes {}", allocation.routingNodes());
    }

    private static ClusterInfo parseClusterInfo() {
        return ClusterInfo.EMPTY;
    }

    private static ClusterState parseMinimizedClusterState() {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }
}
