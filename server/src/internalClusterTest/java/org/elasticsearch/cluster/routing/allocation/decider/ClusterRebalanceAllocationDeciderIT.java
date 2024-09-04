/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class ClusterRebalanceAllocationDeciderIT extends ESIntegTestCase {
    public void testDefault() {
        internalCluster().startNode();
        assertEquals(
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS,
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.get(
                internalCluster().getInstance(ClusterService.class).getSettings()
            )
        );
    }

    public void testDefaultLegacyAllocator() {
        internalCluster().startNode(
            Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.BALANCED_ALLOCATOR)
        );
        assertEquals(
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE,
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.get(
                internalCluster().getInstance(ClusterService.class).getSettings()
            )
        );
    }
}
