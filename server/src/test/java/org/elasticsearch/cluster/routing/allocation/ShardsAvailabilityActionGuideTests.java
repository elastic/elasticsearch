/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_CHECK_ALLOCATION_EXPLAIN_API;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_INDEX_ROUTING_ALLOCATION;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_NODE_CAPACITY;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_RESTORE_FROM_SNAPSHOT;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.DIAGNOSE_SHARDS_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ENABLE_INDEX_ALLOCATION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ENABLE_TIER_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.INCREASE_SHARD_LIMIT_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.MIGRATE_TO_TIERS_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.RESTORE_FROM_SNAPSHOT_ACTION_GUIDE;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.TIER_CAPACITY_ACTION_GUIDE;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardsAvailabilityActionGuideTests extends ESTestCase {

    private final ShardsAvailabilityHealthIndicatorService service;

    public ShardsAvailabilityActionGuideTests() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        service = new ShardsAvailabilityHealthIndicatorService(clusterService, mock(AllocationService.class), mock(SystemIndices.class));
    }

    public void testRestoreFromSnapshotAction() {
        assertThat(ACTION_RESTORE_FROM_SNAPSHOT.helpURL(), is(RESTORE_FROM_SNAPSHOT_ACTION_GUIDE));
    }

    public void testDiagnoseShardsAction() {
        assertThat(ACTION_CHECK_ALLOCATION_EXPLAIN_API.helpURL(), is(DIAGNOSE_SHARDS_ACTION_GUIDE));
    }

    public void testEnableIndexAllocation() {
        assertThat(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.helpURL(), is(ENABLE_INDEX_ALLOCATION_GUIDE));
    }

    public void testDiagnoseEnableClusterAllocation() {
        assertThat(ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION.helpURL(), is(ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE));
    }

    public void testEnableIndexRoutingAllocation() {
        assertThat(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.helpURL(), is(ENABLE_INDEX_ALLOCATION_GUIDE));
    }

    public void testEnableClusterRoutingAllocation() {
        assertThat(ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION.helpURL(), is(ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE));
    }

    public void testEnableDataTiers() {
        assertThat(service.getAddNodesWithRoleAction(DataTier.DATA_HOT).helpURL(), is(ENABLE_TIER_ACTION_GUIDE));
    }

    public void testIncreaseShardLimitIndexSettingInTier() {
        assertThat(service.getIncreaseShardLimitIndexSettingAction(DataTier.DATA_HOT).helpURL(), is(INCREASE_SHARD_LIMIT_ACTION_GUIDE));
        assertThat(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING.helpURL(), is(INCREASE_SHARD_LIMIT_ACTION_GUIDE));
    }

    public void testIncreaseShardLimitClusterSettingInTier() {
        assertThat(
            service.getIncreaseShardLimitClusterSettingAction(DataTier.DATA_HOT).helpURL(),
            is(INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE)
        );
        assertThat(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING.helpURL(), is(INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE));
    }

    public void testMigrateDataRequiredToDataTiers() {
        assertThat(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA.helpURL(), is(MIGRATE_TO_TIERS_ACTION_GUIDE));
        assertThat(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA.helpURL(), is(MIGRATE_TO_TIERS_ACTION_GUIDE));
    }

    public void testIncreaseTierCapacity() {
        assertThat(ACTION_INCREASE_NODE_CAPACITY.helpURL(), is(TIER_CAPACITY_ACTION_GUIDE));
    }
}
