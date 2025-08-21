/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class GatewayServiceIT extends ESIntegTestCase {

    public static final Setting<Boolean> TEST_SETTING = Setting.boolSetting(
        "gateway.test.setting",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String ALLOCATOR_NAME = "test-shards-allocator";

    public static class TestPlugin extends Plugin implements ClusterPlugin {

        private final AtomicBoolean settingApplied = new AtomicBoolean();

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(TEST_SETTING);
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestAllocationDecider(settings, clusterSettings, settingApplied));
        }

        @Override
        public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
            return Map.of(ALLOCATOR_NAME, new ExistingShardsAllocator() {
                @Override
                public void beforeAllocation(RoutingAllocation allocation) {
                    if (allocation.globalRoutingTable().routingTables().values().stream().anyMatch(table -> table.iterator().hasNext())) {
                        // state is recovered so we must have applied the setting
                        assertTrue(settingApplied.get());
                    }
                }

                @Override
                public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}

                @Override
                public void allocateUnassigned(
                    ShardRouting shardRouting,
                    RoutingAllocation allocation,
                    UnassignedAllocationHandler unassignedAllocationHandler
                ) {

                }

                @Override
                public AllocateUnassignedDecision explainUnassignedShardAllocation(
                    ShardRouting unassignedShard,
                    RoutingAllocation routingAllocation
                ) {
                    return AllocateUnassignedDecision.NOT_TAKEN;
                }

                @Override
                public void cleanCaches() {}

                @Override
                public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

                @Override
                public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

                @Override
                public int getNumberOfInFlightFetches() {
                    return 0;
                }
            });
        }
    }

    private static class TestAllocationDecider extends AllocationDecider {
        TestAllocationDecider(Settings settings, ClusterSettings clusterSettings, AtomicBoolean settingApplied) {
            if (TEST_SETTING.get(settings)) {
                settingApplied.set(true);
            } else {
                clusterSettings.addSettingsUpdateConsumer(TEST_SETTING, b -> settingApplied.set(true));
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(TEST_SETTING.getKey(), true).build();
    }

    public void testSettingsAppliedBeforeReroute() throws Exception {
        updateClusterSettings(Settings.builder().put(TEST_SETTING.getKey(), true));

        createIndex("test-index");

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(TEST_SETTING.getKey(), false).build();
            }
        });

        ensureGreen();

    }

}
