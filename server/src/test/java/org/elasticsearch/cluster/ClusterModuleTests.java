/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeReplacementAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeShutdownAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ClusterModuleTests extends ModuleTestCase {
    private ClusterInfoService clusterInfoService = EmptyClusterInfoService.INSTANCE;
    private ClusterService clusterService;
    private static ThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @AfterClass
    public static void terminateThreadPool() {
        assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        threadPool = null;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            (TaskManager) null
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    static class FakeAllocationDecider extends AllocationDecider {
        protected FakeAllocationDecider() {}
    }

    static class FakeShardsAllocator implements ShardsAllocator {
        @Override
        public void allocate(RoutingAllocation allocation) {
            // noop
        }

        @Override
        public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
            throw new UnsupportedOperationException("explain API not supported on FakeShardsAllocator");
        }
    }

    public void testRegisterClusterDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(
                e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice"
            );
        }
    }

    public void testRegisterClusterDynamicSetting() {
        SettingsModule module = new SettingsModule(
            Settings.EMPTY,
            Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope)
        );
        assertInstanceBinding(module, ClusterSettings.class, service -> service.isDynamicSetting("foo.bar"));
    }

    public void testRegisterIndexDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(
                e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice"
            );
        }
    }

    public void testRegisterIndexDynamicSetting() {
        SettingsModule module = new SettingsModule(
            Settings.EMPTY,
            Setting.boolSetting("index.foo.bar", false, Property.Dynamic, Property.IndexScope)
        );
        assertInstanceBinding(module, IndexScopedSettings.class, service -> service.isDynamicSetting("index.foo.bar"));
    }

    public void testRegisterAllocationDeciderDuplicate() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterModule(Settings.EMPTY, clusterService, Collections.<ClusterPlugin>singletonList(new ClusterPlugin() {
                @Override
                public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                    return Collections.singletonList(new EnableAllocationDecider(clusterSettings));
                }
            }), clusterInfoService, null, threadPool, EmptySystemIndices.INSTANCE, WriteLoadForecaster.DEFAULT)
        );
        assertEquals(e.getMessage(), "Cannot specify allocation decider [" + EnableAllocationDecider.class.getName() + "] twice");
    }

    public void testRegisterAllocationDecider() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService, Collections.singletonList(new ClusterPlugin() {
            @Override
            public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                return Collections.singletonList(new FakeAllocationDecider());
            }
        }), clusterInfoService, null, threadPool, EmptySystemIndices.INSTANCE, WriteLoadForecaster.DEFAULT);
        assertTrue(module.deciderList.stream().anyMatch(d -> d.getClass().equals(FakeAllocationDecider.class)));
    }

    private ClusterModule newClusterModuleWithShardsAllocator(Settings settings, String name, Supplier<ShardsAllocator> supplier) {
        return new ClusterModule(settings, clusterService, Collections.singletonList(new ClusterPlugin() {
            @Override
            public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
                return Collections.singletonMap(name, supplier);
            }
        }), clusterInfoService, null, threadPool, EmptySystemIndices.INSTANCE, WriteLoadForecaster.DEFAULT);
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "custom").build();
        ClusterModule module = newClusterModuleWithShardsAllocator(settings, "custom", FakeShardsAllocator::new);
        assertEquals(FakeShardsAllocator.class, module.shardsAllocator.getClass());
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> newClusterModuleWithShardsAllocator(Settings.EMPTY, ClusterModule.BALANCED_ALLOCATOR, FakeShardsAllocator::new)
        );
        assertEquals("ShardsAllocator [" + ClusterModule.BALANCED_ALLOCATOR + "] already defined", e.getMessage());
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterModule(
                settings,
                clusterService,
                Collections.emptyList(),
                clusterInfoService,
                null,
                threadPool,
                EmptySystemIndices.INSTANCE,
                WriteLoadForecaster.DEFAULT
            )
        );
        assertEquals("Unknown ShardsAllocator [dne]", e.getMessage());
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testShardsAllocatorFactoryNull() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "bad").build();
        expectThrows(NullPointerException.class, () -> newClusterModuleWithShardsAllocator(settings, "bad", () -> null));
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    // makes sure that the allocation deciders are setup in the correct order, such that the
    // slower allocation deciders come last and we can exit early if there is a NO decision without
    // running them. If the order of the deciders is changed for a valid reason, the order should be
    // changed in the test too.
    public void testAllocationDeciderOrder() {
        List<Class<? extends AllocationDecider>> expectedDeciders = Arrays.asList(
            MaxRetryAllocationDecider.class,
            ResizeAllocationDecider.class,
            ReplicaAfterPrimaryActiveAllocationDecider.class,
            RebalanceOnlyWhenActiveAllocationDecider.class,
            ClusterRebalanceAllocationDecider.class,
            ConcurrentRebalanceAllocationDecider.class,
            EnableAllocationDecider.class,
            NodeVersionAllocationDecider.class,
            SnapshotInProgressAllocationDecider.class,
            RestoreInProgressAllocationDecider.class,
            NodeShutdownAllocationDecider.class,
            NodeReplacementAllocationDecider.class,
            FilterAllocationDecider.class,
            SameShardAllocationDecider.class,
            DiskThresholdDecider.class,
            ThrottlingAllocationDecider.class,
            ShardsLimitAllocationDecider.class,
            AwarenessAllocationDecider.class
        );
        Collection<AllocationDecider> deciders = ClusterModule.createAllocationDeciders(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            Collections.emptyList()
        );
        Iterator<AllocationDecider> iter = deciders.iterator();
        int idx = 0;
        while (iter.hasNext()) {
            AllocationDecider decider = iter.next();
            assertSame(decider.getClass(), expectedDeciders.get(idx++));
        }
    }

    public void testRejectsReservedExistingShardsAllocatorName() {
        final ClusterModule clusterModule = new ClusterModule(
            Settings.EMPTY,
            clusterService,
            List.of(existingShardsAllocatorPlugin(GatewayAllocator.ALLOCATOR_NAME)),
            clusterInfoService,
            null,
            threadPool,
            EmptySystemIndices.INSTANCE,
            WriteLoadForecaster.DEFAULT
        );
        expectThrows(IllegalArgumentException.class, () -> clusterModule.setExistingShardsAllocators(new TestGatewayAllocator()));
    }

    public void testRejectsDuplicateExistingShardsAllocatorName() {
        final ClusterModule clusterModule = new ClusterModule(
            Settings.EMPTY,
            clusterService,
            List.of(existingShardsAllocatorPlugin("duplicate"), existingShardsAllocatorPlugin("duplicate")),
            clusterInfoService,
            null,
            threadPool,
            EmptySystemIndices.INSTANCE,
            WriteLoadForecaster.DEFAULT
        );
        expectThrows(IllegalArgumentException.class, () -> clusterModule.setExistingShardsAllocators(new TestGatewayAllocator()));
    }

    private static ClusterPlugin existingShardsAllocatorPlugin(final String allocatorName) {
        return new ClusterPlugin() {
            @Override
            public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
                return Collections.singletonMap(allocatorName, new TestGatewayAllocator());
            }
        };
    }
}
