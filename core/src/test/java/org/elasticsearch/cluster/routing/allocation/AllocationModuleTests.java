package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;

public class AllocationModuleTests extends ModuleTestCase {

    public static class FakeAllocationDecider extends AllocationDecider {
        protected FakeAllocationDecider(Settings settings) {
            super(settings);
        }
    }

    public static class FakeShardsAllocator implements ShardsAllocator {
        @Override
        public void applyStartedShards(StartedRerouteAllocation allocation) {}
        @Override
        public void applyFailedShards(FailedRerouteAllocation allocation) {}
        @Override
        public boolean allocateUnassigned(RoutingAllocation allocation) {
            return false;
        }
        @Override
        public boolean rebalance(RoutingAllocation allocation) {
            return false;
        }
        @Override
        public boolean move(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return false;
        }
    }

    public void testRegisterAllocationDeciderDuplicate() {
        AllocationModule module = new AllocationModule(Settings.EMPTY);
        try {
            module.registerAllocationDecider(EnableAllocationDecider.class);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Cannot register AllocationDecider"));
            assertTrue(e.getMessage().contains("twice"));
        }
    }

    public void testRegisterAllocationDecider() {
        AllocationModule module = new AllocationModule(Settings.EMPTY);
        module.registerAllocationDecider(FakeAllocationDecider.class);
        assertSetMultiBinding(module, AllocationDecider.class, FakeAllocationDecider.class);
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(AllocationModule.SHARDS_ALLOCATOR_TYPE_KEY, "custom").build();
        AllocationModule module = new AllocationModule(settings);
        module.registerShardAllocator("custom", FakeShardsAllocator.class);
        assertBinding(module, ShardsAllocator.class, FakeShardsAllocator.class);
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        AllocationModule module = new AllocationModule(Settings.EMPTY);
        try {
            module.registerShardAllocator(AllocationModule.BALANCED_ALLOCATOR, FakeShardsAllocator.class);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("already registered"));
        }
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(AllocationModule.SHARDS_ALLOCATOR_TYPE_KEY, "dne").build();
        AllocationModule module = new AllocationModule(settings);
        assertBindingFailure(module, "Unknown ShardsAllocator");
    }

    public void testEvenShardsAllocatorBackcompat() {
        Settings settings = Settings.builder()
            .put(AllocationModule.SHARDS_ALLOCATOR_TYPE_KEY, AllocationModule.EVEN_SHARD_COUNT_ALLOCATOR).build();
        AllocationModule module = new AllocationModule(settings);
        assertBinding(module, ShardsAllocator.class, BalancedShardsAllocator.class);
    }
}
