/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;

import java.util.HashMap;
import java.util.Map;
public class ClusterModuleTests extends ModuleTestCase {
    private ClusterService clusterService = new ClusterService(Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);
    public static class FakeAllocationDecider extends AllocationDecider {
        protected FakeAllocationDecider(Settings settings) {
            super(settings);
        }
    }

    static class FakeShardsAllocator implements ShardsAllocator {
        @Override
        public boolean allocate(RoutingAllocation allocation) {
            return false;
        }

        @Override
        public Map<DiscoveryNode, Float> weighShard(RoutingAllocation allocation, ShardRouting shard) {
            return new HashMap<>();
        }
    }

    static class FakeIndexTemplateFilter implements IndexTemplateFilter {
        @Override
        public boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template) {
            return false;
        }
    }

    public void testRegisterClusterDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterClusterDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY,
            Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope));
        assertInstanceBinding(module, ClusterSettings.class, service -> service.hasDynamicSetting("foo.bar"));
    }

    public void testRegisterIndexDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterIndexDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY,
            Setting.boolSetting("index.foo.bar", false, Property.Dynamic, Property.IndexScope));
        assertInstanceBinding(module, IndexScopedSettings.class, service -> service.hasDynamicSetting("index.foo.bar"));
    }

    public void testRegisterAllocationDeciderDuplicate() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService);
        try {
            module.registerAllocationDecider(EnableAllocationDecider.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Can't register the same [allocation_decider] more than once for [" + EnableAllocationDecider.class.getName() + "]");
        }
    }

    public void testRegisterAllocationDecider() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService);
        module.registerAllocationDecider(FakeAllocationDecider.class);
        assertSetMultiBinding(module, AllocationDecider.class, FakeAllocationDecider.class);
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "custom").build();
        ClusterModule module = new ClusterModule(settings, clusterService);
        module.registerShardsAllocator("custom", FakeShardsAllocator.class);
        assertBinding(module, ShardsAllocator.class, FakeShardsAllocator.class);
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService);
        try {
            module.registerShardsAllocator(ClusterModule.BALANCED_ALLOCATOR, FakeShardsAllocator.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [shards_allocator] more than once for [balanced]");
        }
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "dne").build();
        ClusterModule module = new ClusterModule(settings, clusterService);
        assertBindingFailure(module, "Unknown [shards_allocator]");
    }

    public void testEvenShardsAllocatorBackcompat() {
        Settings settings = Settings.builder()
            .put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR).build();
        ClusterModule module = new ClusterModule(settings, clusterService);
        assertBinding(module, ShardsAllocator.class, BalancedShardsAllocator.class);
    }

    public void testRegisterIndexTemplateFilterDuplicate() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService);
        try {
            module.registerIndexTemplateFilter(FakeIndexTemplateFilter.class);
            module.registerIndexTemplateFilter(FakeIndexTemplateFilter.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Can't register the same [index_template_filter] more than once for [" + FakeIndexTemplateFilter.class.getName() + "]");
        }
    }

    public void testRegisterIndexTemplateFilter() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService);
        module.registerIndexTemplateFilter(FakeIndexTemplateFilter.class);
        assertSetMultiBinding(module, IndexTemplateFilter.class, FakeIndexTemplateFilter.class);
    }
}
