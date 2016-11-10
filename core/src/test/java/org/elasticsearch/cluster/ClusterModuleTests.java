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

import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.script.ScriptMetaData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ClusterModuleTests extends ModuleTestCase {
    private ClusterService clusterService = new ClusterService(Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);
    static class FakeAllocationDecider extends AllocationDecider {
        protected FakeAllocationDecider(Settings settings) {
            super(settings);
        }
    }

    static class FakeShardsAllocator implements ShardsAllocator {
        @Override
        public void allocate(RoutingAllocation allocation) {
            // noop
        }

        @Override
        public Map<DiscoveryNode, Float> weighShard(RoutingAllocation allocation, ShardRouting shard) {
            return new HashMap<>();
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ClusterModule(Settings.EMPTY, clusterService,
                Collections.<ClusterPlugin>singletonList(new ClusterPlugin() {
                    @Override
                    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                        return Collections.singletonList(new EnableAllocationDecider(settings, clusterSettings));
                    }
                })));
        assertEquals(e.getMessage(),
            "Cannot specify allocation decider [" + EnableAllocationDecider.class.getName() + "] twice");
    }

    public void testRegisterAllocationDecider() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService,
            Collections.singletonList(new ClusterPlugin() {
                @Override
                public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                    return Collections.singletonList(new FakeAllocationDecider(settings));
                }
            }));
        assertTrue(module.allocationDeciders.stream().anyMatch(d -> d.getClass().equals(FakeAllocationDecider.class)));
    }

    private ClusterModule newClusterModuleWithShardsAllocator(Settings settings, String name, Supplier<ShardsAllocator> supplier) {
        return new ClusterModule(settings, clusterService, Collections.singletonList(
            new ClusterPlugin() {
                @Override
                public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
                    return Collections.singletonMap(name, supplier);
                }
            }
        ));
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "custom").build();
        ClusterModule module = newClusterModuleWithShardsAllocator(settings, "custom", FakeShardsAllocator::new);
        assertEquals(FakeShardsAllocator.class, module.shardsAllocator.getClass());
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            newClusterModuleWithShardsAllocator(Settings.EMPTY, ClusterModule.BALANCED_ALLOCATOR, FakeShardsAllocator::new));
        assertEquals("ShardsAllocator [" + ClusterModule.BALANCED_ALLOCATOR + "] already defined", e.getMessage());
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ClusterModule(settings, clusterService, Collections.emptyList()));
        assertEquals("Unknown ShardsAllocator [dne]", e.getMessage());
    }

    public void testShardsAllocatorFactoryNull() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "bad").build();
        NullPointerException e = expectThrows(NullPointerException.class, () ->
            newClusterModuleWithShardsAllocator(settings, "bad", () -> null));
    }

    // makes sure that the allocation deciders are setup in the correct order, such that the
    // slower allocation deciders come last and we can exit early if there is a NO decision without
    // running them. If the order of the deciders is changed for a valid reason, the order should be
    // changed in the test too.
    public void testAllocationDeciderOrder() {
        List<Class<? extends AllocationDecider>> expectedDeciders = Arrays.asList(
            MaxRetryAllocationDecider.class,
            ReplicaAfterPrimaryActiveAllocationDecider.class,
            RebalanceOnlyWhenActiveAllocationDecider.class,
            ClusterRebalanceAllocationDecider.class,
            ConcurrentRebalanceAllocationDecider.class,
            EnableAllocationDecider.class,
            NodeVersionAllocationDecider.class,
            SnapshotInProgressAllocationDecider.class,
            FilterAllocationDecider.class,
            SameShardAllocationDecider.class,
            DiskThresholdDecider.class,
            ThrottlingAllocationDecider.class,
            ShardsLimitAllocationDecider.class,
            AwarenessAllocationDecider.class);
        Collection<AllocationDecider> deciders = ClusterModule.createAllocationDeciders(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), Collections.emptyList());
        Iterator<AllocationDecider> iter = deciders.iterator();
        int idx = 0;
        while (iter.hasNext()) {
            AllocationDecider decider = iter.next();
            assertSame(decider.getClass(), expectedDeciders.get(idx++));
        }
    }

    public void testCustomClusterCustoms() {
        ClusterPlugin plugin = new ClusterPlugin() {
            @Override
            public Collection<ClusterState.Custom> getCustomClusterState() {
                return Collections.singleton(createCustomClusterState("custom_type"));
            }

            @Override
            public Collection<MetaData.Custom> getCustomMetadata() {
                return Collections.singleton(createCustomMetadata("custom_type"));
            }

            @Override
            public Collection<IndexMetaData.Custom> getCustomIndexMetadata() {
                return Collections.singleton(createCustomIndexMetadata("custom_type"));
            }
        };

        CustomPrototypeRegistry registry = ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin));
        assertThat(registry.getClusterStatePrototype(SnapshotsInProgress.TYPE), not(nullValue()));
        assertThat(registry.getClusterStatePrototype(RestoreInProgress.TYPE), not(nullValue()));
        assertThat(registry.getClusterStatePrototype("custom_type"), not(nullValue()));
        assertThat(registry.getClusterStatePrototype("does_not_exist"), nullValue());
        expectThrows(IllegalArgumentException.class, () -> registry.getClusterStatePrototypeSafe("does_not_exist"));

        assertThat(registry.getMetadataPrototype(RepositoriesMetaData.TYPE), not(nullValue()));
        assertThat(registry.getMetadataPrototype(IngestMetadata.TYPE), not(nullValue()));
        assertThat(registry.getMetadataPrototype(ScriptMetaData.TYPE), not(nullValue()));
        assertThat(registry.getMetadataPrototype(IndexGraveyard.TYPE), not(nullValue()));
        assertThat(registry.getMetadataPrototype("custom_type"), not(nullValue()));
        assertThat(registry.getMetadataPrototype("does_not_exist"), nullValue());
        expectThrows(IllegalArgumentException.class, () -> registry.getMetadataPrototypeSafe("does_not_exist"));

        assertThat(registry.getIndexMetadataPrototype("custom_type"), not(nullValue()));
        assertThat(registry.getIndexMetadataPrototype("does_not_exist"), nullValue());
        expectThrows(IllegalArgumentException.class, () -> registry.getIndexMetadataPrototypeSafe("does_not_exist"));

        List<NamedWriteableRegistry.Entry> entries = registry.getNamedWriteables();
        assertThat(entries.size(), equalTo(9));
    }

    public void testCustomClusterCustoms_duplicateParts() {
        ClusterPlugin plugin1 = new ClusterPlugin() {
            @Override
            public Collection<ClusterState.Custom> getCustomClusterState() {
                return Collections.singleton(createCustomClusterState(SnapshotsInProgress.TYPE));
            }
        };
        IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin1)));
        assertThat(e.getMessage(), equalTo("Custom cluster state [snapshots] already declared"));

        ClusterPlugin plugin2 = new ClusterPlugin() {
            @Override
            public Collection<ClusterState.Custom> getCustomClusterState() {
                return Arrays.asList(createCustomClusterState("custom_type"), createCustomClusterState("custom_type"));
            }
        };
        e = expectThrows(IllegalStateException.class, () -> ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin2)));
        assertThat(e.getMessage(), equalTo("Custom cluster state [custom_type] already declared"));

        ClusterPlugin plugin3 = new ClusterPlugin() {

            @Override
            public Collection<MetaData.Custom> getCustomMetadata() {
                return Collections.singleton(createCustomMetadata(IndexGraveyard.TYPE));
            }
        };
        e = expectThrows(IllegalStateException.class, () -> ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin3)));
        assertThat(e.getMessage(), equalTo("Custom metadata [index-graveyard] already declared"));

        ClusterPlugin plugin4 = new ClusterPlugin() {

            @Override
            public Collection<MetaData.Custom> getCustomMetadata() {
                return Arrays.asList(createCustomMetadata("custom_type"), createCustomMetadata("custom_type"));
            }
        };
        e = expectThrows(IllegalStateException.class, () -> ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin4)));
        assertThat(e.getMessage(), equalTo("Custom metadata [custom_type] already declared"));

        ClusterPlugin plugin5 = new ClusterPlugin() {

            @Override
            public Collection<IndexMetaData.Custom> getCustomIndexMetadata() {
                return Arrays.asList(createCustomIndexMetadata("custom_type"), createCustomIndexMetadata("custom_type"));
            }
        };
        e = expectThrows(IllegalStateException.class, () -> ClusterModule.createCustomPrototypeRegistry(Collections.singleton(plugin5)));
        assertThat(e.getMessage(), equalTo("Custom index metadata [custom_type] already declared"));
    }

    private static ClusterState.Custom createCustomClusterState(String type) {
        return new ClusterState.Custom() {

            @Override
            public Diff<ClusterState.Custom> diff(ClusterState.Custom previousState) {
                return null;
            }

            @Override
            public Diff<ClusterState.Custom> readDiffFrom(StreamInput in, CustomPrototypeRegistry registry) throws IOException {
                return null;
            }

            @Override
            public ClusterState.Custom readFrom(StreamInput in) throws IOException {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public String type() {
                return type;
            }
        };
    }

    private MetaData.Custom createCustomMetadata(String type) {
        return new MetaData.Custom() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
                return null;
            }

            @Override
            public EnumSet<MetaData.XContentContext> context() {
                return null;
            }

            @Override
            public Diff<MetaData.Custom> diff(MetaData.Custom previousState) {
                return null;
            }

            @Override
            public Diff<MetaData.Custom> readDiffFrom(StreamInput in, CustomPrototypeRegistry registry) throws IOException {
                return null;
            }

            @Override
            public MetaData.Custom readFrom(StreamInput in) throws IOException {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
    }

    private IndexMetaData.Custom createCustomIndexMetadata(String type) {
        return new IndexMetaData.Custom() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public IndexMetaData.Custom fromMap(Map<String, Object> map) throws IOException {
                return null;
            }

            @Override
            public IndexMetaData.Custom fromXContent(XContentParser parser) throws IOException {
                return null;
            }

            @Override
            public IndexMetaData.Custom mergeWith(IndexMetaData.Custom another) {
                return null;
            }

            @Override
            public Diff<IndexMetaData.Custom> diff(IndexMetaData.Custom previousState) {
                return null;
            }

            @Override
            public Diff<IndexMetaData.Custom> readDiffFrom(StreamInput in, CustomPrototypeRegistry registry) throws IOException {
                return null;
            }

            @Override
            public IndexMetaData.Custom readFrom(StreamInput in) throws IOException {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };
    }
}
