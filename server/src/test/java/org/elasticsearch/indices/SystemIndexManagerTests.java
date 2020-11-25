/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexManager.UpgradeStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class SystemIndexManagerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();

    private static final String SYSTEM_INDEX_NAME = ".myindex-1";
    private static final String SYSTEM_INDEX_PATTERN = ".myindex-*";
    private static final String SYSTEM_INDEX_ALIAS = ".myindex";

    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYSTEM_INDEX_PATTERN)
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setAliasName(SYSTEM_INDEX_ALIAS)
        .setIndexFormat(6)
        .setSettings(getSettings())
        .setMappings(getMappings())
        .setVersionMetaKey("version")
        .build();

    private Client client;

    @Before
    public void setUpManager() {
        client = mock(Client.class);
        // final ThreadPool threadPool = mock(ThreadPool.class);
        // when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        // when(threadPool.generic()).thenReturn(EsExecutors.newDirectExecutorService());
        // when(mockClient.threadPool()).thenReturn(threadPool);
        // when(mockClient.settings()).thenReturn(Settings.EMPTY);
    }

    /**
     * Check that the manager skips over descriptors whose indices cannot be managed.
     */
    public void testManagerSkipsDescriptorsThatAreNotManaged() {
        SystemIndexDescriptor d1 = new SystemIndexDescriptor(".foo-1", "");
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .build();

        SystemIndices systemIndices = new SystemIndices(Map.of("index 1", List.of(d1), "index 2", List.of(d2)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final List<SystemIndexDescriptor> eligibleDescriptors = manager.getEligibleDescriptors(
            Metadata.builder()
                .put(getIndexMetadata(d1, null, 6, IndexMetadata.State.OPEN))
                .put(getIndexMetadata(d2, d2.getMappings(), 6, IndexMetadata.State.OPEN))
                .build()
        );

        assertThat(eligibleDescriptors, hasSize(1));
        assertThat(eligibleDescriptors, contains(d2));
    }

    /**
     * Check that the manager skips over indices that don't exist yet, since system indices are
     * created on-demand.
     */
    public void testManagerSkipsDescriptorsForIndicesThatDoNotExist() {
        SystemIndexDescriptor d1 = SystemIndexDescriptor.builder()
            .setIndexPattern(".foo-*")
            .setPrimaryIndex(".foo-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .build();
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .build();

        SystemIndices systemIndices = new SystemIndices(Map.of("index 1", List.of(d1), "index 2", List.of(d2)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final List<SystemIndexDescriptor> eligibleDescriptors = manager.getEligibleDescriptors(
            Metadata.builder().put(getIndexMetadata(d2, d2.getMappings(), 6, IndexMetadata.State.OPEN)).build()
        );

        assertThat(eligibleDescriptors, hasSize(1));
        assertThat(eligibleDescriptors, contains(d2));
    }

    public void testManagerSkipsClosedIndices() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", List.of(DESCRIPTOR)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR, IndexMetadata.State.CLOSE);

        assertThat(manager.isUpgradeRequired(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.CLOSED));
    }

    public void testManagerSkipsIndicesWithRedStatus() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", List.of(DESCRIPTOR)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR);
        markShardsUnavailable(clusterStateBuilder);

        assertThat(manager.isUpgradeRequired(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.UNHEALTHY));
    }

    public void testManagerSkipsIndicesWithOutdatedFormat() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", List.of(DESCRIPTOR)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR, 5);
        markShardsAvailable(clusterStateBuilder);

        assertThat(manager.isUpgradeRequired(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.NEEDS_UPGRADE));
    }

    public void testManagerSkipsIndicesWithUpToDateMappings() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", List.of(DESCRIPTOR)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR);
        markShardsAvailable(clusterStateBuilder);

        assertThat(manager.isUpgradeRequired(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.UP_TO_DATE));
    }

    public void testManagerProcessesIndicesWithOutdatedMappings() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", List.of(DESCRIPTOR)));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR, Strings.toString(getMappings("1.0.0")));
        markShardsAvailable(clusterStateBuilder);

        assertThat(manager.isUpgradeRequired(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE));
    }

    // public void testDescriptorWithoutPrimaryShards() throws IOException {
    // final ClusterState.Builder clusterStateBuilder = createClusterState(DESCRIPTOR);
    // Index index = new Index(SYSTEM_INDEX_NAME, UUID.randomUUID().toString());
    // ShardRouting shardRouting = ShardRouting.newUnassigned(
    // new ShardId(index, 0),
    // true,
    // RecoverySource.ExistingStoreRecoverySource.INSTANCE,
    // new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
    // );
    // String nodeId = ESTestCase.randomAlphaOfLength(8);
    // IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0)).addShard(
    // shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize())
    // .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
    // ).build();
    // clusterStateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addIndexShard(table).build()).build());
    // manager.clusterChanged(event(clusterStateBuilder));
    //
    // assertIndexUpToDateButNotAvailable();
    // }
    //
    // public void testDescriptorHealthChangeListeners() throws Exception {
    // final AtomicBoolean listenerCalled = new AtomicBoolean(false);
    // final AtomicReference<SecurityIndexManager.State> previousState = new AtomicReference<>();
    // final AtomicReference<SecurityIndexManager.State> currentState = new AtomicReference<>();
    // final BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> listener = (prevState, state) -> {
    // previousState.set(prevState);
    // currentState.set(state);
    // listenerCalled.set(true);
    // };
    // manager.addIndexStateListener(listener);
    //
    // // index doesn't exist and now exists
    // final ClusterState.Builder clusterStateBuilder = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME
    // );
    // markShardsAvailable(clusterStateBuilder);
    // final ClusterState clusterState = clusterStateBuilder.build();
    // manager.clusterChanged(event(ClusterState.builder(clusterState)));
    //
    // assertTrue(listenerCalled.get());
    // assertNull(previousState.get().indexHealth);
    // assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);
    //
    // // reset and call with no change to the index
    // listenerCalled.set(false);
    // previousState.set(null);
    // currentState.set(null);
    // ClusterChangedEvent event = new ClusterChangedEvent("same index health", clusterState, clusterState);
    // manager.clusterChanged(event);
    //
    // assertFalse(listenerCalled.get());
    // assertNull(previousState.get());
    // assertNull(currentState.get());
    //
    // // index with different health
    // listenerCalled.set(false);
    // previousState.set(null);
    // currentState.set(null);
    // Index prevIndex = clusterState.getRoutingTable().index(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
    // final ClusterState newClusterState = ClusterState.builder(clusterState)
    // .routingTable(
    // RoutingTable.builder()
    // .add(
    // IndexRoutingTable.builder(prevIndex)
    // .addIndexShard(
    // new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0)).addShard(
    // ShardRouting.newUnassigned(
    // new ShardId(prevIndex, 0),
    // true,
    // RecoverySource.ExistingStoreRecoverySource.INSTANCE,
    // new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
    // )
    // .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
    // .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
    // ).build()
    // )
    // )
    // .build()
    // )
    // .build();
    //
    // event = new ClusterChangedEvent("different index health", newClusterState, clusterState);
    // manager.clusterChanged(event);
    // assertTrue(listenerCalled.get());
    // assertEquals(ClusterHealthStatus.GREEN, previousState.get().indexHealth);
    // assertEquals(ClusterHealthStatus.RED, currentState.get().indexHealth);
    //
    // // swap prev and current
    // listenerCalled.set(false);
    // previousState.set(null);
    // currentState.set(null);
    // event = new ClusterChangedEvent("different index health swapped", clusterState, newClusterState);
    // manager.clusterChanged(event);
    // assertTrue(listenerCalled.get());
    // assertEquals(ClusterHealthStatus.RED, previousState.get().indexHealth);
    // assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);
    // }
    //
    // public void testWriteBeforeStateNotRecovered() throws Exception {
    // final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
    // final AtomicReference<Exception> prepareException = new AtomicReference<>(null);
    // manager.checkIndexStateThenExecute(ex -> { prepareException.set(ex); }, () -> { prepareRunnableCalled.set(true); });
    // assertThat(prepareException.get(), is(notNullValue()));
    // assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
    // assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
    // assertThat(prepareRunnableCalled.get(), is(false));
    // prepareException.set(null);
    // prepareRunnableCalled.set(false);
    // // state not recovered
    // final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
    // manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks)));
    // manager.checkIndexStateThenExecute(ex -> { prepareException.set(ex); }, () -> { prepareRunnableCalled.set(true); });
    // assertThat(prepareException.get(), is(notNullValue()));
    // assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
    // assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
    // assertThat(prepareRunnableCalled.get(), is(false));
    // prepareException.set(null);
    // prepareRunnableCalled.set(false);
    // // state recovered with index
    // ClusterState.Builder clusterStateBuilder = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT
    // );
    // markShardsAvailable(clusterStateBuilder);
    // manager.clusterChanged(event(clusterStateBuilder));
    // manager.checkIndexStateThenExecute(ex -> { prepareException.set(ex); }, () -> { prepareRunnableCalled.set(true); });
    // assertThat(prepareException.get(), is(nullValue()));
    // assertThat(prepareRunnableCalled.get(), is(true));
    // }
    //
    // public void testListenerNotCalledBeforeStateNotRecovered() throws Exception {
    // final AtomicBoolean listenerCalled = new AtomicBoolean(false);
    // manager.addIndexStateListener((prev, current) -> { listenerCalled.set(true); });
    // final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
    // // state not recovered
    // manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks)));
    // assertThat(manager.isStateRecovered(), is(false));
    // assertThat(listenerCalled.get(), is(false));
    // // state recovered with index
    // ClusterState.Builder clusterStateBuilder = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT
    // );
    // markShardsAvailable(clusterStateBuilder);
    // manager.clusterChanged(event(clusterStateBuilder));
    // assertThat(manager.isStateRecovered(), is(true));
    // assertThat(listenerCalled.get(), is(true));
    // }
    //
    // public void testDescriptorOutOfDateListeners() throws Exception {
    // final AtomicBoolean listenerCalled = new AtomicBoolean(false);
    // manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
    // AtomicBoolean upToDateChanged = new AtomicBoolean();
    // manager.addIndexStateListener((prev, current) -> {
    // listenerCalled.set(true);
    // upToDateChanged.set(prev.isIndexUpToDate != current.isIndexUpToDate);
    // });
    // assertTrue(manager.isIndexUpToDate());
    //
    // manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
    // assertFalse(listenerCalled.get());
    // assertTrue(manager.isIndexUpToDate());
    //
    // // index doesn't exist and now exists with wrong format
    // ClusterState.Builder clusterStateBuilder = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT - 1
    // );
    // markShardsAvailable(clusterStateBuilder);
    // manager.clusterChanged(event(clusterStateBuilder));
    // assertTrue(listenerCalled.get());
    // assertTrue(upToDateChanged.get());
    // assertFalse(manager.isIndexUpToDate());
    //
    // listenerCalled.set(false);
    // assertFalse(listenerCalled.get());
    // manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
    // assertTrue(listenerCalled.get());
    // assertTrue(upToDateChanged.get());
    // assertTrue(manager.isIndexUpToDate());
    //
    // listenerCalled.set(false);
    // // index doesn't exist and now exists with correct format
    // clusterStateBuilder = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT
    // );
    // markShardsAvailable(clusterStateBuilder);
    // manager.clusterChanged(event(clusterStateBuilder));
    // assertTrue(listenerCalled.get());
    // assertFalse(upToDateChanged.get());
    // assertTrue(manager.isIndexUpToDate());
    // }
    //
    // public void testProcessClosedIndexState() throws Exception {
    // // Index initially exists
    // final ClusterState.Builder indexAvailable = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // IndexMetadata.State.OPEN
    // );
    // markShardsAvailable(indexAvailable);
    //
    // manager.clusterChanged(event(indexAvailable));
    // assertThat(manager.indexExists(), is(true));
    // assertThat(manager.isAvailable(), is(true));
    //
    // // Now close it
    // final ClusterState.Builder indexClosed = createClusterState(
    // RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7,
    // RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
    // TEMPLATE_NAME,
    // IndexMetadata.State.CLOSE
    // );
    // if (randomBoolean()) {
    // // In old/mixed cluster versions closed indices have no routing table
    // indexClosed.routingTable(RoutingTable.EMPTY_ROUTING_TABLE);
    // } else {
    // markShardsAvailable(indexClosed);
    // }
    //
    // manager.clusterChanged(event(indexClosed));
    // assertThat(manager.indexExists(), is(true));
    // assertThat(manager.isAvailable(), is(false));
    // }

    public static ClusterState.Builder createClusterState(SystemIndexDescriptor descriptor) {
        return createClusterState(descriptor, descriptor.getMappings());
    }

    public static ClusterState.Builder createClusterState(SystemIndexDescriptor descriptor, String mappings) {
        return createClusterState(descriptor, mappings, IndexMetadata.State.OPEN);
    }

    public static ClusterState.Builder createClusterState(SystemIndexDescriptor descriptor, IndexMetadata.State state) {
        return createClusterState(descriptor, descriptor.getMappings(), 6, state);
    }

    public static ClusterState.Builder createClusterState(SystemIndexDescriptor descriptor, String mappings, IndexMetadata.State state) {
        return createClusterState(descriptor, mappings, 6, state);
    }

    public static ClusterState.Builder createClusterState(SystemIndexDescriptor descriptor, int format) {
        return createClusterState(descriptor, descriptor.getMappings(), format, IndexMetadata.State.OPEN);
    }

    private static ClusterState.Builder createClusterState(
        SystemIndexDescriptor descriptor,
        String mappings,
        int format,
        IndexMetadata.State state
    ) {
        IndexMetadata.Builder indexMeta = getIndexMetadata(descriptor, mappings, format, state);

        Metadata.Builder metadataBuilder = new Metadata.Builder();
        metadataBuilder.put(indexMeta);

        return ClusterState.builder(state()).metadata(metadataBuilder.build());
    }

    private void markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        clusterStateBuilder.routingTable(buildIndexRoutingTable(DESCRIPTOR.getPrimaryIndex()));
    }

    private void markShardsUnavailable(ClusterState.Builder clusterStateBuilder) {
        final RoutingTable routingTable = buildIndexRoutingTable(DESCRIPTOR.getPrimaryIndex());

        Index prevIndex = routingTable.index(DESCRIPTOR.getPrimaryIndex()).getIndex();

        final RoutingTable unavailableRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(prevIndex)
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0)).addShard(
                            ShardRouting.newUnassigned(
                                new ShardId(prevIndex, 0),
                                true,
                                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
                            )
                                .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
                                .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
                        ).build()
                    )
            )
            .build();

        clusterStateBuilder.routingTable(unavailableRoutingTable);
    }

    private static ClusterState state() {
        final DiscoveryNodes nodes = DiscoveryNodes.builder().masterNodeId("1").localNodeId("1").build();
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes).metadata(Metadata.builder().generateClusterUuidIfNeeded()).build();
    }

    private static IndexMetadata.Builder getIndexMetadata(
        SystemIndexDescriptor descriptor,
        String mappings,
        int format,
        IndexMetadata.State state
    ) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(
            descriptor.getPrimaryIndex() == null ? descriptor.getIndexPattern() : descriptor.getPrimaryIndex()
        );

        final Settings.Builder settingsBuilder = Settings.builder();
        if (descriptor.getSettings() != null) {
            settingsBuilder.put(descriptor.getSettings());
        } else {
            settingsBuilder.put(getSettings());
        }
        settingsBuilder.put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), format);
        indexMetadata.settings(settingsBuilder.build());

        if (descriptor.getAliasName() != null) {
            indexMetadata.putAlias(AliasMetadata.builder(descriptor.getAliasName()).build());
        }
        indexMetadata.state(state);
        if (mappings != null) {
            indexMetadata.putMapping(mappings);
        }

        return indexMetadata;
    }

    private static RoutingTable buildIndexRoutingTable(String indexName) {
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0)).addShard(
            shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize()).moveToStarted()
        ).build();
        return RoutingTable.builder().add(IndexRoutingTable.builder(index).addIndexShard(table).build()).build();
    }

    private ClusterChangedEvent event(ClusterState.Builder clusterStateBuilder) {
        return new ClusterChangedEvent("test-event", clusterStateBuilder.build(), EMPTY_CLUSTER_STATE);
    }

    private static Settings getSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6)
            .build();
    }

    private static XContentBuilder getMappings() {
        return getMappings(Version.CURRENT.toString());
    }

    private static XContentBuilder getMappings(String version) {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", version);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("completed");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("task");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("action");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("cancellable");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("id");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("parent_task_id");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("node");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("running_time_in_nanos");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("start_time_in_millis");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("type");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("status");
                            builder.field("type", "object");
                            builder.field("enabled", false);
                            builder.endObject();

                            builder.startObject("description");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("headers");
                            builder.field("type", "object");
                            builder.field("enabled", false);
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("response");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("error");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + SYSTEM_INDEX_NAME + " index mappings", e);
        }
    }
}
