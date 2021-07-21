/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Map;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexManager.UpgradeStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemIndexManagerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();

    private static final String SYSTEM_INDEX_NAME = ".myindex-1";

    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".myindex-*")
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setAliasName(".myindex")
        .setIndexFormat(6)
        .setSettings(getSettings())
        .setMappings(getMappings())
        .setVersionMetaKey("version")
        .setOrigin("FAKE_ORIGIN")
        .build();

    private static final SystemIndices.Feature FEATURE = new SystemIndices.Feature(
        "foo",
        "a test feature",
        org.elasticsearch.core.List.of(DESCRIPTOR)
    );

    private Client client;

    @Before
    public void setUpManager() {
        client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(Settings.EMPTY);
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
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(Map.of(
            "index 1", new SystemIndices.Feature("index 1", "index 1 feature", org.elasticsearch.core.List.of(d1)),
            "index 2", new SystemIndices.Feature("index 2", "index 2 feature", org.elasticsearch.core.List.of(d2))));
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
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(Map.of(
            "index 1", new SystemIndices.Feature("index 1", "index 1 feature", org.elasticsearch.core.List.of(d1)),
            "index 2", new SystemIndices.Feature("index 2", "index 2 feature", org.elasticsearch.core.List.of(d2))));;
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final List<SystemIndexDescriptor> eligibleDescriptors = manager.getEligibleDescriptors(
            Metadata.builder().put(getIndexMetadata(d2, d2.getMappings(), 6, IndexMetadata.State.OPEN)).build()
        );

        assertThat(eligibleDescriptors, hasSize(1));
        assertThat(eligibleDescriptors, contains(d2));
    }

    /**
     * Check that the manager won't try to upgrade closed indices.
     */
    public void testManagerSkipsClosedIndices() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        final ClusterState.Builder clusterStateBuilder = createClusterState(IndexMetadata.State.CLOSE);

        assertThat(manager.getUpgradeStatus(clusterStateBuilder.build(), DESCRIPTOR), equalTo(UpgradeStatus.CLOSED));
    }

    /**
     * Check that the manager won't try to upgrade unhealthy indices.
     */
    public void testManagerSkipsIndicesWithRedStatus() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(manager.getUpgradeStatus(markShardsUnavailable(createClusterState()), DESCRIPTOR), equalTo(UpgradeStatus.UNHEALTHY));
    }

    /**
     * Check that the manager won't try to upgrade indices where the `index.format` setting
     * is earlier than an expected value.
     */
    public void testManagerSkipsIndicesWithOutdatedFormat() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(manager.getUpgradeStatus(markShardsAvailable(createClusterState(5)), DESCRIPTOR), equalTo(UpgradeStatus.NEEDS_UPGRADE));
    }

    /**
     * Check that the manager won't try to upgrade indices where their mappings are already up-to-date.
     */
    public void testManagerSkipsIndicesWithUpToDateMappings() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(manager.getUpgradeStatus(markShardsAvailable(createClusterState()), DESCRIPTOR), equalTo(UpgradeStatus.UP_TO_DATE));
    }

    /**
     * Check that the manager will try to upgrade indices where their mappings are out-of-date.
     */
    public void testManagerProcessesIndicesWithOutdatedMappings() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(
            manager.getUpgradeStatus(markShardsAvailable(createClusterState(Strings.toString(getMappings("1.0.0")))), DESCRIPTOR),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices where the version in the metadata is null or absent.
     */
    public void testManagerProcessesIndicesWithNullVersionMetadata() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(
            manager.getUpgradeStatus(markShardsAvailable(createClusterState(Strings.toString(getMappings(null)))), DESCRIPTOR),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager submits the expected request for an index whose mappings are out-of-date.
     */
    public void testManagerSubmitsPutRequest() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        manager.clusterChanged(event(markShardsAvailable(createClusterState(Strings.toString(getMappings("1.0.0"))))));

        verify(client, times(1)).execute(any(PutMappingAction.class), any(PutMappingRequest.class), any());
    }

    /**
     * Check that this
     */
    public void testCanHandleIntegerMetaVersion() {
        SystemIndices systemIndices = new SystemIndices(Map.of("MyIndex", FEATURE));
        SystemIndexManager manager = new SystemIndexManager(systemIndices, client);

        assertThat(
            manager.getUpgradeStatus(markShardsAvailable(createClusterState(Strings.toString(getMappings(3)))), DESCRIPTOR),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    private static ClusterState.Builder createClusterState() {
        return createClusterState(SystemIndexManagerTests.DESCRIPTOR.getMappings());
    }

    private static ClusterState.Builder createClusterState(String mappings) {
        return createClusterState(mappings, IndexMetadata.State.OPEN);
    }

    private static ClusterState.Builder createClusterState(IndexMetadata.State state) {
        return createClusterState(SystemIndexManagerTests.DESCRIPTOR.getMappings(), 6, state);
    }

    private static ClusterState.Builder createClusterState(String mappings, IndexMetadata.State state) {
        return createClusterState(mappings, 6, state);
    }

    private static ClusterState.Builder createClusterState(int format) {
        return createClusterState(SystemIndexManagerTests.DESCRIPTOR.getMappings(), format, IndexMetadata.State.OPEN);
    }

    private static ClusterState.Builder createClusterState(String mappings, int format, IndexMetadata.State state) {
        IndexMetadata.Builder indexMeta = getIndexMetadata(SystemIndexManagerTests.DESCRIPTOR, mappings, format, state);

        Metadata.Builder metadataBuilder = new Metadata.Builder();
        metadataBuilder.put(indexMeta);

        return ClusterState.builder(state()).metadata(metadataBuilder.build());
    }

    private ClusterState markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        final ClusterState cs = clusterStateBuilder.build();
        return ClusterState.builder(cs)
            .routingTable(buildIndexRoutingTable(cs.metadata().index(DESCRIPTOR.getPrimaryIndex()).getIndex()))
            .build();
    }

    private ClusterState markShardsUnavailable(ClusterState.Builder clusterStateBuilder) {
        final ClusterState cs = clusterStateBuilder.build();
        final RoutingTable routingTable = buildIndexRoutingTable(cs.metadata().index(DESCRIPTOR.getPrimaryIndex()).getIndex());

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

        return ClusterState.builder(cs).routingTable(unavailableRoutingTable).build();
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
            try {
                MappingMetadata mappingMetadata = new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    XContentHelper.convertToMap(JsonXContent.jsonXContent, mappings, true)
                );
                indexMetadata.putMapping(mappingMetadata);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return indexMetadata;
    }

    private static RoutingTable buildIndexRoutingTable(Index index) {
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

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
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
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + SYSTEM_INDEX_NAME + " index mappings", e);
        }
    }

    // Prior to 7.12.0, .tasks had _meta.version: 3 so we need to be sure we can handle that
    private static XContentBuilder getMappings(int version) {
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
