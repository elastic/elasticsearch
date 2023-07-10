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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexMappingUpdateService.UpgradeStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemIndexMappingUpdateServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();

    private static final String SYSTEM_INDEX_NAME = ".myindex-1";

    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".myindex-*")
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setAliasName(".myindex")
        .setIndexFormat(new SystemIndexDescriptor.IndexFormat(6, null))
        .setSettings(getSettings())
        .setMappings(getMappings())
        .setVersionMetaKey("version")
        .setOrigin("FAKE_ORIGIN")
        .build();

    private static final SystemIndices.Feature FEATURE = new SystemIndices.Feature("foo", "a test feature", List.of(DESCRIPTOR));

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
        SystemIndexDescriptor d1 = SystemIndexDescriptorUtils.createUnmanaged(".foo-1*", "");
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(6, null))
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature("index 1", "index 1 feature", List.of(d1)),
                new SystemIndices.Feature("index 2", "index 2 feature", List.of(d2))
            )
        );
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(systemIndices, client);

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
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(6, null))
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(6, null))
            .setVersionMetaKey("version")
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature("index 1", "index 1 feature", List.of(d1)),
                new SystemIndices.Feature("index 2", "index 2 feature", List.of(d2))
            )
        );
        ;
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(systemIndices, client);

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
        final ClusterState.Builder clusterStateBuilder = createClusterState(IndexMetadata.State.CLOSE);
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(clusterStateBuilder.build(), DESCRIPTOR),
            equalTo(UpgradeStatus.CLOSED)
        );
    }

    /**
     * Check that the manager won't try to upgrade unhealthy indices.
     */
    public void testManagerSkipsIndicesWithRedStatus() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsUnavailable(createClusterState()), DESCRIPTOR),
            equalTo(UpgradeStatus.UNHEALTHY)
        );
    }

    /**
     * Check that the manager won't try to upgrade indices where the `index.format` setting
     * is earlier than an expected value.
     */
    public void testManagerSkipsIndicesWithOutdatedFormat() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsAvailable(createClusterState(5)), DESCRIPTOR),
            equalTo(UpgradeStatus.NEEDS_UPGRADE)
        );
    }

    /**
     * Check that the manager won't try to upgrade indices where their mappings are already up-to-date.
     */
    public void testManagerSkipsIndicesWithUpToDateMappings() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsAvailable(createClusterState()), DESCRIPTOR),
            equalTo(UpgradeStatus.UP_TO_DATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices where their mappings are out-of-date.
     */
    public void testManagerProcessesIndicesWithOutdatedMappings() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createClusterState(Strings.toString(getMappings("1.0.0")))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices where the mappings metadata is null or absent.
     */
    public void testManagerProcessesIndicesWithNullMetadata() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createClusterState(Strings.toString(getMappings(builder -> {})))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices where the version in the metadata is null or absent.
     */
    public void testManagerProcessesIndicesWithNullVersionMetadata() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createClusterState(Strings.toString(getMappings((String) null)))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager submits the expected request for an index whose mappings are out-of-date.
     */
    public void testManagerSubmitsPutRequest() {
        SystemIndices systemIndices = new SystemIndices(List.of(FEATURE));
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(systemIndices, client);

        manager.clusterChanged(event(markShardsAvailable(createClusterState(Strings.toString(getMappings("1.0.0"))))));

        verify(client, times(1)).execute(any(PutMappingAction.class), any(PutMappingRequest.class), any());
    }

    /**
     * Check that this
     */
    public void testCanHandleIntegerMetaVersion() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createClusterState(Strings.toString(getMappings(3)))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    private static ClusterState.Builder createClusterState() {
        return createClusterState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings());
    }

    private static ClusterState.Builder createClusterState(String mappings) {
        return createClusterState(mappings, IndexMetadata.State.OPEN);
    }

    private static ClusterState.Builder createClusterState(IndexMetadata.State state) {
        return createClusterState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings(), 6, state);
    }

    private static ClusterState.Builder createClusterState(String mappings, IndexMetadata.State state) {
        return createClusterState(mappings, 6, state);
    }

    private static ClusterState.Builder createClusterState(int format) {
        return createClusterState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings(), format, IndexMetadata.State.OPEN);
    }

    private static ClusterState.Builder createClusterState(String mappings, int format, IndexMetadata.State state) {
        IndexMetadata.Builder indexMeta = getIndexMetadata(SystemIndexMappingUpdateServiceTests.DESCRIPTOR, mappings, format, state);

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
                        IndexShardRoutingTable.builder(new ShardId(prevIndex, 0))
                            .addShard(
                                ShardRouting.newUnassigned(
                                    new ShardId(prevIndex, 0),
                                    true,
                                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                                    ShardRouting.Role.DEFAULT
                                )
                                    .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
                                    .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
                            )
                    )
            )
            .build();

        return ClusterState.builder(cs).routingTable(unavailableRoutingTable).build();
    }

    private static ClusterState state() {
        final DiscoveryNode node = DiscoveryNodeUtils.builder("1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()).build();
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes).metadata(Metadata.builder().generateClusterUuidIfNeeded()).build();
    }

    private static IndexMetadata.Builder getIndexMetadata(
        SystemIndexDescriptor descriptor,
        String mappings,
        int format,
        IndexMetadata.State state
    ) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(
            descriptor.isAutomaticallyManaged() ? descriptor.getPrimaryIndex() : descriptor.getIndexPattern()
        );

        final Settings.Builder settingsBuilder = Settings.builder();
        if (descriptor.isAutomaticallyManaged() == false || SystemIndexDescriptor.DEFAULT_SETTINGS.equals(descriptor.getSettings())) {
            settingsBuilder.put(getSettings());
        } else {
            settingsBuilder.put(descriptor.getSettings());
        }
        settingsBuilder.put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), format);
        indexMetadata.settings(settingsBuilder.build());

        if (descriptor.isAutomaticallyManaged() && descriptor.getAliasName() != null) {
            indexMetadata.putAlias(AliasMetadata.builder(descriptor.getAliasName()).build());
        }
        indexMetadata.state(state);
        if (mappings != null) {
            indexMetadata.putMapping(mappings);
        }

        return indexMetadata;
    }

    private static RoutingTable buildIndexRoutingTable(Index index) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        return RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addIndexShard(
                        IndexShardRoutingTable.builder(new ShardId(index, 0))
                            .addShard(
                                shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize())
                                    .moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                            )
                    )
                    .build()
            )
            .build();
    }

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
    }

    private static Settings getSettings() {
        return indexSettings(Version.CURRENT, 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6).build();
    }

    private static XContentBuilder getMappings() {
        return getMappings(Version.CURRENT.toString());
    }

    private static XContentBuilder getMappings(String version) {
        return getMappings(builder -> builder.object("_meta", meta -> meta.field("version", version)));
    }

    // Prior to 7.12.0, .tasks had _meta.version: 3 so we need to be sure we can handle that
    private static XContentBuilder getMappings(int version) {
        return getMappings(builder -> builder.object("_meta", meta -> meta.field("version", version)));
    }

    private static XContentBuilder getMappings(CheckedConsumer<XContentBuilder, IOException> metaCallback) {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                metaCallback.accept(builder);

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
