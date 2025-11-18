/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
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
import org.elasticsearch.index.IndexVersion;
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
import static org.mockito.ArgumentMatchers.same;
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
        .setIndexFormat(6)
        .setSettings(getSettings())
        .setMappings(getMappings())
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
            .setIndexFormat(6)
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature("index 1", "index 1 feature", List.of(d1)),
                new SystemIndices.Feature("index 2", "index 2 feature", List.of(d2))
            )
        );
        final var projectId = randomProjectIdOrDefault();
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(
            systemIndices,
            client,
            TestProjectResolvers.singleProject(projectId)
        );

        final List<SystemIndexDescriptor> eligibleDescriptors = manager.getEligibleDescriptors(
            ProjectMetadata.builder(projectId)
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
            .setIndexFormat(6)
            .setOrigin("FAKE_ORIGIN")
            .build();
        SystemIndexDescriptor d2 = SystemIndexDescriptor.builder()
            .setIndexPattern(".bar-*")
            .setPrimaryIndex(".bar-1")
            .setMappings(getMappings())
            .setSettings(getSettings())
            .setIndexFormat(6)
            .setOrigin("FAKE_ORIGIN")
            .build();

        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature("index 1", "index 1 feature", List.of(d1)),
                new SystemIndices.Feature("index 2", "index 2 feature", List.of(d2))
            )
        );

        final var projectId = randomProjectIdOrDefault();
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(
            systemIndices,
            client,
            TestProjectResolvers.singleProject(projectId)
        );

        final List<SystemIndexDescriptor> eligibleDescriptors = manager.getEligibleDescriptors(
            ProjectMetadata.builder(projectId).put(getIndexMetadata(d2, d2.getMappings(), 6, IndexMetadata.State.OPEN)).build()
        );

        assertThat(eligibleDescriptors, hasSize(1));
        assertThat(eligibleDescriptors, contains(d2));
    }

    /**
     * Check that the manager won't try to upgrade closed indices.
     */
    public void testManagerSkipsClosedIndices() {
        final ProjectState projectState = createProjectState(IndexMetadata.State.CLOSE);
        assertThat(SystemIndexMappingUpdateService.getUpgradeStatus(projectState, DESCRIPTOR), equalTo(UpgradeStatus.CLOSED));
    }

    /**
     * Check that the manager won't try to upgrade unhealthy indices.
     */
    public void testManagerSkipsIndicesWithRedStatus() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsUnavailable(createProjectState()), DESCRIPTOR),
            equalTo(UpgradeStatus.UNHEALTHY)
        );
    }

    /**
     * Check that the manager won't try to upgrade indices where the `index.format` setting
     * is earlier than an expected value.
     */
    public void testManagerSkipsIndicesWithOutdatedFormat() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsAvailable(createProjectState(5)), DESCRIPTOR),
            equalTo(UpgradeStatus.NEEDS_UPGRADE)
        );
    }

    /**
     * Check that the manager won't try to upgrade indices where their mappings are already up-to-date.
     */
    public void testManagerSkipsIndicesWithUpToDateMappings() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(markShardsAvailable(createProjectState()), DESCRIPTOR),
            equalTo(UpgradeStatus.UP_TO_DATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices when we have the old mappings version but not the new one
     */
    public void testManagerProcessesIndicesWithOldMappingsVersion() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createProjectState(Strings.toString(getMappings("1.0.0", null)))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    /**
     * Check that the manager will try to upgrade indices where their mappings are out-of-date.
     */
    public void testManagerProcessesIndicesWithOutdatedMappings() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createProjectState(Strings.toString(getMappings("1.0.0", 4)))),
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
                markShardsAvailable(createProjectState(Strings.toString(getMappings(builder -> {})))),
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
                markShardsAvailable(createProjectState(Strings.toString(getMappings((String) null, null)))),
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
        final ProjectState projectState = createProjectState(Strings.toString(getMappings("1.0.0", 4)));
        SystemIndexMappingUpdateService manager = new SystemIndexMappingUpdateService(
            systemIndices,
            client,
            TestProjectResolvers.singleProject(projectState.projectId())
        );

        manager.clusterChanged(event(markShardsAvailable(projectState)));

        verify(client, times(1)).execute(same(TransportPutMappingAction.TYPE), any(PutMappingRequest.class), any());
    }

    /**
     * Check that this
     */
    public void testCanHandleIntegerMetaVersion() {
        assertThat(
            SystemIndexMappingUpdateService.getUpgradeStatus(
                markShardsAvailable(createProjectState(Strings.toString(getMappings(3)))),
                DESCRIPTOR
            ),
            equalTo(UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
        );
    }

    private static ProjectState createProjectState() {
        return createProjectState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings());
    }

    private static ProjectState createProjectState(String mappings) {
        return createProjectState(mappings, 6, IndexMetadata.State.OPEN);
    }

    private static ProjectState createProjectState(IndexMetadata.State state) {
        return createProjectState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings(), 6, state);
    }

    private static ProjectState createProjectState(int format) {
        return createProjectState(SystemIndexMappingUpdateServiceTests.DESCRIPTOR.getMappings(), format, IndexMetadata.State.OPEN);
    }

    private static ProjectState createProjectState(String mappings, int format, IndexMetadata.State state) {
        final var projectId = randomProjectIdOrDefault();
        IndexMetadata.Builder indexMeta = getIndexMetadata(SystemIndexMappingUpdateServiceTests.DESCRIPTOR, mappings, format, state);

        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        projectBuilder.put(indexMeta);

        final DiscoveryNode node = DiscoveryNodeUtils.builder("1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()).build();
        final Metadata metadata = Metadata.builder().generateClusterUuidIfNeeded().put(projectBuilder).build();
        final ClusterState clusterState = ClusterState.builder(CLUSTER_NAME)
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        return clusterState.projectState(projectId);
    }

    private ProjectState markShardsAvailable(ProjectState project) {
        final ClusterState previousClusterState = project.cluster();
        final GlobalRoutingTable routingTable = GlobalRoutingTable.builder(previousClusterState.globalRoutingTable())
            .put(project.projectId(), buildIndexRoutingTable(project.metadata().index(DESCRIPTOR.getPrimaryIndex()).getIndex()))
            .build();
        final ClusterState newClusterState = ClusterState.builder(previousClusterState).routingTable(routingTable).build();
        return newClusterState.projectState(project.projectId());
    }

    private ProjectState markShardsUnavailable(ProjectState projectState) {
        Index prevIndex = projectState.routingTable().index(DESCRIPTOR.getPrimaryIndex()).getIndex();
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

        return ClusterState.builder(projectState.cluster())
            .routingTable(GlobalRoutingTable.builder().put(projectState.projectId(), unavailableRoutingTable).build())
            .build()
            .projectState(projectState.projectId());
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

    private ClusterChangedEvent event(ProjectState projectState) {
        return new ClusterChangedEvent("test-event", projectState.cluster(), EMPTY_CLUSTER_STATE);
    }

    private static Settings getSettings() {
        return indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6).build();
    }

    private static XContentBuilder getMappings() {
        return getMappings(Version.CURRENT.toString(), 6);
    }

    private static XContentBuilder getMappings(String nodeVersion, Integer mappingsVersion) {
        return getMappings(builder -> builder.object("_meta", meta -> {
            meta.field("version", nodeVersion);
            meta.field(SystemIndexDescriptor.VERSION_META_KEY, mappingsVersion);
        }));
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
