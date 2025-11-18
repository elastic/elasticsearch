/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.SecurityFeatures;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper.routingTable;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.FILE_SETTINGS_METADATA_NAMESPACE;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SecurityIndexManagerTests extends ESTestCase {
    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();
    private SystemIndexDescriptor descriptorSpy;
    private ThreadPool threadPool;
    private ProjectId projectId;
    private SecurityIndexManager manager;

    private int putMappingRequestCount = 0;

    @Before
    public void setUpManager() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // Build a mock client that always accepts put mappings requests
        final Client client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof PutMappingRequest) {
                    putMappingRequestCount++;
                    listener.onResponse((Response) AcknowledgedResponse.of(true));
                }
            }
        };

        final FeatureService featureService = new FeatureService(List.of(new SecurityFeatures()));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        final SystemIndexDescriptor descriptor = new SecuritySystemIndices(clusterService.getSettings()).getSystemIndexDescriptors()
            .stream()
            .filter(d -> d.getAliasName().equals(SecuritySystemIndices.SECURITY_MAIN_ALIAS))
            .findFirst()
            .get();
        descriptorSpy = spy(descriptor);
        projectId = randomProjectIdOrDefault();
        manager = SecurityIndexManager.buildSecurityIndexManager(
            client,
            clusterService,
            featureService,
            TestProjectResolvers.singleProject(projectId),
            descriptorSpy
        );
    }

    public void testIndexWithUpToDateMappingAndTemplate() {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));

        final IndexState index = manager.getProject(projectId);
        assertThat(index.indexExists(), Matchers.equalTo(true));
        assertThat(index.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(true));
        assertThat(index.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(true));
        assertThat(index.isMappingUpToDate(), Matchers.equalTo(true));
    }

    public void testIndexWithoutPrimaryShards() {
        assertInitialState();

        final ClusterState cs = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        ).build();
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(cs);
        Index index = cs.metadata().getProject(projectId).index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        clusterStateBuilder.routingTable(
            routingTable(
                projectId,
                IndexRoutingTable.builder(index)
                    .addIndexShard(
                        IndexShardRoutingTable.builder(new ShardId(index, 0))
                            .addShard(
                                shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize())
                                    .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
                            )
                    )
                    .build()
            )
        );
        manager.clusterChanged(event(clusterStateBuilder.build()));

        assertIndexUpToDateButNotAvailable();
    }

    public void testIndexAvailability() {
        assertInitialState();
        final ClusterState cs = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        ).build();
        Index index = cs.metadata().getProject(projectId).index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
        ShardId shardId = new ShardId(index, 0);
        ShardRouting primary = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.INDEX_ONLY
        );
        ShardRouting replica = ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null),
            ShardRouting.Role.SEARCH_ONLY
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        String nodeId2 = ESTestCase.randomAlphaOfLength(8);

        // primary/index unavailable, replica/search unavailable
        IndexShardRoutingTable.Builder indxShardRoutingTableBuilder = IndexShardRoutingTable.builder(shardId)
            .addShard(
                primary.initialize(nodeId, null, primary.getExpectedShardSize())
                    .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
            )
            .addShard(
                replica.initialize(nodeId2, null, replica.getExpectedShardSize())
                    .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
            );
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addIndexShard(indxShardRoutingTableBuilder);
        GlobalRoutingTable routingTable = GlobalRoutingTable.builder()
            .put(projectId, RoutingTable.builder().add(indexRoutingTableBuilder.build()).build())
            .build();
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(cs);
        clusterStateBuilder.routingTable(routingTable);
        ClusterState clusterState = clusterStateBuilder.build();
        manager.clusterChanged(event(clusterState));
        var projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), Matchers.equalTo(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(projectIndex.isProjectAvailable(), Matchers.equalTo(true));

        // primary/index available, replica/search available
        indxShardRoutingTableBuilder = IndexShardRoutingTable.builder(shardId)
            .addShard(
                primary.initialize(nodeId, null, primary.getExpectedShardSize()).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            )
            .addShard(
                replica.initialize(nodeId2, null, replica.getExpectedShardSize())
                    .moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) // start replica
            );
        indexRoutingTableBuilder = IndexRoutingTable.builder(index).addIndexShard(indxShardRoutingTableBuilder);
        routingTable = GlobalRoutingTable.builder()
            .put(projectId, RoutingTable.builder().add(indexRoutingTableBuilder.build()).build())
            .build();
        clusterStateBuilder = ClusterState.builder(cs);
        clusterStateBuilder.routingTable(routingTable);
        clusterState = clusterStateBuilder.build();
        manager.clusterChanged(event(clusterState));
        projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), Matchers.equalTo(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(true));
        assertThat(projectIndex.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(projectIndex.isProjectAvailable(), Matchers.equalTo(true));

        // primary/index available, replica/search unavailable
        indxShardRoutingTableBuilder = IndexShardRoutingTable.builder(shardId)
            .addShard(
                primary.initialize(nodeId, null, primary.getExpectedShardSize()).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            )
            .addShard(replica.initialize(nodeId2, null, replica.getExpectedShardSize())); // initialized, but not started
        indexRoutingTableBuilder = IndexRoutingTable.builder(index).addIndexShard(indxShardRoutingTableBuilder);
        routingTable = GlobalRoutingTable.builder()
            .put(projectId, RoutingTable.builder().add(indexRoutingTableBuilder.build()).build())
            .build();
        clusterStateBuilder = ClusterState.builder(cs);
        clusterStateBuilder.routingTable(routingTable);
        clusterState = clusterStateBuilder.build();
        manager.clusterChanged(event(clusterState));
        projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), Matchers.equalTo(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(true));
        assertThat(projectIndex.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(projectIndex.isProjectAvailable(), Matchers.equalTo(true));

        // primary/index unavailable, replica/search available
        // it is not currently possibly to have unassigned primaries with assigned replicas
    }

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
    }

    public void testIndexHealthChangeListeners() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final AtomicReference<ProjectId> projectIdRef = new AtomicReference<>();
        final AtomicReference<IndexState> previousState = new AtomicReference<>();
        final AtomicReference<IndexState> currentState = new AtomicReference<>();
        final TriConsumer<ProjectId, IndexState, IndexState> listener = (projId, prevState, state) -> {
            projectIdRef.set(projId);
            previousState.set(prevState);
            currentState.set(state);
            listenerCalled.set(true);
        };
        manager.addStateListener(listener);

        // index doesn't exist and now exists
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        );
        final ClusterState clusterState = markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterState));

        assertTrue(listenerCalled.get());
        assertThat(projectIdRef.get(), equalTo(projectId));
        assertNull(previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);

        // reset and call with no change to the index
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        ClusterChangedEvent event = new ClusterChangedEvent("same index health", clusterState, clusterState);
        manager.clusterChanged(event);

        assertFalse(listenerCalled.get());
        assertNull(previousState.get());
        assertNull(currentState.get());

        // index with different health
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        Index prevIndex = clusterState.routingTable(projectId).index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
        final ClusterState newClusterState = ClusterState.builder(clusterState)
            .routingTable(
                routingTable(
                    projectId,
                    IndexRoutingTable.builder(prevIndex)
                        .addIndexShard(
                            new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0)).addShard(
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
                        .build()
                )
            )
            .build();

        event = new ClusterChangedEvent("different index health", newClusterState, clusterState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertThat(projectIdRef.get(), equalTo(projectId));
        assertEquals(ClusterHealthStatus.GREEN, previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.RED, currentState.get().indexHealth);

        // swap prev and current
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        event = new ClusterChangedEvent("different index health swapped", clusterState, newClusterState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertThat(projectIdRef.get(), equalTo(projectId));
        assertEquals(ClusterHealthStatus.RED, previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);
    }

    public void testWriteBeforeStateNotRecovered() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(notNullValue()));
        assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(prepareRunnableCalled.get(), is(false));

        prepareException.set(null);
        prepareRunnableCalled.set(false);
        // state not recovered
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks).build()));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(notNullValue()));
        assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(prepareRunnableCalled.get(), is(false));

        prepareException.set(null);
        prepareRunnableCalled.set(false);
        // state recovered with index
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(nullValue()));
        assertThat(prepareRunnableCalled.get(), is(true));
    }

    /**
     * Check that the security index manager will update an index's mappings if they are out-of-date.
     * Although the {@index SystemIndexManager} normally handles this, the {@link SecurityIndexManager}
     * expects to be able to handle this also.
     */
    public void testCanUpdateIndexMappings() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        // Ensure that the mappings for the index are out-of-date, so that the security index manager will
        // attempt to update them.
        int previousVersion = randomValueOtherThanMany(
            v -> v.onOrAfter(SecurityMainIndexMappingVersion.latest()),
            () -> randomFrom(SecurityMainIndexMappingVersion.values())
        ).id();

        // State recovered with index, with mappings with a prior version
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(previousVersion)
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(true));
        assertThat(prepareException.get(), nullValue());
        // Verify that the client to send put mapping was used
        assertThat(putMappingRequestCount, equalTo(1));
    }

    /**
     * Check that the security index manager will refuse to update mappings on an index
     * if the corresponding {@link SystemIndexDescriptor} requires a higher mapping version
     * that the cluster's current minimum version.
     */
    public void testCannotUpdateIndexMappingsWhenMinMappingVersionTooLow() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        // Hard-code a failure here.
        doReturn("Nope").when(descriptorSpy).getMinimumMappingsVersionMessage(anyString(), any());
        doReturn(null).when(descriptorSpy)
            .getDescriptorCompatibleWith(eq(new SystemIndexDescriptor.MappingsVersion(SecurityMainIndexMappingVersion.latest().id(), 0)));

        // Ensure that the mappings for the index are out-of-date, so that the security index manager will
        // attempt to update them.
        int previousVersion = randomValueOtherThanMany(
            v -> v.onOrAfter(SecurityMainIndexMappingVersion.latest()),
            () -> randomFrom(SecurityMainIndexMappingVersion.values())
        ).id();

        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(previousVersion)
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(false));

        final Exception exception = prepareException.get();
        assertThat(exception, not(nullValue()));
        assertThat(exception, instanceOf(IllegalStateException.class));
        assertThat(exception.getMessage(), equalTo("Nope"));
        // Verify that the client to send put mapping was never used
        assertThat(putMappingRequestCount, equalTo(0));
    }

    /**
     * Check that the security index manager will not update mappings on an index if the mapping version wasn't bumped
     */
    public void testNoUpdateWhenIndexMappingsVersionNotBumped() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(SecurityMainIndexMappingVersion.latest().id())
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(true));
        assertThat(prepareException.get(), is(nullValue()));
        // Verify that the client to send put mapping was never used
        assertThat(putMappingRequestCount, equalTo(0));
    }

    /**
     * Check that the security index manager will not update mappings on an index if there is no mapping version in cluster state
     */
    public void testNoUpdateWhenNoIndexMappingsVersionInClusterState() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(SecurityMainIndexMappingVersion.latest().id()),
            Map.of()
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.getProject(projectId).prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(true));
        assertThat(prepareException.get(), is(nullValue()));
        // Verify that the client to send put mapping was never used
        assertThat(putMappingRequestCount, equalTo(0));
    }

    public void testListenerNotCalledBeforeStateNotRecovered() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        manager.addStateListener((proj, prev, current) -> listenerCalled.set(true));
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        // state not recovered
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks).build()));
        assertThat(manager.getProject(projectId).isProjectAvailable(), is(false));
        assertThat(listenerCalled.get(), is(false));
        // state recovered with index
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertThat(manager.getProject(projectId).isProjectAvailable(), is(true));
        assertThat(listenerCalled.get(), is(true));
    }

    public void testIndexOutOfDateListeners() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final ClusterState clusterState = state();
        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterState, clusterState));
        AtomicBoolean upToDateChanged = new AtomicBoolean();
        manager.addStateListener((projId, prev, current) -> {
            assertThat(projId, equalTo(projectId));
            listenerCalled.set(true);
            upToDateChanged.set(prev.isIndexUpToDate != current.isIndexUpToDate);
        });
        assertThat(manager.getProject(projectId).isIndexUpToDate(), is(true));

        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterState, clusterState));
        assertFalse(listenerCalled.get());
        assertTrue(manager.getProject(projectId).isIndexUpToDate());

        // index doesn't exist and now exists with wrong format
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT - 1
        );
        manager.clusterChanged(new ClusterChangedEvent("test-event", markShardsAvailable(clusterStateBuilder), clusterState));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertFalse(manager.getProject(projectId).isIndexUpToDate());

        listenerCalled.set(false);
        assertFalse(listenerCalled.get());
        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterState, clusterState));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertTrue(manager.getProject(projectId).isIndexUpToDate());

        listenerCalled.set(false);
        // index doesn't exist and now exists with correct format
        clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(new ClusterChangedEvent("test-event", markShardsAvailable(clusterStateBuilder), clusterState));
        assertTrue(listenerCalled.get());
        assertFalse(upToDateChanged.get());
        assertTrue(manager.getProject(projectId).isIndexUpToDate());
    }

    public void testReadyForMigration() {
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        clusterStateBuilder.nodeFeatures(
            Map.of("1", new SecurityFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet()))
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertTrue(manager.getProject(projectId).isReadyForSecurityMigration(new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }
        }));
    }

    public void testNotReadyForMigrationBecauseOfFeature() {
        final ProjectId projectId = DEFAULT_PROJECT_ID;
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        clusterStateBuilder.nodeFeatures(
            Map.of("1", new SecurityFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet()))
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertFalse(manager.getProject(projectId).isReadyForSecurityMigration(new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of(new NodeFeature("not a real feature"));
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }
        }));
    }

    public void testNotReadyForMigrationBecauseOfMappingVersion() {
        final ProjectId projectId = DEFAULT_PROJECT_ID;
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        clusterStateBuilder.nodeFeatures(
            Map.of("1", new SecurityFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet()))
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertFalse(manager.getProject(projectId).isReadyForSecurityMigration(new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 1000;
            }
        }));
    }

    public void testNotReadyForMigrationBecauseOfPrecondition() {
        final ProjectId projectId = DEFAULT_PROJECT_ID;
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        clusterStateBuilder.nodeFeatures(
            Map.of("1", new SecurityFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet()))
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertFalse(manager.getProject(projectId).isReadyForSecurityMigration(new SecurityMigrations.SecurityMigration() {
            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }

            @Override
            public boolean checkPreConditions(IndexState securityIndexManagerState) {
                return false;
            }
        }));
    }

    private ClusterState.Builder clusterStateBuilderForMigrationTesting() {
        return createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
    }

    public void testGetRoleMappingsCleanupMigrationStatus() {
        {
            assertThat(
                SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
                    clusterStateBuilderForMigrationTesting().build().projectState(projectId),
                    SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION
                ),
                equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.DONE)
            );
        }
        {
            // Migration should be skipped
            ClusterState.Builder clusterStateBuilder = clusterStateBuilderForMigrationTesting();
            Metadata.Builder metadataBuilder = Metadata.builder(state().metadata());
            metadataBuilder.put(ReservedStateMetadata.builder(FILE_SETTINGS_METADATA_NAMESPACE).build());
            assertThat(
                SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
                    clusterStateBuilder.metadata(metadataBuilder).build().projectState(projectId),
                    1
                ),
                equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.SKIP)
            );
        }
        {
            // Not ready for migration
            ClusterState.Builder clusterStateBuilder = clusterStateBuilderForMigrationTesting();
            Metadata.Builder metadataBuilder = Metadata.builder(state().metadata());
            ReservedStateMetadata.Builder builder = ReservedStateMetadata.builder(FILE_SETTINGS_METADATA_NAMESPACE);
            // File settings role mappings exist
            ReservedStateHandlerMetadata reservedStateHandlerMetadata = new ReservedStateHandlerMetadata(
                ReservedRoleMappingAction.NAME,
                Set.of("role_mapping_1")
            );
            builder.putHandler(reservedStateHandlerMetadata);
            metadataBuilder.put(builder.build());

            // No role mappings in cluster state yet
            metadataBuilder.putCustom(RoleMappingMetadata.TYPE, new RoleMappingMetadata(Set.of()));

            assertThat(
                SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
                    clusterStateBuilder.metadata(metadataBuilder).build().projectState(projectId),
                    1
                ),
                equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.NOT_READY)
            );
        }
        {
            // Old role mappings in cluster state
            final ClusterState.Builder clusterStateBuilder = clusterStateBuilderForMigrationTesting();
            Metadata.Builder metadataBuilder = Metadata.builder(state().metadata());
            ReservedStateMetadata.Builder builder = ReservedStateMetadata.builder(FILE_SETTINGS_METADATA_NAMESPACE);
            // File settings role mappings exist
            ReservedStateHandlerMetadata reservedStateHandlerMetadata = new ReservedStateHandlerMetadata(
                ReservedRoleMappingAction.NAME,
                Set.of("role_mapping_1")
            );
            builder.putHandler(reservedStateHandlerMetadata);
            metadataBuilder.put(builder.build());

            // Role mappings in cluster state with fallback name
            metadataBuilder.putCustom(
                RoleMappingMetadata.TYPE,
                new RoleMappingMetadata(Set.of(new ExpressionRoleMapping(RoleMappingMetadata.FALLBACK_NAME, null, null, null, null, true)))
            );

            assertThat(
                SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
                    clusterStateBuilder.metadata(metadataBuilder).build().projectState(projectId),
                    1
                ),
                equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.NOT_READY)
            );
        }
        {
            // Ready for migration
            final ClusterState.Builder clusterStateBuilder = clusterStateBuilderForMigrationTesting();
            Metadata.Builder metadataBuilder = Metadata.builder(state().metadata());
            ReservedStateMetadata.Builder builder = ReservedStateMetadata.builder(FILE_SETTINGS_METADATA_NAMESPACE);
            // File settings role mappings exist
            ReservedStateHandlerMetadata reservedStateHandlerMetadata = new ReservedStateHandlerMetadata(
                ReservedRoleMappingAction.NAME,
                Set.of("role_mapping_1")
            );
            builder.putHandler(reservedStateHandlerMetadata);
            metadataBuilder.put(builder.build());

            // Role mappings in cluster state
            metadataBuilder.putCustom(
                RoleMappingMetadata.TYPE,
                new RoleMappingMetadata(Set.of(new ExpressionRoleMapping("role_mapping_1", null, null, null, null, true)))
            );

            assertThat(
                SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
                    clusterStateBuilder.metadata(metadataBuilder).build().projectState(projectId),
                    1
                ),
                equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.READY)
            );
        }
    }

    public void testProcessClosedIndexState() {
        // Index initially exists
        final ClusterState.Builder indexAvailable = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        manager.clusterChanged(event(markShardsAvailable(indexAvailable)));
        var projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), is(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), is(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), is(true));

        // Now close it
        ClusterState.Builder indexClosed = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.CLOSE
        );
        if (randomBoolean()) {
            // In old/mixed cluster versions closed indices have no routing table
            indexClosed.routingTable(routingTable(projectId, RoutingTable.EMPTY_ROUTING_TABLE));
        } else {
            indexClosed = ClusterState.builder(markShardsAvailable(indexClosed));
        }

        manager.clusterChanged(event(indexClosed.build()));
        projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), is(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), is(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), is(false));
    }

    public void testAddRemoveProjects() {
        final ProjectId projectId1 = randomUniqueProjectId();
        final ProjectId projectId2 = randomUniqueProjectId();
        final ProjectId projectId3 = randomUniqueProjectId();

        final Map<ProjectId, Tuple<IndexState, IndexState>> listeners = new HashMap<>();
        manager.addStateListener((projId, oldState, newState) -> listeners.put(projId, Tuple.tuple(oldState, newState)));

        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(false));
        manager.clusterChanged(new ClusterChangedEvent("initial", ClusterState.EMPTY_STATE, ClusterState.EMPTY_STATE));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(false));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(false));

        assertThat(listeners, aMapWithSize(1));
        assertThat(listeners.keySet(), contains(DEFAULT_PROJECT_ID));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v1().isProjectAvailable(), is(false));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v2().isProjectAvailable(), is(true));
        listeners.clear();

        ClusterState oldState = ClusterState.EMPTY_STATE;
        Metadata metadata = Metadata.builder(oldState.metadata()).put(ProjectMetadata.builder(projectId1)).build();
        ClusterState newState = ClusterState.builder(oldState)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        manager.clusterChanged(new ClusterChangedEvent("add-project-1", newState, oldState));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(false));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId1).indexExists(), is(false));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(false));

        assertThat(listeners, aMapWithSize(1));
        assertThat(listeners.keySet(), contains(projectId1));
        assertThat(listeners.get(projectId1).v1().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId1).v2().isProjectAvailable(), is(true));
        listeners.clear();

        oldState = newState;
        metadata = Metadata.builder(oldState.metadata()).put(createProjectMetadata(projectId2)).build();
        newState = ClusterState.builder(oldState)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        manager.clusterChanged(new ClusterChangedEvent("add-project-2", newState, oldState));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(false));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId1).indexExists(), is(false));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId2).indexExists(), is(true));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(false));

        assertThat(listeners, aMapWithSize(1));
        assertThat(listeners.keySet(), contains(projectId2));
        assertThat(listeners.get(projectId2).v1().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId2).v2().isProjectAvailable(), is(true));
        listeners.clear();

        oldState = newState;
        metadata = Metadata.builder(oldState.metadata())
            .put(createProjectMetadata(DEFAULT_PROJECT_ID)) // <- adds security index to project
            .put(createProjectMetadata(projectId1)) // <- adds security index to project
            .build();
        newState = ClusterState.builder(oldState)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        manager.clusterChanged(new ClusterChangedEvent("add-indices", newState, oldState));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(true));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId1).indexExists(), is(true));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId2).indexExists(), is(true));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(false));

        assertThat(listeners, aMapWithSize(2));
        assertThat(listeners.keySet(), containsInAnyOrder(DEFAULT_PROJECT_ID, projectId1));

        assertThat(listeners.get(DEFAULT_PROJECT_ID).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v1().indexExists(), is(false));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v2().isProjectAvailable(), is(true));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v2().indexExists(), is(true));

        assertThat(listeners.get(projectId1).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId1).v1().indexExists(), is(false));
        assertThat(listeners.get(projectId1).v2().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId1).v2().indexExists(), is(true));

        listeners.clear();

        oldState = newState;
        metadata = Metadata.builder(oldState.metadata()).removeProject(projectId1).put(createProjectMetadata(projectId3)).build();
        newState = ClusterState.builder(oldState)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        manager.clusterChanged(new ClusterChangedEvent("remove-project1 + add-project-3", newState, oldState));

        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(true));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId2).indexExists(), is(true));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(true));
        assertThat(manager.getProject(projectId3).indexExists(), is(true));

        assertThat(listeners, aMapWithSize(2));
        assertThat(listeners.keySet(), containsInAnyOrder(projectId1, projectId3));

        assertThat(listeners.get(projectId1).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId1).v1().indexExists(), is(true));
        assertThat(listeners.get(projectId1).v2().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId1).v2().indexExists(), is(false));

        assertThat(listeners.get(projectId3).v1().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId3).v1().indexExists(), is(false));
        assertThat(listeners.get(projectId3).v2().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId3).v2().indexExists(), is(true));
        listeners.clear();

        oldState = newState;
        newState = ClusterState.EMPTY_STATE;
        manager.clusterChanged(new ClusterChangedEvent("reset-to-empty", newState, oldState));

        assertThat(manager.getProject(DEFAULT_PROJECT_ID).isProjectAvailable(), is(true));
        assertThat(manager.getProject(DEFAULT_PROJECT_ID).indexExists(), is(false));
        assertThat(manager.getProject(projectId1).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId2).isProjectAvailable(), is(false));
        assertThat(manager.getProject(projectId3).isProjectAvailable(), is(false));

        assertThat(listeners, aMapWithSize(3));
        assertThat(listeners.keySet(), containsInAnyOrder(DEFAULT_PROJECT_ID, projectId2, projectId3));

        assertThat(listeners.get(DEFAULT_PROJECT_ID).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v1().indexExists(), is(true));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v2().isProjectAvailable(), is(true));
        assertThat(listeners.get(DEFAULT_PROJECT_ID).v2().indexExists(), is(false));

        assertThat(listeners.get(projectId2).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId2).v1().indexExists(), is(true));
        assertThat(listeners.get(projectId2).v2().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId2).v2().indexExists(), is(false));

        assertThat(listeners.get(projectId3).v1().isProjectAvailable(), is(true));
        assertThat(listeners.get(projectId3).v1().indexExists(), is(true));
        assertThat(listeners.get(projectId3).v2().isProjectAvailable(), is(false));
        assertThat(listeners.get(projectId3).v2().indexExists(), is(false));
        listeners.clear();
    }

    private void assertInitialState() {
        final ProjectId projectId = DEFAULT_PROJECT_ID;
        final var projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), Matchers.equalTo(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isMappingUpToDate(), Matchers.equalTo(false));
        assertThat(projectIndex.isProjectAvailable(), Matchers.equalTo(false));
    }

    private void assertIndexUpToDateButNotAvailable() {
        final var projectIndex = manager.getProject(projectId);
        assertThat(projectIndex.indexExists(), Matchers.equalTo(true));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS), Matchers.equalTo(false));
        assertThat(projectIndex.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(projectIndex.isProjectAvailable(), Matchers.equalTo(true));
    }

    public ClusterState.Builder createClusterState(String indexName, String aliasName) {
        return createClusterState(indexName, aliasName, IndexMetadata.State.OPEN);
    }

    public ClusterState.Builder createClusterState(String indexName, String aliasName, IndexMetadata.State state) {
        return createClusterState(indexName, aliasName, SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT, state, getMappings());
    }

    public ClusterState.Builder createClusterState(String indexName, String aliasName, int format) {
        return createClusterState(indexName, aliasName, format, IndexMetadata.State.OPEN, getMappings());
    }

    private ClusterState.Builder createClusterState(
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings
    ) {
        return createClusterState(
            indexName,
            aliasName,
            format,
            state,
            mappings,
            Map.of(indexName, new SystemIndexDescriptor.MappingsVersion(SecurityMainIndexMappingVersion.latest().id(), 0))
        );
    }

    private ClusterState.Builder createClusterState(
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings,
        Map<String, SystemIndexDescriptor.MappingsVersion> compatibilityVersions
    ) {
        final Metadata metadata = Metadata.builder()
            .put(createProjectMetadata(projectId, indexName, aliasName, format, state, mappings))
            .build();
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew);

        return ClusterState.builder(state())
            .metadata(metadata)
            .routingTable(routingTable)
            .putCompatibilityVersions("test", new CompatibilityVersions(TransportVersion.current(), compatibilityVersions));
    }

    public static ProjectMetadata.Builder createProjectMetadata(
        ProjectId projectId,
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings
    ) {
        return ProjectMetadata.builder(projectId).put(getIndexMetadata(indexName, aliasName, format, state, mappings));
    }

    private static ProjectMetadata.Builder createProjectMetadata(ProjectId projectId) {
        return createProjectMetadata(
            projectId,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings()
        );
    }

    private ClusterState markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        final ClusterState cs = clusterStateBuilder.build();
        final RoutingTable projectRouting = SecurityTestUtils.buildIndexRoutingTable(
            cs.metadata().getProject(projectId).index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex()
        );
        return ClusterState.builder(cs).routingTable(GlobalRoutingTable.builder().put(projectId, projectRouting).build()).build();
    }

    private ClusterState state() {
        return state(projectId);
    }

    public static ClusterState state(ProjectId projectId) {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .masterNodeId("1")
            .localNodeId("1")
            .build();
        final Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(projectId)).generateClusterUuidIfNeeded().build();
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes).metadata(metadata).build();
    }

    private static IndexMetadata.Builder getIndexMetadata(
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings
    ) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), format));
        indexMetadata.putAlias(AliasMetadata.builder(aliasName).build());
        indexMetadata.state(state);
        if (mappings != null) {
            indexMetadata.putMapping(mappings);
        }

        return indexMetadata;
    }

    public static String getMappings() {
        return getMappings(SecurityMainIndexMappingVersion.latest().id());
    }

    private static String getMappings(Integer version) {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                if (version != null) {
                    builder.field(SystemIndexDescriptor.VERSION_META_KEY, version);
                }
                // This is expected to be ignored
                builder.field("security-version", "8.13.0");
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
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build index mappings", e);
        }
    }
}
