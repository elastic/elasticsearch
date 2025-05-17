/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRolesSynchronizer.MarkRolesAsSyncedTask;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesSynchronizer.QUERYABLE_BUILT_IN_ROLES_FEATURE;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtilsTests.buildQueryableBuiltInRoles;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class QueryableBuiltInRolesSynchronizerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("queryable-built-in-roles-synchronizer-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();

    private QueryableBuiltInRolesSynchronizer synchronizer;
    private NativeRolesStore nativeRolesStore;
    private ClusterService clusterService;
    private FeatureService featureService;
    private ThreadPool threadPool;

    private MasterServiceTaskQueue<MarkRolesAsSyncedTask> taskQueue;

    private QueryableReservedRolesProvider reservedRolesProvider;

    @BeforeClass
    public static void setupReservedRolesStore() {
        new ReservedRolesStore(); // initialize the store
    }

    @Before
    public void setupSynchronizer() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        nativeRolesStore = mock(NativeRolesStore.class);
        clusterService = mock(ClusterService.class);
        taskQueue = mockTaskQueue(clusterService);

        QueryableBuiltInRolesProviderFactory rolesProviderFactory = mock(QueryableBuiltInRolesProviderFactory.class);
        reservedRolesProvider = mock(QueryableReservedRolesProvider.class);
        when(rolesProviderFactory.createProvider(any(), any())).thenReturn(reservedRolesProvider);

        featureService = mock(FeatureService.class);

        synchronizer = new QueryableBuiltInRolesSynchronizer(
            clusterService,
            featureService,
            rolesProviderFactory,
            nativeRolesStore,
            mock(ReservedRolesStore.class),
            mock(FileRolesStore.class),
            threadPool
        );
    }

    private void assertInitialState() {
        verifyNoInteractions(nativeRolesStore);
        verifyNoInteractions(featureService);
        verifyNoInteractions(taskQueue);

        verify(reservedRolesProvider, times(1)).addListener(any());
        verifyNoMoreInteractions(reservedRolesProvider);

        verify(clusterService, times(1)).addLifecycleListener(any());
        verify(clusterService, times(1)).createTaskQueue(eq("mark-built-in-roles-as-synced-task-queue"), eq(Priority.LOW), any());
        verifyNoMoreInteractions(clusterService);

        verify(threadPool, times(1)).generic();
        verifyNoMoreInteractions(threadPool);
    }

    public void testSuccessfulSync() {
        assertInitialState();

        ClusterState clusterState = markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster())
            .blocks(emptyClusterBlocks())
            .build();

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor")
            )
        );
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));

        synchronizer.clusterChanged(event(clusterState));

        // called once on successful sync to update the digests in the cluster state
        verify(taskQueue, times(1)).submitTask(any(), argThat(task -> task.getNewRoleDigests().equals(builtInRoles.rolesDigest())), any());
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(featureService, times(1)).clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verify(reservedRolesProvider, times(1)).getRoles();
        verify(nativeRolesStore, times(1)).putRoles(
            eq(WriteRequest.RefreshPolicy.IMMEDIATE),
            eq(builtInRoles.roleDescriptors()),
            eq(false),
            any()
        );
        verify(clusterService, times(3)).state();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));
        assertThat(synchronizer.getFailedSyncAttempts(), equalTo(0));
    }

    public void testNotMaster() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeNotMaster()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(0)).clusterRecovered();
        verify(reservedRolesProvider, times(0)).getRoles();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testClusterNotRecovered() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster())
                .blocks(stateNotRecoveredClusterBlocks())
                .build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(reservedRolesProvider, times(0)).getRoles();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testNativeRolesDisabled() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        when(reservedRolesProvider.getRoles()).thenReturn(buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR)));
        mockDisabledNativeStore();

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(clusterState, times(2)).nodes();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testMixedVersionsCluster() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(mixedVersionNodes()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(clusterState, times(4)).nodes();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);

    }

    public void testNoDataNodes() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(noDataNodes()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(clusterState, times(3)).nodes();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testFeatureNotSupported() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(false);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(clusterState, times(4)).nodes();
        verify(featureService, times(1)).clusterHasFeature(eq(clusterState), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testSecurityIndexDeleted() {
        assertInitialState();

        ClusterState previousClusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster()).blocks(emptyClusterBlocks()).build()
        );
        ClusterState currentClusterState = spy(
            ClusterState.builder(CLUSTER_NAME)
                .nodes(localNodeMaster())
                .blocks(emptyClusterBlocks())
                .metadata(Metadata.builder().generateClusterUuidIfNeeded())
                .build()
        );
        when(clusterService.state()).thenReturn(currentClusterState);

        synchronizer.clusterChanged(event(currentClusterState, previousClusterState));

        verify(previousClusterState, times(2)).metadata();
        verify(currentClusterState, times(2)).metadata();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testSecurityIndexCreated() {
        assertInitialState();

        ClusterState currentClusterState = spy(
            markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster()).blocks(emptyClusterBlocks()).build()
        );

        ClusterState previousClusterState = spy(
            ClusterState.builder(CLUSTER_NAME)
                .nodes(localNodeMaster())
                .blocks(emptyClusterBlocks())
                .metadata(Metadata.builder().generateClusterUuidIfNeeded())
                .build()
        );

        when(clusterService.state()).thenReturn(currentClusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));

        synchronizer.clusterChanged(event(currentClusterState, previousClusterState));

        verify(taskQueue, times(1)).submitTask(any(), argThat(task -> task.getNewRoleDigests().equals(builtInRoles.rolesDigest())), any());
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(featureService, times(1)).clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verify(reservedRolesProvider, times(1)).getRoles();
        verify(nativeRolesStore, times(1)).putRoles(
            eq(WriteRequest.RefreshPolicy.IMMEDIATE),
            eq(builtInRoles.roleDescriptors()),
            eq(false),
            any()
        );
        verify(clusterService, times(3)).state();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));
    }

    public void testSecurityIndexClosed() {
        assertInitialState();

        ClusterState clusterState = spy(
            markShardsAvailable(createClusterStateWithClosedSecurityIndex()).nodes(localNodeMaster()).blocks(emptyClusterBlocks()).build()
        );

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(false);

        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());

        synchronizer.clusterChanged(event(clusterState));

        verify(clusterState, times(1)).clusterRecovered();
        verify(nativeRolesStore, times(1)).isEnabled();
        verify(clusterState, times(4)).nodes();
        verify(featureService, times(1)).clusterHasFeature(eq(clusterState), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
    }

    public void testUnexpectedSyncFailures() {
        assertInitialState();

        ClusterState clusterState = markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster())
            .blocks(emptyClusterBlocks())
            .build();

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final Set<String> roles = randomReservedRoles(randomIntBetween(1, 10));
        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(
            roles.stream().map(ReservedRolesStore::roleDescriptor).collect(Collectors.toSet())
        );
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockNativeRolesStoreWithFailure(builtInRoles.roleDescriptors(), Set.of(), new IllegalStateException("unexpected failure"));
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));

        for (int i = 1; i <= QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS + 5; i++) {
            synchronizer.clusterChanged(event(clusterState));
            if (i < QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS) {
                assertThat(synchronizer.getFailedSyncAttempts(), equalTo(i));
            } else {
                assertThat(synchronizer.getFailedSyncAttempts(), equalTo(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS));
            }
        }

        verify(nativeRolesStore, times(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS)).isEnabled();
        verify(featureService, times(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS)).clusterHasFeature(
            any(),
            eq(QUERYABLE_BUILT_IN_ROLES_FEATURE)
        );
        verify(reservedRolesProvider, times(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS)).getRoles();
        verify(nativeRolesStore, times(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS)).putRoles(
            eq(WriteRequest.RefreshPolicy.IMMEDIATE),
            eq(builtInRoles.roleDescriptors()),
            eq(false),
            any()
        );
        verify(clusterService, times(QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS)).state();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));
    }

    public void testFailedSyncAttemptsGetsResetAfterSuccessfulSync() {
        assertInitialState();

        ClusterState clusterState = markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster())
            .blocks(emptyClusterBlocks())
            .build();

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final Set<String> roles = randomReservedRoles(randomIntBetween(1, 10));
        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(
            roles.stream().map(ReservedRolesStore::roleDescriptor).collect(Collectors.toSet())
        );
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockNativeRolesStoreWithFailure(builtInRoles.roleDescriptors(), Set.of(), new IllegalStateException("unexpected failure"));
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));

        // assert failed sync attempts are counted
        int numOfSimulatedFailures = randomIntBetween(1, QueryableBuiltInRolesSynchronizer.MAX_FAILED_SYNC_ATTEMPTS - 1);
        for (int i = 0; i < numOfSimulatedFailures; i++) {
            synchronizer.clusterChanged(event(clusterState));
            assertThat(synchronizer.getFailedSyncAttempts(), equalTo(i + 1));
        }
        assertThat(synchronizer.getFailedSyncAttempts(), equalTo(numOfSimulatedFailures));

        // assert successful sync resets the failed sync attempts
        mockEnabledNativeStore(builtInRoles.roleDescriptors(), Set.of());
        synchronizer.clusterChanged(event(clusterState));
        assertThat(synchronizer.getFailedSyncAttempts(), equalTo(0));

        verify(nativeRolesStore, times(numOfSimulatedFailures + 1)).isEnabled();
        verify(featureService, times(numOfSimulatedFailures + 1)).clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verify(reservedRolesProvider, times(numOfSimulatedFailures + 1)).getRoles();
        verify(nativeRolesStore, times(numOfSimulatedFailures + 1)).putRoles(
            eq(WriteRequest.RefreshPolicy.IMMEDIATE),
            eq(builtInRoles.roleDescriptors()),
            eq(false),
            any()
        );
        verify(taskQueue, times(1)).submitTask(any(), argThat(task -> task.getNewRoleDigests().equals(builtInRoles.rolesDigest())), any());
        verify(clusterService, times(numOfSimulatedFailures + 3)).state();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));
    }

    public void testExpectedSyncFailuresAreNotCounted() {
        assertInitialState();

        ClusterState clusterState = markShardsAvailable(createClusterStateWithOpenSecurityIndex()).nodes(localNodeMaster())
            .blocks(emptyClusterBlocks())
            .build();

        when(clusterService.state()).thenReturn(clusterState);
        when(featureService.clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE))).thenReturn(true);

        final Set<String> roles = randomReservedRoles(randomIntBetween(1, 10));
        final QueryableBuiltInRoles builtInRoles = buildQueryableBuiltInRoles(
            roles.stream().map(ReservedRolesStore::roleDescriptor).collect(Collectors.toSet())
        );
        when(reservedRolesProvider.getRoles()).thenReturn(builtInRoles);
        mockNativeRolesStoreWithFailure(builtInRoles.roleDescriptors(), Set.of(), new UnavailableShardsException(null, "expected failure"));
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));

        synchronizer.clusterChanged(event(clusterState));

        assertThat(synchronizer.getFailedSyncAttempts(), equalTo(0));

        verify(nativeRolesStore, times(1)).isEnabled();
        verify(featureService, times(1)).clusterHasFeature(any(), eq(QUERYABLE_BUILT_IN_ROLES_FEATURE));
        verify(reservedRolesProvider, times(1)).getRoles();
        verify(nativeRolesStore, times(1)).putRoles(
            eq(WriteRequest.RefreshPolicy.IMMEDIATE),
            eq(builtInRoles.roleDescriptors()),
            eq(false),
            any()
        );
        verify(clusterService, times(1)).state();
        verifyNoMoreInteractions(nativeRolesStore, featureService, taskQueue, reservedRolesProvider, threadPool, clusterService);
        assertThat(synchronizer.isSynchronizationInProgress(), equalTo(false));
    }

    private Set<String> randomReservedRoles(int count) {
        assert count >= 0;
        if (count == 0) {
            return Set.of();
        }
        if (count == 1) {
            return Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        }

        final Set<String> reservedRoles = new HashSet<>();
        reservedRoles.add(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        final Set<String> allReservedRolesExceptSuperuser = ReservedRolesStore.names()
            .stream()
            .filter(role -> false == role.equals(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))
            .collect(Collectors.toSet());
        reservedRoles.addAll(randomUnique(() -> randomFrom(allReservedRolesExceptSuperuser), count - 1));
        return Collections.unmodifiableSet(reservedRoles);
    }

    private static ClusterState.Builder markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        final ClusterState cs = clusterStateBuilder.build();
        return ClusterState.builder(cs)
            .routingTable(
                SecurityTestUtils.buildIndexRoutingTable(
                    cs.metadata().getProject().index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex()
                )
            );
    }

    @SuppressWarnings("unchecked")
    private MasterServiceTaskQueue<MarkRolesAsSyncedTask> mockTaskQueue(ClusterService clusterService) {
        final MasterServiceTaskQueue<MarkRolesAsSyncedTask> masterServiceTaskQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.<MarkRolesAsSyncedTask>createTaskQueue(eq("mark-built-in-roles-as-synced-task-queue"), eq(Priority.LOW), any()))
            .thenReturn(masterServiceTaskQueue);
        doAnswer(i -> {
            assertThat(synchronizer.isSynchronizationInProgress(), equalTo(true));
            MarkRolesAsSyncedTask task = i.getArgument(1);
            var result = task.execute(clusterService.state());
            task.success(result.v2());
            return null;
        }).when(masterServiceTaskQueue).submitTask(any(), any(), any());
        return masterServiceTaskQueue;
    }

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
    }

    private ClusterChangedEvent event(ClusterState currentClusterState, ClusterState previousClusterState) {
        return new ClusterChangedEvent("test-event", currentClusterState, previousClusterState);
    }

    private static DiscoveryNodes localNodeMaster() {
        return nodes(randomIntBetween(1, 3), true).build();
    }

    private static DiscoveryNodes localNodeNotMaster() {
        return nodes(randomIntBetween(1, 3), false).build();
    }

    private static DiscoveryNodes noDataNodes() {
        return nodes(0, true).build();
    }

    private DiscoveryNodes mixedVersionNodes() {
        VersionInformation oldVersion = new VersionInformation(
            VersionUtils.randomVersionBetween(random(), null, VersionUtils.getPreviousVersion()),
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersionUtils.randomCompatibleVersion(random())
        );
        return nodes(randomIntBetween(1, 3), true).add(
            DiscoveryNodeUtils.builder("old-data-node")
                .name("old-data-node")
                .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                .version(oldVersion)
                .build()
        ).build();
    }

    private static ClusterBlocks emptyClusterBlocks() {
        return ClusterBlocks.EMPTY_CLUSTER_BLOCK;
    }

    private static ClusterBlocks stateNotRecoveredClusterBlocks() {
        return ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build();
    }

    private static DiscoveryNodes.Builder nodes(int dataNodes, boolean localNodeMaster) {
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();

        int totalNodes = 0;
        List<String> dataNodeIds = new ArrayList<>();
        for (int i = 0; i < dataNodes; i++) {
            String dataNodeId = "data-id-" + totalNodes++;
            dataNodeIds.add(dataNodeId);
            nodesBuilder.add(
                DiscoveryNodeUtils.builder(dataNodeId).name("data-node-" + i).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build()
            );
        }

        String masterNodeId = "master-id-" + totalNodes++;
        nodesBuilder.add(DiscoveryNodeUtils.builder(masterNodeId).name("master-node").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build());

        final String localNodeId;
        if (localNodeMaster) {
            localNodeId = masterNodeId;
        } else {
            localNodeId = randomFrom(dataNodeIds);
        }
        return nodesBuilder.localNodeId(localNodeId).masterNodeId(masterNodeId);
    }

    private void mockDisabledNativeStore() {
        when(nativeRolesStore.isEnabled()).thenReturn(false);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockEnabledNativeStore(final Collection<RoleDescriptor> rolesToUpsert, final Collection<String> rolesToDelete) {
        when(nativeRolesStore.isEnabled()).thenReturn(true);
        doAnswer(i -> {
            assertThat(synchronizer.isSynchronizationInProgress(), equalTo(true));
            ((ActionListener) i.getArgument(3)).onResponse(
                new BulkRolesResponse(
                    rolesToUpsert.stream()
                        .map(role -> BulkRolesResponse.Item.success(role.getName(), DocWriteResponse.Result.CREATED))
                        .toList()
                )
            );
            return null;
        }).when(nativeRolesStore)
            .putRoles(eq(WriteRequest.RefreshPolicy.IMMEDIATE), eq(rolesToUpsert), eq(false), any(ActionListener.class));

        doAnswer(i -> {
            ((ActionListener) i.getArgument(3)).onResponse(
                new BulkRolesResponse(
                    rolesToDelete.stream().map(role -> BulkRolesResponse.Item.success(role, DocWriteResponse.Result.DELETED)).toList()
                )
            );
            return null;
        }).when(nativeRolesStore)
            .deleteRoles(eq(rolesToDelete), eq(WriteRequest.RefreshPolicy.IMMEDIATE), eq(false), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockNativeRolesStoreWithFailure(
        final Collection<RoleDescriptor> rolesToUpsert,
        final Collection<String> rolesToDelete,
        Exception failure
    ) {
        when(nativeRolesStore.isEnabled()).thenReturn(true);
        doAnswer(i -> {
            assertThat(synchronizer.isSynchronizationInProgress(), equalTo(true));
            ((ActionListener) i.getArgument(3)).onResponse(
                new BulkRolesResponse(rolesToUpsert.stream().map(role -> BulkRolesResponse.Item.failure(role.getName(), failure)).toList())
            );
            return null;
        }).when(nativeRolesStore)
            .putRoles(eq(WriteRequest.RefreshPolicy.IMMEDIATE), eq(rolesToUpsert), eq(false), any(ActionListener.class));

        doAnswer(i -> {
            ((ActionListener) i.getArgument(3)).onResponse(
                new BulkRolesResponse(rolesToDelete.stream().map(role -> BulkRolesResponse.Item.failure(role, failure)).toList())
            );
            return null;
        }).when(nativeRolesStore)
            .deleteRoles(eq(rolesToDelete), eq(WriteRequest.RefreshPolicy.IMMEDIATE), eq(false), any(ActionListener.class));
    }

    private static ClusterState.Builder createClusterStateWithOpenSecurityIndex() {
        return createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
    }

    private static ClusterState.Builder createClusterStateWithClosedSecurityIndex() {
        return createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.CLOSE
        );
    }

    private static ClusterState.Builder createClusterState(String indexName, String aliasName, IndexMetadata.State state) {
        @FixForMultiProject(description = "randomize project-id")
        final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        final Metadata metadata = Metadata.builder()
            .put(
                SecurityIndexManagerTests.createProjectMetadata(
                    projectId,
                    indexName,
                    aliasName,
                    SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
                    state,
                    SecurityIndexManagerTests.getMappings()
                )
            )
            .build();
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew);

        return ClusterState.builder(SecurityIndexManagerTests.state(projectId))
            .metadata(metadata)
            .routingTable(routingTable)
            .putCompatibilityVersions(
                "test",
                new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(
                        indexName,
                        new SystemIndexDescriptor.MappingsVersion(SecuritySystemIndices.SecurityMainIndexMappingVersion.latest().id(), 0)
                    )
                )
            );
    }
}
