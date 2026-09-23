/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchShardWarmVolumesAction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardWarmVolumesTests extends ESTestCase {

    public void testShouldFetchOnlyWhenRelocationAndSourceMarkedForRemoval() {
        Index index = new Index("idx", randomUUID());
        ShardId shardId = new ShardId(index, 0);
        long startedAtMillis = randomNonNegativeLong();
        ClusterState drain = drainState(index, "source", "target", startedAtMillis);
        ClusterState rebalance = ClusterState.builder(drain)
            .metadata(Metadata.builder(drain.metadata()).removeCustom(NodesShutdownMetadata.TYPE))
            .build();

        ShardRouting relocating = TestShardRouting.shardRoutingBuilder(shardId, "target", false, INITIALIZING)
            .withRelocatingNodeId("source")
            .withRole(ShardRouting.Role.SEARCH_ONLY)
            .build();
        ShardRouting replicaInit = TestShardRouting.shardRoutingBuilder(shardId, "target", false, INITIALIZING)
            .withRole(ShardRouting.Role.SEARCH_ONLY)
            .build();

        assertTrue(ShardWarmVolumes.shouldFetch(relocating, drain));
        assertFalse(ShardWarmVolumes.shouldFetch(replicaInit, drain));
        assertFalse(ShardWarmVolumes.shouldFetch(relocating, rebalance));
    }

    public void testOneRpcForManyFetchesFromSameSource() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        Client client = mock(Client.class);
        AtomicReference<ActionListener<TransportFetchShardWarmVolumesAction.Response>> held = new AtomicReference<>();
        doAnswer(invocation -> {
            held.set(invocation.getArgument(2));
            return null;
        }).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ShardWarmVolumes volumes = newVolumes(client, state);

        for (int i = 0; i < 10; i++) {
            volumes.maybeFetch(state, "source");
        }
        verify(client, times(1)).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        assertTrue(volumes.isInFlight("source"));
        assertThat(volumes.peek("source"), nullValue());
    }

    public void testFailureLeavesMemoEmptyAndAllowsRetry() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        Client client = mock(Client.class);
        AtomicReference<ActionListener<TransportFetchShardWarmVolumesAction.Response>> held = new AtomicReference<>();
        doAnswer(invocation -> {
            held.set(invocation.getArgument(2));
            return null;
        }).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ShardWarmVolumes volumes = newVolumes(client, state);

        volumes.maybeFetch(state, "source");
        held.get().onFailure(new RuntimeException("rpc failed"));
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));

        volumes.maybeFetch(state, "source");
        verify(client, times(2)).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
    }

    public void testSynchronousExecuteFailureClearsInFlightAndAllowsRetry() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        Client client = mock(Client.class);
        AtomicReference<ActionListener<TransportFetchShardWarmVolumesAction.Response>> held = new AtomicReference<>();
        doThrow(new IllegalStateException("execute failed")).doAnswer(invocation -> {
            held.set(invocation.getArgument(2));
            return null;
        }).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ShardWarmVolumes volumes = newVolumes(client, state);

        volumes.maybeFetch(state, "source");
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));

        volumes.maybeFetch(state, "source");
        verify(client, times(2)).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        assertTrue(volumes.isInFlight("source"));
        assertNotNull(held.get());
    }

    public void testCompletionAfterRemovedNodesIsNotStored() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState withSource = drainState(index, "source", "target", startedAtMillis);
        ClusterState withoutSource = ClusterState.builder(withSource)
            .nodes(DiscoveryNodes.builder(withSource.nodes()).remove("source"))
            .build();
        Client client = mock(Client.class);
        AtomicReference<ActionListener<TransportFetchShardWarmVolumesAction.Response>> held = new AtomicReference<>();
        doAnswer(invocation -> {
            held.set(invocation.getArgument(2));
            return null;
        }).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ClusterService clusterService = clusterService(withSource);
        ShardWarmVolumes volumes = new ShardWarmVolumes(client, clusterService);
        volumes.maybeFetch(withSource, "source");

        when(clusterService.state()).thenReturn(withoutSource);
        volumes.clusterChanged(new ClusterChangedEvent("test", withoutSource, withSource));
        held.get().onResponse(new TransportFetchShardWarmVolumesAction.Response(startedAtMillis, Map.of(index, Map.of(0, 10L))));
        assertThat(volumes.peek("source"), nullValue());
    }

    public void testGenerationMismatchRefetches() {
        Index index = new Index("idx", randomUUID());
        long firstGen = randomLongBetween(1, 1000);
        long secondGen = firstGen + randomLongBetween(1, 1000);
        ClusterState first = drainState(index, "source", "target", firstGen);
        ClusterState second = drainState(index, "source", "target", secondGen);
        Client client = mock(Client.class);
        doAnswer(invocation -> null).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ShardWarmVolumes volumes = newVolumes(client, second);
        volumes.put("source", new ShardWarmVolumes.Entry(firstGen, Map.of(new ShardId(index, 0), 10L)));

        assertThat(volumes.get(second, "source"), nullValue());
        volumes.maybeFetch(second, "source");
        verify(client, times(1)).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
    }

    public void testSuccessfulPutIsVisibleToGet() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        Client client = mock(Client.class);
        AtomicReference<ActionListener<TransportFetchShardWarmVolumesAction.Response>> held = new AtomicReference<>();
        doAnswer(invocation -> {
            held.set(invocation.getArgument(2));
            return null;
        }).when(client).execute(eq(TransportFetchShardWarmVolumesAction.TYPE), any(), any());
        ShardWarmVolumes volumes = newVolumes(client, state);
        volumes.maybeFetch(state, "source");
        var response = new TransportFetchShardWarmVolumesAction.Response(startedAtMillis, Map.of(index, Map.of(0, 99L)));
        held.get().onResponse(response);
        assertThat(volumes.get(state, "source").volumes(), equalTo(Map.of(new ShardId(index, 0), 99L)));
        assertThat(volumes.get(state, "source"), sameInstance(volumes.peek("source")));
    }

    private static ShardWarmVolumes newVolumes(Client client, ClusterState state) {
        return new ShardWarmVolumes(client, clusterService(state));
    }

    private static ClusterService clusterService(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING.getKey(), true)
                .build(),
            Set.of(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static ClusterState drainState(Index index, String sourceNodeId, String targetNodeId, long startedAtMillis) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 1))
            .build();
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(sourceNodeId)
            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
            .setReason("test")
            .setStartedAtMillis(startedAtMillis)
            .setNodeSeen(true)
            .build();
        return ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(sourceNodeId))
                    .add(DiscoveryNodeUtils.create(targetNodeId))
                    .localNodeId(targetNodeId)
                    .masterNodeId(targetNodeId)
                    .build()
            )
            .putCompatibilityVersions(sourceNodeId, TransportVersion.current(), Map.of())
            .putCompatibilityVersions(targetNodeId, TransportVersion.current(), Map.of())
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(sourceNodeId, shutdown)))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .build();
    }
}
