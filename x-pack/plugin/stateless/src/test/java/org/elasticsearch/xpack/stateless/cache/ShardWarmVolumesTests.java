/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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

    public void testClaimFetchWinsOnceWhileInFlight() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        ShardWarmVolumes volumes = newVolumes();

        assertTrue(volumes.claimFetch(state, "source"));
        assertTrue(volumes.isInFlight("source"));
        assertFalse(volumes.claimFetch(state, "source"));
    }

    public void testEmptyEntryIsMissForFormulaAndBlocksClaim() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        ShardWarmVolumes volumes = newVolumes();
        volumes.put("source", new ShardWarmVolumes.Entry(startedAtMillis, Map.of()));

        assertThat(volumes.get(state, "source"), nullValue());
        assertNotNull(volumes.entryForGeneration(state, "source"));
        assertFalse(volumes.claimFetch(state, "source"));
    }

    public void testGenerationMismatchIsMissAndAllowsReclaim() {
        Index index = new Index("idx", randomUUID());
        long firstGen = randomLongBetween(1, 1000);
        long secondGen = firstGen + randomLongBetween(1, 1000);
        ClusterState second = drainState(index, "source", "target", secondGen);
        ShardWarmVolumes volumes = newVolumes();
        volumes.put("source", new ShardWarmVolumes.Entry(firstGen, Map.of(new ShardId(index, 0), 10L)));

        assertThat(volumes.get(second, "source"), nullValue());
        assertThat(volumes.entryForGeneration(second, "source"), nullValue());
        assertTrue(volumes.claimFetch(second, "source"));
    }

    public void testCompleteFetchStoresAndReleasesClaim() {
        Index index = new Index("idx", randomUUID());
        ShardId shardId = new ShardId(index, 0);
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        ShardWarmVolumes volumes = newVolumes();
        assertTrue(volumes.claimFetch(state, "source"));

        volumes.completeFetch(state, "source", "source", startedAtMillis, Map.of(shardId, 10L));
        assertFalse(volumes.isInFlight("source"));
        assertThat(volumes.get(state, "source").volumes(), equalTo(Map.of(shardId, 10L)));
        assertFalse(volumes.claimFetch(state, "source"));
    }

    public void testCompleteFetchGenerationMismatchDoesNotStore() {
        Index index = new Index("idx", randomUUID());
        ShardId shardId = new ShardId(index, 0);
        long firstGen = randomLongBetween(1, 1000);
        long secondGen = firstGen + randomLongBetween(1, 1000);
        ClusterState first = drainState(index, "source", "target", firstGen);
        ClusterState second = drainState(index, "source", "target", secondGen);
        ShardWarmVolumes volumes = newVolumes();
        assertTrue(volumes.claimFetch(first, "source"));

        volumes.completeFetch(second, "source", "source", firstGen, Map.of(shardId, 10L));
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));
    }

    public void testCompleteFetchAfterSourceLeftDoesNotStore() {
        Index index = new Index("idx", randomUUID());
        ShardId shardId = new ShardId(index, 0);
        long startedAtMillis = randomNonNegativeLong();
        ClusterState withSource = drainState(index, "source", "target", startedAtMillis);
        ClusterState withoutSource = ClusterState.builder(withSource)
            .nodes(DiscoveryNodes.builder(withSource.nodes()).remove("source"))
            .build();
        ShardWarmVolumes volumes = newVolumes();
        assertTrue(volumes.claimFetch(withSource, "source"));

        volumes.completeFetch(withoutSource, "source", "source", startedAtMillis, Map.of(shardId, 10L));
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));
    }

    public void testDisabledSettingDoesNotClaimOrReturnStoredEntry() {
        Index index = new Index("idx", randomUUID());
        ShardId shardId = new ShardId(index, 0);
        long startedAtMillis = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", startedAtMillis);
        ShardWarmVolumes volumes = newVolumes(false);
        volumes.put("source", new ShardWarmVolumes.Entry(startedAtMillis, Map.of(shardId, 99L)));

        assertFalse(volumes.claimFetch(state, "source"));
        assertThat(volumes.get(state, "source"), nullValue());
    }

    public void testRemovedNodesCleanup() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState withSource = drainState(index, "source", "target", startedAtMillis);
        ClusterState withoutSource = ClusterState.builder(withSource)
            .nodes(DiscoveryNodes.builder(withSource.nodes()).remove("source"))
            .build();
        ShardWarmVolumes volumes = newVolumes();
        assertTrue(volumes.claimFetch(withSource, "source"));
        volumes.put("source", new ShardWarmVolumes.Entry(startedAtMillis, Map.of(new ShardId(index, 0), 10L)));
        volumes.clusterChanged(new ClusterChangedEvent("test", withoutSource, withSource));
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));
    }

    public void testAbsentNodeSweptWhenNodesDidNotChange() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState withSource = drainState(index, "source", "target", startedAtMillis);
        ClusterState withoutSource = ClusterState.builder(withSource)
            .nodes(DiscoveryNodes.builder(withSource.nodes()).remove("source"))
            .build();
        ClusterState later = ClusterState.builder(withoutSource).incrementVersion().build();
        ShardWarmVolumes volumes = newVolumes();
        volumes.put("source", new ShardWarmVolumes.Entry(startedAtMillis, Map.of(new ShardId(index, 0), 10L)));
        volumes.clusterChanged(new ClusterChangedEvent("test", later, withoutSource));
        assertThat(volumes.peek("source"), nullValue());
    }

    public void testCancelledShutdownClearsMemoAndInFlight() {
        Index index = new Index("idx", randomUUID());
        long startedAtMillis = randomNonNegativeLong();
        ClusterState drain = drainState(index, "source", "target", startedAtMillis);
        ClusterState cancelled = ClusterState.builder(drain)
            .metadata(Metadata.builder(drain.metadata()).removeCustom(NodesShutdownMetadata.TYPE))
            .build();
        ShardWarmVolumes volumes = newVolumes();
        assertTrue(volumes.claimFetch(drain, "source"));
        volumes.put("source", new ShardWarmVolumes.Entry(startedAtMillis, Map.of(new ShardId(index, 0), 10L)));

        volumes.clusterChanged(new ClusterChangedEvent("test", cancelled, drain));
        assertThat(volumes.peek("source"), nullValue());
        assertFalse(volumes.isInFlight("source"));
    }

    public void testNewShutdownGenerationEvictsStaleMemo() {
        Index index = new Index("idx", randomUUID());
        long firstGen = randomLongBetween(1, 1000);
        long secondGen = firstGen + randomLongBetween(1, 1000);
        ClusterState first = drainState(index, "source", "target", firstGen);
        ClusterState second = drainState(index, "source", "target", secondGen);
        ShardWarmVolumes volumes = newVolumes();
        volumes.put("source", new ShardWarmVolumes.Entry(firstGen, Map.of(new ShardId(index, 0), 10L)));

        volumes.clusterChanged(new ClusterChangedEvent("test", second, first));
        assertThat(volumes.peek("source"), nullValue());
        assertTrue(volumes.claimFetch(second, "source"));
    }

    private static ShardWarmVolumes newVolumes() {
        return newVolumes(true);
    }

    private static ShardWarmVolumes newVolumes(boolean enabled) {
        return new ShardWarmVolumes(
            new ClusterSettings(
                Settings.builder()
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING.getKey(), enabled)
                    .build(),
                Set.of(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING)
            )
        );
    }

    private static ClusterState drainState(Index index, String sourceNodeId, String targetNodeId, long startedAtMillis) {
        return drainState(index, Map.of(sourceNodeId, startedAtMillis), targetNodeId, TransportVersion.current());
    }

    private static ClusterState drainState(Index index, Map<String, Long> shutdowns, String targetNodeId, TransportVersion minVersion) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 1))
            .build();
        Map<String, SingleNodeShutdownMetadata> shutdownMetadata = new HashMap<>();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(targetNodeId))
            .localNodeId(targetNodeId)
            .masterNodeId(targetNodeId);
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.putCompatibilityVersions(targetNodeId, minVersion, Map.of());
        for (var entry : shutdowns.entrySet()) {
            shutdownMetadata.put(
                entry.getKey(),
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(entry.getKey())
                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                    .setReason("test")
                    .setStartedAtMillis(entry.getValue())
                    .setNodeSeen(true)
                    .build()
            );
            nodes.add(DiscoveryNodeUtils.create(entry.getKey()));
            state.putCompatibilityVersions(entry.getKey(), minVersion, Map.of());
        }
        return state.nodes(nodes.build())
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .build();
    }
}
