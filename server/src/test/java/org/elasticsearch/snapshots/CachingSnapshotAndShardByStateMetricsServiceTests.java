/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachingSnapshotAndShardByStateMetricsServiceTests extends ESTestCase {

    public void testMetricsAreOnlyCalculatedWhileClusterServiceIsStartedAndLocalNodeIsMaster() {
        final var indexName = randomIdentifier();
        final var repositoryName = randomIdentifier();
        final ClusterService clusterService = mock(ClusterService.class);
        final CachingSnapshotAndShardByStateMetricsService byStateMetricsService = new CachingSnapshotAndShardByStateMetricsService(
            clusterService
        );

        // No metrics should be recorded before the cluster service is started
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());
        verify(clusterService, never()).state();

        // Simulate the metrics service being started/a state being applied
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        final ClusterState withSnapshotsInProgress = createClusterStateWithSnapshotsInProgress(indexName, repositoryName);
        when(clusterService.state()).thenReturn(withSnapshotsInProgress);

        // This time we should publish some metrics
        final Collection<LongWithAttributes> shardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> snapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(shardsByState, not(empty()));
        assertThat(snapshotsByState, not(empty()));
        verify(clusterService, times(2)).state();

        reset(clusterService);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        // Then observe a new state in which we aren't master
        final ClusterState noLongerMaster = ClusterState.builder(withSnapshotsInProgress)
            .nodes(
                DiscoveryNodes.builder(withSnapshotsInProgress.nodes())
                    .masterNodeId(
                        randomValueOtherThan(
                            withSnapshotsInProgress.nodes().getLocalNodeId(),
                            () -> randomFrom(withSnapshotsInProgress.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()))
                        )
                    )
            )
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(noLongerMaster);

        // We should no longer publish metrics
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());

        // Become master again
        final ClusterState masterAgain = ClusterState.builder(noLongerMaster)
            .nodes(DiscoveryNodes.builder(noLongerMaster.nodes()).masterNodeId(noLongerMaster.nodes().getLocalNodeId()))
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(masterAgain);

        // We should return cached metrics because the SnapshotsInProgress hasn't changed
        final Collection<LongWithAttributes> secondShardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> secondSnapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(secondShardsByState, sameInstance(shardsByState));
        assertThat(secondSnapshotsByState, sameInstance(snapshotsByState));

        // Update SnapshotsInProgress
        final ClusterState newSnapshotsInProgress = ClusterState.builder(masterAgain)
            .putCustom(SnapshotsInProgress.TYPE, createSnapshotsInProgress(masterAgain, indexName, repositoryName))
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(newSnapshotsInProgress);

        // We should return fresh metrics because the SnapshotsInProgress has changed
        final Collection<LongWithAttributes> thirdShardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> thirdSnapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(thirdShardsByState, not(empty()));
        assertThat(thirdSnapshotsByState, not(empty()));
        assertThat(thirdShardsByState, not(sameInstance(shardsByState)));
        assertThat(thirdSnapshotsByState, not(sameInstance(snapshotsByState)));

        // Then the cluster service is stopped, we should no longer publish metrics
        reset(clusterService);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STOPPED);
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());
        verify(clusterService, never()).state();
    }

    private ClusterState createClusterStateWithSnapshotsInProgress(String indexName, String repositoryName) {
        // Need to have at least 2 nodes, so we can test when another node is the master
        final ClusterState state = ClusterStateCreationUtils.state(indexName, randomIntBetween(2, 5), randomIntBetween(1, 2));
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(state.nodes().getLocalNodeId()))
            .putProjectMetadata(
                ProjectMetadata.builder(state.getMetadata().getProject(ProjectId.DEFAULT))
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(List.of(new RepositoryMetadata(repositoryName, "fs", Settings.EMPTY)))
                    )
            )
            .putCustom(SnapshotsInProgress.TYPE, createSnapshotsInProgress(state, indexName, repositoryName))
            .incrementVersion()
            .build();
    }

    private SnapshotsInProgress createSnapshotsInProgress(ClusterState clusterState, String indexName, String repositoryName) {
        final IndexMetadata index = clusterState.projectState(ProjectId.DEFAULT).metadata().index(indexName);
        return SnapshotsInProgress.EMPTY.withAddedEntry(createEntry(index, repositoryName));
    }

    private SnapshotsInProgress.Entry createEntry(IndexMetadata indexMetadata, String repositoryName) {
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(ProjectId.DEFAULT, repositoryName, new SnapshotId("", "")),
            false,
            randomBoolean(),
            SnapshotsInProgress.State.STARTED,
            Map.of(indexMetadata.getIndex().getName(), new IndexId(indexMetadata.getIndex().getName(), randomIdentifier())),
            List.of(),
            Collections.emptyList(),
            0,
            1,
            IntStream.range(0, indexMetadata.getNumberOfShards())
                .mapToObj(i -> new ShardId(indexMetadata.getIndex(), i))
                .collect(
                    Collectors.toUnmodifiableMap(
                        Function.identity(),
                        shardId -> new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), ShardGeneration.newGeneration())
                    )
                ),
            null,
            null,
            null
        );
    }
}
