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
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachingSnapshotAndShardByStateMetricsServiceTests extends ESTestCase {

    public void testMetricsAreOnlyCalculatedAfterAValidClusterStateHasBeenSeen() {
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
        final ClusterState withSnapshotsInProgress = createClusterStateWithSnapshotsInProgress();
        when(clusterService.state()).thenReturn(withSnapshotsInProgress);

        // This time we should publish some metrics
        assertThat(byStateMetricsService.getShardsByState(), not(empty()));
        assertThat(byStateMetricsService.getSnapshotsByState(), not(empty()));
        verify(clusterService, times(2)).state();

        reset(clusterService);

        // Then publish a new state in which we aren't master
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
            .build();
        when(clusterService.state()).thenReturn(noLongerMaster);

        // We should no longer publish metrics
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());
        verify(clusterService, never()).state();
    }

    private ClusterState createClusterStateWithSnapshotsInProgress() {
        final var indexName = randomIdentifier();
        final var repositoryName = randomIdentifier();
        final ClusterState state = ClusterStateCreationUtils.state(indexName, randomIntBetween(1, 3), randomIntBetween(1, 2));
        final IndexMetadata index = state.projectState(ProjectId.DEFAULT).metadata().index(indexName);
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(state.nodes().getLocalNodeId()))
            .putProjectMetadata(
                ProjectMetadata.builder(state.getMetadata().getProject(ProjectId.DEFAULT))
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(List.of(new RepositoryMetadata(repositoryName, "fs", Settings.EMPTY)))
                    )
            )
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(createEntry(index, repositoryName)))
            .incrementVersion()
            .build();
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
