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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SnapshotMetricsServiceTests extends ESTestCase {

    public void testMetricsAreOnlyCalculatedAfterAValidClusterStateHasBeenSeen() {
        final ClusterService clusterService = mock(ClusterService.class);
        final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        final SnapshotMetrics snapshotMetrics = new SnapshotMetrics(recordingMeterRegistry);
        final SnapshotMetricsService snapshotsService = new SnapshotMetricsService(snapshotMetrics, clusterService);

        // No metrics should be recorded before seeing a cluster state
        recordingMeterRegistry.getRecorder().collect();
        assertThat(
            recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE),
            empty()
        );
        assertThat(
            recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, SnapshotMetrics.SNAPSHOTS_BY_STATE),
            empty()
        );
        verifyNoInteractions(clusterService);

        // Simulate a cluster state being applied
        final ClusterState withSnapshotsInProgress = createClusterStateWithSnapshotsInProgress();
        when(clusterService.state()).thenReturn(withSnapshotsInProgress);
        snapshotsService.clusterChanged(new ClusterChangedEvent("test", withSnapshotsInProgress, withSnapshotsInProgress));

        // This time we should publish some metrics
        recordingMeterRegistry.getRecorder().collect();
        assertThat(
            recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE),
            not(empty())
        );
        assertThat(
            recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, SnapshotMetrics.SNAPSHOTS_BY_STATE),
            not(empty())
        );
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
