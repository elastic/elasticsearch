/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus.Stage;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotShardsServiceTests extends ESTestCase {

    /**
     * Stateful nodes handle both indexing and search requests
     * If the node has at least one role that contains data, then we expect the {@code SnapshotShardsService} to be activated.
     */
    public void testShouldActivateSnapshotShardsServiceWithStatefulNodeAndAtLeastOneDataRole() {
        // Check that any node exclusively with a role that contains data has the SnapshotShardsService activated
        List<String> nodeRolesThatContainData = DiscoveryNodeRole.roles()
            .stream()
            .filter(DiscoveryNodeRole::canContainData)
            .map(DiscoveryNodeRole::roleName)
            .toList();
        for (String role : nodeRolesThatContainData) {
            Settings settings = Settings.builder().put("node.roles", role).put("stateless.enabled", false).build();
            assertTrue(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));
        }

        // Compute a random combination of all roles, with a minimum of one role that contains data, and expect the SnapshotShardsService to
        // be activated.
        // NB The VOTING_ONLY_NODE_ROLE also requires MASTER_ROLE to be set which we can't guarantee so we remove it
        List<String> nodeRolesThatDoNotContainData = DiscoveryNodeRole.roles()
            .stream()
            .filter(
                discoveryNodeRole -> discoveryNodeRole.canContainData() == false
                    && discoveryNodeRole != DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE
            )
            .map(DiscoveryNodeRole::roleName)
            .toList();
        List<String> nodeRoles = randomNonEmptySubsetOf(nodeRolesThatContainData);
        nodeRoles.addAll(randomSubsetOf(nodeRolesThatDoNotContainData));
        String nodeRolesString = String.join(",", nodeRoles);

        Settings settings = Settings.builder().put("node.roles", nodeRolesString).put("stateless.enabled", false).build();
        assertTrue(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));
    }

    /**
     * Stateful nodes handle both indexing and search requests.
     * If the stateful node has no roles that contains data, then we expect the {@code SnapshotShardsService} to not be activated.
     */
    public void testShouldActivateSnapshotShardsServiceWithStatefulNodeAndNoDataRoles() {
        // Specifically test the DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE role
        Settings settings = Settings.builder().put("node.roles", "voting_only,master").put("stateless.enabled", false).build();
        assertFalse(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));

        List<String> nodeRolesThatDoNotContainData = DiscoveryNodeRole.roles()
            .stream()
            .filter(
                discoveryNodeRole -> discoveryNodeRole.canContainData() == false
                    && discoveryNodeRole != DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE
            )
            .map(DiscoveryNodeRole::roleName)
            .toList();
        String nodeRolesString = String.join(",", randomNonEmptySubsetOf(nodeRolesThatDoNotContainData));

        settings = Settings.builder().put("node.roles", nodeRolesString).put("stateless.enabled", false).build();
        assertFalse(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));
    }

    /**
     * Stateless indexing and search nodes are separated by tier
     * If this is an indexing node then we expect the {@code SnapshotShardsService} to be activated
     */
    public void testShouldActivateSnapshotShardsServiceWithStatelessIndexNode() {
        Settings settings = Settings.builder().put("node.roles", "index").put("stateless.enabled", true).build();
        assertTrue(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));
    }

    /**
     * Stateless indexing and search nodes are separated by tier
     * If this is a search node then we expect the {@code SnapshotShardsService} to <i>not</i> be activated,
     */
    public void testShouldActivateSnapshotShardsServiceWithStatelessSearchNode() {
        Settings settings = Settings.builder().put("node.roles", "search").put("stateless.enabled", true).build();
        assertFalse(SnapshotShardsService.shouldActivateSnapshotShardsService(settings));
    }

    public void testClusterChangedAbortsTrackedShardOnDeletedIndexWhenRelocationSupported() throws Exception {
        runClusterChangedAbortScenario(true, /*expectAborted*/ true);
    }

    public void testClusterChangedDoesNotAbortWhenRelocationNotSupported() throws Exception {
        runClusterChangedAbortScenario(false, /*expectAborted*/ false);
    }

    /**
     * Registers shard snapshots for two indices, deletes one, and verifies whether the deleted index's shard was
     * aborted based on {@code supportsRelocationDuringSnapshot}. The surviving index's shard must always remain as INIT.
     */
    private void runClusterChangedAbortScenario(boolean supportsRelocationDuringSnapshot, boolean expectAborted) throws Exception {
        final var localNode = DiscoveryNodeUtils.create("node", "node");
        try (
            var threadPool = new TestThreadPool(getTestName());
            var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode);
            var transportService = MockTransportService.createMockTransportService(new MockTransport(), threadPool)
        ) {
            final var factory = new SnapshotShardContextFactory() {
                @Override
                public SubscribableListener<SnapshotShardContext> asyncCreate(
                    ShardId shardId,
                    Snapshot snapshot,
                    IndexId indexId,
                    IndexShardSnapshotStatus snapshotStatus,
                    IndexVersion repositoryMetaVersion,
                    long snapshotStartTime,
                    ActionListener<ShardSnapshotResult> listener
                ) {
                    return new SubscribableListener<>();
                }

                @Override
                public boolean supportsRelocationDuringSnapshot() {
                    return supportsRelocationDuringSnapshot;
                }
            };
            final var repositoriesService = mock(RepositoriesService.class);
            when(repositoriesService.repository(any(), any())).thenReturn(mock(Repository.class));

            try (
                var service = new SnapshotShardsService(
                    Settings.EMPTY,
                    clusterService,
                    repositoriesService,
                    transportService,
                    mock(IndicesService.class),
                    factory
                )
            ) {
                service.start();
                final var projectId = ProjectId.DEFAULT;
                final var deletedShardId = new ShardId("deleted-idx", UUIDs.randomBase64UUID(), 0);
                final var survivingShardId = new ShardId("surviving-idx", UUIDs.randomBase64UUID(), 0);
                final var snapshot = new Snapshot(
                    projectId,
                    randomRepoName(),
                    new SnapshotId(randomSnapshotName(), UUIDs.randomBase64UUID())
                );

                final var initialState = clusterStateWithStartedEntry(
                    localNode,
                    projectId,
                    snapshot,
                    List.of(deletedShardId, survivingShardId)
                );
                ClusterServiceUtils.setState(clusterService, initialState);

                // After the cluster state is applied, both shard statuses are registered by startNewShardSnapshots.
                assertShardStage(service, snapshot, deletedShardId, Stage.INIT);
                assertShardStage(service, snapshot, survivingShardId, Stage.INIT);

                // Delete one of the two indices.
                final var stateWithIndexDeleted = withIndicesRemoved(initialState, projectId, List.of(deletedShardId.getIndexName()));
                ClusterServiceUtils.setState(clusterService, stateWithIndexDeleted);

                if (expectAborted) {
                    // Stage is ABORTED if the async snapshot task attached its listener before our abort fired, or
                    // FAILURE if it ran afterwards and ensureNotAborted caught the abort. Either way the status has
                    // moved out of INIT.
                    assertShardStage(service, snapshot, deletedShardId, Stage.ABORTED, Stage.FAILURE);
                } else {
                    assertShardStage(service, snapshot, deletedShardId, Stage.INIT);
                }
                // The surviving index's shard is never touched by the abort path regardless of the relocation flag.
                assertShardStage(service, snapshot, survivingShardId, Stage.INIT);

                // A subsequent beforeIndexShardClosed for the same shard is the same-node abort path. It must be a
                // safe no-op if the cluster-state-driven abort already fired (idempotency of
                // IndexShardSnapshotStatus#abortIfNotCompleted), and transition the status from INIT to the abort
                // end-state otherwise. Either way the shard ends in ABORTED or FAILURE.
                service.beforeIndexShardClosed(deletedShardId, null, Settings.EMPTY);
                assertShardStage(service, snapshot, deletedShardId, Stage.ABORTED, Stage.FAILURE);
            }
        }
    }

    private static void assertShardStage(SnapshotShardsService service, Snapshot snapshot, ShardId shardId, Stage... expectedStages) {
        assertThat(service.currentSnapshotShards(snapshot).get(shardId).getStage(), is(oneOf(expectedStages)));
    }

    /**
     * Builds a cluster state containing a single started {@link SnapshotsInProgress.Entry} covering the given shards
     * (all assigned to {@code localNode}). Each shard's index is added to the project metadata.
     */
    private static ClusterState clusterStateWithStartedEntry(
        DiscoveryNode localNode,
        ProjectId projectId,
        Snapshot snapshot,
        List<ShardId> shards
    ) {
        final var projectBuilder = ProjectMetadata.builder(projectId);
        for (var shardId : shards) {
            projectBuilder.put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .build(),
                false
            );
        }

        final var snapshotEntry = SnapshotsInProgress.startedEntry(
            snapshot,
            randomBoolean(),
            randomBoolean(),
            shards.stream().collect(Collectors.toMap(ShardId::getIndexName, s -> new IndexId(s.getIndexName(), s.getIndex().getUUID()))),
            List.of(),
            System.currentTimeMillis(),
            1L,
            shards.stream()
                .collect(
                    Collectors.toMap(
                        s1 -> s1,
                        s1 -> new SnapshotsInProgress.ShardSnapshotStatus(localNode.getId(), ShardGeneration.newGeneration(random()))
                    )
                ),
            Map.of(),
            IndexVersion.current(),
            List.of()
        );

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .putProjectMetadata(projectBuilder.build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(snapshotEntry))
            .build();
    }

    /**
     * Returns a new cluster state equal to {@code previous} but with the given index names removed from the given
     * project's metadata, which is what triggers {@link org.elasticsearch.cluster.ClusterChangedEvent#indicesDeleted()}.
     */
    private static ClusterState withIndicesRemoved(ClusterState previous, ProjectId projectId, List<String> indicesToRemove) {
        final var oldProject = previous.metadata().getProject(projectId);
        final var projectBuilder = ProjectMetadata.builder(oldProject);
        for (var name : indicesToRemove) {
            projectBuilder.remove(name);
        }
        return ClusterState.builder(previous).metadata(Metadata.builder(previous.metadata()).put(projectBuilder.build())).build();
    }

    public void testSummarizeFailure() {
        final RuntimeException wrapped = new RuntimeException("wrapped");
        assertThat(SnapshotShardsService.summarizeFailure(wrapped), is("RuntimeException[wrapped]"));
        final RuntimeException wrappedWithNested = new RuntimeException("wrapped", new IOException("nested"));
        assertThat(SnapshotShardsService.summarizeFailure(wrappedWithNested), is("RuntimeException[wrapped]; nested: IOException[nested]"));
        final RuntimeException wrappedWithTwoNested = new RuntimeException("wrapped", new IOException("nested", new IOException("root")));
        assertThat(
            SnapshotShardsService.summarizeFailure(wrappedWithTwoNested),
            is("RuntimeException[wrapped]; nested: IOException[nested]; nested: IOException[root]")
        );
    }

    public void testEqualsAndHashcodeUpdateIndexShardSnapshotStatusRequest() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new UpdateIndexShardSnapshotStatusRequest(
                new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))),
                new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5)),
                new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), ShardGeneration.newGeneration(random()))
            ),
            request -> new UpdateIndexShardSnapshotStatusRequest(request.snapshot(), request.shardId(), request.status()),
            request -> {
                final boolean mutateSnapshot = randomBoolean();
                final boolean mutateShardId = randomBoolean();
                final boolean mutateStatus = (mutateSnapshot || mutateShardId) == false || randomBoolean();
                return new UpdateIndexShardSnapshotStatusRequest(
                    mutateSnapshot
                        ? new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random())))
                        : request.snapshot(),
                    mutateShardId
                        ? new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5))
                        : request.shardId(),
                    mutateStatus
                        ? new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), ShardGeneration.newGeneration(random()))
                        : request.status()
                );
            }
        );
    }

}
