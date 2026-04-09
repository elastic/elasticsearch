/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

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
