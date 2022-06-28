/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardFollowTasksExecutorAssignmentTests extends ESTestCase {

    public void testAssignmentToNodeWithDataAndRemoteClusterClientRoles() {
        runAssignmentTest(
            newNode(
                Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE),
                VersionUtils.randomVersion(random())
            ),
            newNodes(
                between(0, 8),
                () -> Sets.newHashSet(randomSubsetOf(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE))),
                Version.CURRENT
            ),
            (theSpecial, assignment) -> {
                assertTrue(assignment.isAssigned());
                assertThat(assignment.getExecutorNode(), equalTo(theSpecial.getId()));
            }
        );
    }

    public void testDataRoleWithoutRemoteClusterServiceRole() {
        runNoAssignmentTest(Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    }

    public void testRemoteClusterClientRoleWithoutDataRole() {
        runNoAssignmentTest(Collections.singleton(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public void testNodeWithLegacyRolesOnly() {
        final Version oldVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_6_0_0,
            VersionUtils.getPreviousVersion(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE_VERSION)
        );
        runAssignmentTest(
            newNode(Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE), oldVersion),
            newNodes(
                between(0, 8),
                () -> Sets.newHashSet(randomSubsetOf(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE))),
                Version.CURRENT
            ),
            (theSpecial, assignment) -> {
                assertTrue(assignment.isAssigned());
                assertThat(assignment.getExecutorNode(), equalTo(theSpecial.getId()));
            }
        );
    }

    private void runNoAssignmentTest(final Set<DiscoveryNodeRole> roles) {
        runAssignmentTest(newNode(roles, Version.CURRENT), Collections.emptySet(), (theSpecial, assignment) -> {
            assertFalse(assignment.isAssigned());
            assertThat(assignment.getExplanation(), equalTo("no nodes found with data and remote cluster client roles"));
        });
    }

    private void runAssignmentTest(
        final DiscoveryNode targetNode,
        final Set<DiscoveryNode> otherNodes,
        final BiConsumer<DiscoveryNode, Assignment> consumer
    ) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Collections.singleton(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT))
        );
        final SettingsModule settingsModule = mock(SettingsModule.class);
        when(settingsModule.getSettings()).thenReturn(Settings.EMPTY);
        final ShardFollowTasksExecutor executor = new ShardFollowTasksExecutor(
            mock(Client.class),
            mock(ThreadPool.class),
            clusterService,
            settingsModule
        );
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("test"));
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(targetNode);
        for (DiscoveryNode node : otherNodes) {
            nodesBuilder.add(node);
        }
        clusterStateBuilder.nodes(nodesBuilder);
        final Assignment assignment = executor.getAssignment(
            mock(ShardFollowTask.class),
            clusterStateBuilder.nodes().getAllNodes(),
            clusterStateBuilder.build()
        );
        consumer.accept(targetNode, assignment);
    }

    private static DiscoveryNode newNode(final Set<DiscoveryNodeRole> roles, final Version version) {
        return new DiscoveryNode(
            "node_" + UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            roles,
            version
        );
    }

    private static Set<DiscoveryNode> newNodes(int numberOfNodes, Supplier<Set<DiscoveryNodeRole>> rolesSupplier, Version version) {
        Set<DiscoveryNode> nodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode(rolesSupplier.get(), version));
        }
        return nodes;
    }
}
