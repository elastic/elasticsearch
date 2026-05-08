/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting.Role;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Single;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.routing.ShardRouting.Role.INDEX_ONLY;
import static org.elasticsearch.cluster.routing.ShardRouting.Role.SEARCH_ONLY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ProjectIsolationAllocationDeciderTests extends ESAllocationTestCase {

    /**
     * For a project configured with index tier isolation, {@code canAllocate} returns yes only when the node's
     * {@code isolated_tier} attribute matches the configured pool name.
     */
    public void testIsolatedProjectRequiresMatchingNodeAttribute() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider projectIsolationAllocationDecider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);

        assertThat(
            projectIsolationAllocationDecider.canAllocate(
                indexOnlyShardRouting,
                clusterState.getRoutingNodes().node("n-iso"),
                routingAllocation
            ).type(),
            equalTo(Type.YES)
        );
        assertThat(
            projectIsolationAllocationDecider.canAllocate(
                indexOnlyShardRouting,
                clusterState.getRoutingNodes().node("n-shared"),
                routingAllocation
            ).type(),
            equalTo(Type.NO)
        );
        assertThat(
            projectIsolationAllocationDecider.canAllocate(
                indexOnlyShardRouting,
                clusterState.getRoutingNodes().node("n-other"),
                routingAllocation
            ).type(),
            equalTo(Type.NO)
        );
    }

    /**
     * For a project configured with search tier isolation, a {@link Role#SEARCH_ONLY} shard is allowed on a node only when that node's
     * {@code isolated_tier} attribute matches the pool, using the same matching idea as the index tier.
     */
    public void testIsolatedProjectSearchTierRequiresMatchingNodeAttribute() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "search", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting searchOnlyShard = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, SEARCH_ONLY);

        assertThat(
            decider.canAllocate(searchOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation).type(),
            equalTo(Type.YES)
        );
        assertThat(
            decider.canAllocate(searchOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation).type(),
            equalTo(Type.NO)
        );
        assertThat(
            decider.canAllocate(searchOnlyShard, clusterState.getRoutingNodes().node("n-other"), routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    /**
     * If isolation is not configured for the cluster, the decider rejects placing the shard on any node that still carries an
     * {@code isolated_tier} attribute.
     */
    public void testNonIsolatedProjectRejectedOnIsolatedNodes() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider projectIsolationAllocationDecider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);

        RoutingNode isolatedIndexingRoutingNode = clusterState.getRoutingNodes().node("n-iso");
        assertThat(
            projectIsolationAllocationDecider.canAllocate(indexOnlyShardRouting, isolatedIndexingRoutingNode, routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    /**
     * If isolation is not configured for the cluster, a shard on a shared node without an {@code isolated_tier} attribute gets
     * {@link Decision#ALWAYS} so other allocation deciders can apply.
     */
    public void testNonIsolatedProjectAllowedOnSharedNode() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);
        RoutingNode sharedNode = clusterState.getRoutingNodes().node("n-shared");
        Decision decision = decider.canAllocate(indexOnlyShardRouting, sharedNode, routingAllocation);
        assertThat(decision.type(), equalTo(Type.YES));
        assertThat(decision, sameInstance(Decision.ALWAYS));
    }

    /**
     * A shard with {@link Role#DEFAULT} does not participate in project isolation. This decider always defers, even when the owning
     * project is configured as isolated for index tiers.
     */
    public void testNonStatefulShardRolesAlwaysDefer() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, isolationTestProjectId, testIndexName);
        ShardId shardId = new ShardId(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID(), 0);
        ShardRouting defaultRoleShard = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            Role.DEFAULT
        );
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        RoutingNode mismatchedIsolationNode = clusterState.getRoutingNodes().node("n-other");

        Decision decision = decider.canAllocate(defaultRoleShard, mismatchedIsolationNode, routingAllocation);
        assertThat(decision, sameInstance(Decision.ALWAYS));
    }

    /**
     * For {@link Role#INDEX_ONLY} shards under index tier isolation, {@code canRemain} agrees with {@code canAllocate} on isolated nodes
     * versus shared nodes.
     */
    public void testCanRemainMatchesCanAllocateIndexTier() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, isolationTestProjectId, testIndexName);

        assertThat(
            decider.canRemain(indexMetadata, indexOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation).type(),
            equalTo(Type.YES)
        );
        assertThat(
            decider.canRemain(indexMetadata, indexOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    /**
     * For {@link Role#SEARCH_ONLY} shards under search tier isolation, {@code canRemain} agrees with {@code canAllocate} on isolated
     * nodes versus shared nodes.
     */
    public void testCanRemainMatchesCanAllocateSearchTier() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "search", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting searchOnlyShard = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, SEARCH_ONLY);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, isolationTestProjectId, testIndexName);

        assertThat(
            decider.canRemain(indexMetadata, searchOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation).type(),
            equalTo(Type.YES)
        );
        assertThat(
            decider.canRemain(indexMetadata, searchOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    /**
     * With allocation debug enabled and a tier mismatch, the decider returns {@link Decision.Type#NO} as a {@link Decision.Single} whose
     * explanation names the project, tier, and pool values involved.
     */
    public void testDebugDecisionExplainsIsolationTierMismatch() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(isolationTestProjectId, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).mutable();
        routingAllocation.debugDecision(true);
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);

        Single decision = (Single) decider.canAllocate(indexOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation);
        assertThat(decision.type(), equalTo(Type.NO));
        String explanation = decision.getExplanation();
        assertThat(explanation, containsString("iso-pool"));
        assertThat(explanation, containsString(isolationTestProjectId.id()));
        assertThat(explanation, containsString("index"));
    }

    /**
     * With allocation debug enabled, a non-isolated shard proposed on an isolated-capable node gets {@link Decision.Type#NO} with an
     * explanation that calls out the non-isolated case and the node's pool name.
     */
    public void testDebugDecisionExplainsNonIsolatedShardOnIsolationNode() {
        ProjectId isolationTestProjectId = randomIsolationTestProjectId();
        String testIndexName = randomTestIndexName();
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(isolationTestProjectId, testIndexName);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).mutable();
        routingAllocation.debugDecision(true);
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, isolationTestProjectId, testIndexName, INDEX_ONLY);

        Single decision = (Single) decider.canAllocate(indexOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation);
        assertThat(decision.type(), equalTo(Type.NO));
        assertThat(decision.getExplanation(), containsString("non-isolated"));
        assertThat(decision.getExplanation(), containsString("iso-pool"));
    }

    private static IndexMetadata routingIndexMetadata(ClusterState clusterState, ProjectId projectId, String indexName) {
        return clusterState.metadata().getProject(projectId).index(indexName);
    }

    private static ShardRouting findShardRoutingByRole(ClusterState clusterState, ProjectId projectId, String indexName, Role role) {
        IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable(projectId).index(indexName).shard(0);
        for (int replicaIndex = 0; replicaIndex < indexShardRoutingTable.size(); replicaIndex++) {
            ShardRouting shardRouting = indexShardRoutingTable.shard(replicaIndex);
            if (shardRouting.role() == role) {
                return shardRouting;
            }
        }
        throw new AssertionError("no shard with role " + role);
    }

    /** JSON body for {@link ProjectIsolationAllocationDecider#CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING} with one tier key. */
    private static String isolatedProjectTierJson(ProjectId projectId, String tierJsonKey, String poolName) {
        return "[{\"project\":\"" + projectId.id() + "\",\"tiers\":{\"" + tierJsonKey + "\":\"" + poolName + "\"}}]";
    }

    /**
     * Cluster with one stateless index and three index nodes: shared, isolated matching "iso-pool", isolated "other-pool".
     */
    private static ClusterState newClusterStateForIsolationFixture(ProjectId projectId, String indexName) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Set<DiscoveryNodeRole> indexingNodeRoles = Set.of(INDEX_ROLE);

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.builder("n-shared").roles(indexingNodeRoles).build())
                    .add(
                        DiscoveryNodeUtils.builder("n-iso")
                            .roles(indexingNodeRoles)
                            .attributes(Map.of(ProjectIsolationAllocationDecider.NODE_ATTRIBUTE_ISOLATED_TIER, "iso-pool"))
                            .build()
                    )
                    .add(
                        DiscoveryNodeUtils.builder("n-other")
                            .roles(indexingNodeRoles)
                            .attributes(Map.of(ProjectIsolationAllocationDecider.NODE_ATTRIBUTE_ISOLATED_TIER, "other-pool"))
                            .build()
                    )
                    .build()
            )
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .routingTable(projectId, RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata).build())
            .build();
    }

    private static ProjectIsolationAllocationDecider newProjectIsolationAllocationDecider(Settings nodeSettings) {
        Set<Setting<?>> clusterSettingsRegistrations = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsRegistrations.add(ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, clusterSettingsRegistrations);
        return new ProjectIsolationAllocationDecider(clusterSettings);
    }

    /** Random lowercase index name valid for allocation tests. */
    private String randomTestIndexName() {
        return randomIdentifier("test-i-");
    }

    /** Random id valid for {@link ProjectId#fromId(String)} (not {@link ProjectId#DEFAULT}). */
    private ProjectId randomIsolationTestProjectId() {
        String id = randomValueOtherThan(ProjectId.DEFAULT.id(), () -> randomAlphanumericOfLength(randomIntBetween(12, 32)));
        return ProjectId.fromId(id);
    }
}
