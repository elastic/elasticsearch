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

    private static final ProjectId ISOLATION_TEST_PROJECT_ID = ProjectId.fromId("projisolatedaaaa");

    private static final String ALLOC_TEST_INDEX_NAME = "alloc-test-idx";

    public void testIsolatedProjectRequiresMatchingNodeAttribute() {
        ProjectIsolationAllocationDecider projectIsolationAllocationDecider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(
            clusterState,
            ISOLATION_TEST_PROJECT_ID,
            ALLOC_TEST_INDEX_NAME,
            INDEX_ONLY
        );

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

    public void testIsolatedProjectSearchTierRequiresMatchingNodeAttribute() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "search", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting searchOnlyShard = findShardRoutingByRole(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME, SEARCH_ONLY);

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

    public void testNonIsolatedProjectRejectedOnIsolatedNodes() {
        ProjectIsolationAllocationDecider projectIsolationAllocationDecider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(
            clusterState,
            ISOLATION_TEST_PROJECT_ID,
            ALLOC_TEST_INDEX_NAME,
            INDEX_ONLY
        );

        RoutingNode isolatedIndexingRoutingNode = clusterState.getRoutingNodes().node("n-iso");
        assertThat(
            projectIsolationAllocationDecider.canAllocate(indexOnlyShardRouting, isolatedIndexingRoutingNode, routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    public void testNonIsolatedProjectAllowedOnSharedNode() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting indexOnlyShardRouting = findShardRoutingByRole(
            clusterState,
            ISOLATION_TEST_PROJECT_ID,
            ALLOC_TEST_INDEX_NAME,
            INDEX_ONLY
        );
        RoutingNode sharedNode = clusterState.getRoutingNodes().node("n-shared");
        Decision decision = decider.canAllocate(indexOnlyShardRouting, sharedNode, routingAllocation);
        assertThat(decision.type(), equalTo(Type.YES));
        assertThat(decision, sameInstance(Decision.ALWAYS));
    }

    public void testNonStatefulShardRolesAlwaysDefer() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
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

    public void testCanRemainMatchesCanAllocateIndexTier() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME, INDEX_ONLY);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);

        assertThat(
            decider.canRemain(indexMetadata, indexOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation).type(),
            equalTo(Type.YES)
        );
        assertThat(
            decider.canRemain(indexMetadata, indexOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    public void testCanRemainMatchesCanAllocateSearchTier() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "search", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).immutable();
        ShardRouting searchOnlyShard = findShardRoutingByRole(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME, SEARCH_ONLY);
        IndexMetadata indexMetadata = routingIndexMetadata(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);

        assertThat(
            decider.canRemain(indexMetadata, searchOnlyShard, clusterState.getRoutingNodes().node("n-iso"), routingAllocation).type(),
            equalTo(Type.YES)
        );
        assertThat(
            decider.canRemain(indexMetadata, searchOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation).type(),
            equalTo(Type.NO)
        );
    }

    public void testDebugDecisionExplainsIsolationTierMismatch() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(
            Settings.builder()
                .put(
                    ProjectIsolationAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ISOLATED_PROJECTS_SETTING.getKey(),
                    isolatedProjectTierJson(ISOLATION_TEST_PROJECT_ID, "index", "iso-pool")
                )
                .build()
        );
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).mutable();
        routingAllocation.debugDecision(true);
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME, INDEX_ONLY);

        Single decision = (Single) decider.canAllocate(indexOnlyShard, clusterState.getRoutingNodes().node("n-shared"), routingAllocation);
        assertThat(decision.type(), equalTo(Type.NO));
        String explanation = decision.getExplanation();
        assertThat(explanation, containsString("iso-pool"));
        assertThat(explanation, containsString(ISOLATION_TEST_PROJECT_ID.id()));
        assertThat(explanation, containsString("index"));
    }

    public void testDebugDecisionExplainsNonIsolatedShardOnIsolationNode() {
        ProjectIsolationAllocationDecider decider = newProjectIsolationAllocationDecider(Settings.EMPTY);
        ClusterState clusterState = newClusterStateForIsolationFixture(ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME);
        RoutingAllocation routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).mutable();
        routingAllocation.debugDecision(true);
        ShardRouting indexOnlyShard = findShardRoutingByRole(clusterState, ISOLATION_TEST_PROJECT_ID, ALLOC_TEST_INDEX_NAME, INDEX_ONLY);

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
}
