/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.IndexShardCountConstraintSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexShardCountAllocationDeciderIT extends ESIntegTestCase {

    public void testIndexShardCountExceedsAverageAllocation() {
        var testHarness = setUpThreeHealthyDataNodesAndVerifyIndexShardsBalancedDistributed();

        // Exclude assignment of shards to the first data nodes via the {@link FilterAllocationDecider} settings.
        // This triggers the balancer to work out a new routing.
        logger.info("---> Remove shard assignments of node " + testHarness.firstDataNodeName + " by excluding first data node.");
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", testHarness.firstDataNodeName));

        refreshClusterInfo();

        int lowerThreshold = testHarness.randomNumberOfShards / 2;
        int upperThreshold = (int) Math.ceil((double) testHarness.randomNumberOfShards / 2);

        var verifyShardCountBalanceListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            var indexRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(testHarness.indexName);
            if (indexRoutingTable == null) {
                return false;
            }
            if (indexRoutingTable.numberOfNodesShardsAreAllocatedOn() != 2) {
                return false;
            }

            Index index = indexRoutingTable.getIndex();
            assertThat(indexRoutingTable.numberOfNodesShardsAreAllocatedOn(), equalTo(2));
            clusterState.getRoutingNodes().stream().forEach(node -> {
                if (node.hasIndex(index)) {
                    assert node.numberOfOwningShardsForIndex(index) >= lowerThreshold;
                    assert node.numberOfOwningShardsForIndex(index) <= upperThreshold;
                }
            });
            return true;
        });

        safeAwait(verifyShardCountBalanceListener);
    }

    private boolean checkShardAssignment(
        RoutingNodes routingNodes,
        Index index,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        int upperLimitFirstDataNode,
        int lowerLimitFirstDataNode,
        int upperLimitSecondDataNode,
        int lowerLimitSecondDataNode,
        int upperLimitThirdDataNode,
        int lowerLimitThirdDataNode
    ) {

        int firstDataNodeRealNumberOfShards = routingNodes.node(firstDataNodeId).numberOfOwningShardsForIndex(index);
        int secondDataNodeRealNumberOfShards = routingNodes.node(secondDataNodeId).numberOfOwningShardsForIndex(index);
        int thirdDataNodeRealNumberOfShards = routingNodes.node(thirdDataNodeId).numberOfOwningShardsForIndex(index);

        return firstDataNodeRealNumberOfShards <= upperLimitFirstDataNode
            && firstDataNodeRealNumberOfShards >= lowerLimitFirstDataNode
            && secondDataNodeRealNumberOfShards <= upperLimitSecondDataNode
            && secondDataNodeRealNumberOfShards >= lowerLimitSecondDataNode
            && thirdDataNodeRealNumberOfShards <= upperLimitThirdDataNode
            && thirdDataNodeRealNumberOfShards >= lowerLimitThirdDataNode;
    }

    private TestHarness setUpThreeHealthyDataNodesAndVerifyIndexShardsBalancedDistributed() {
        Settings settings = Settings.builder()
            .put(
                IndexShardCountConstraintSettings.INDEX_SHARD_COUNT_DECIDER_ENABLED_SETTING.getKey(),
                IndexShardCountConstraintSettings.IndexShardCountDeciderStatus.ENABLED
            )
            .put(IndexShardCountConstraintSettings.INDEX_SHARD_COUNT_DECIDER_LOAD_SKEW_TOLERANCE.getKey(), 1.0d)
            .build();
        internalCluster().startMasterOnlyNode(settings);

        final var dataNodes = internalCluster().startDataOnlyNodes(3, settings);

        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

        final DiscoveryNode firstDiscoveryNode = internalCluster().getInstance(TransportService.class, firstDataNodeName).getLocalNode();
        final DiscoveryNode secondDiscoveryNode = internalCluster().getInstance(TransportService.class, secondDataNodeName).getLocalNode();
        final DiscoveryNode thirdDiscoveryNode = internalCluster().getInstance(TransportService.class, thirdDataNodeName).getLocalNode();

        String format = """
              ---> first node NAME %s and ID %s; second node NAME %s and ID %s; third node NAME %s and ID %s;
            """;
        logger.info(
            Strings.format(
                format,
                firstDataNodeName,
                firstDataNodeId,
                secondDataNodeName,
                secondDataNodeId,
                thirdDataNodeName,
                thirdDataNodeId
            )
        );

        int randomNumberOfShards = randomIntBetween(15, 20);
        String indexName = randomIdentifier();
        int lowerThreshold = randomNumberOfShards / 3;
        int upperThreshold = (int) Math.ceil((double) randomNumberOfShards / 3);

        var verifyShardAllocationListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            var indexRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName);
            if (indexRoutingTable == null) {
                return false;
            }
            return checkShardAssignment(
                clusterState.getRoutingNodes(),
                indexRoutingTable.getIndex(),
                firstDataNodeId,
                secondDataNodeId,
                thirdDataNodeId,
                upperThreshold,
                lowerThreshold,
                upperThreshold,
                lowerThreshold,
                upperThreshold,
                lowerThreshold
            );
        });

        createIndex(
            indexName,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);
        logger.info("---> wait for  [" + randomNumberOfShards + "] shards to be assigned to node ");

        safeAwait(verifyShardAllocationListener);
        return new TestHarness(
            firstDataNodeName,
            secondDataNodeName,
            thirdDataNodeName,
            firstDataNodeId,
            secondDataNodeId,
            thirdDataNodeName,
            firstDiscoveryNode,
            secondDiscoveryNode,
            thirdDiscoveryNode,
            indexName,
            randomNumberOfShards
        );
    }

    record TestHarness(
        String firstDataNodeName,
        String secondDataNodeName,
        String thirdDataNodeName,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        DiscoveryNode firstDiscoveryNode,
        DiscoveryNode secondDiscoveryNode,
        DiscoveryNode thirdDiscoveryNode,
        String indexName,
        int randomNumberOfShards
    ) {}
}
