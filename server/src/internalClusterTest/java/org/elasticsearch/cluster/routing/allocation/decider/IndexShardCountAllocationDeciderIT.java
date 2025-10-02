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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexShardCountAllocationDeciderIT extends ESIntegTestCase {



    /*
        Happy path test scenario

        3 ingestion node

        5 shards index ,
                A       1
                B       2
                C       2

              Propose Allocation to move shard from B to C  -> Not preferred

        Enable this decider
        put very strict load skew tolerance basically ideal
        with this in place the end result should be all not exceeding ideal


        What happes

        So basically first


     */


    /**
     * Happy path -
     *
     *
     *
     *
     *
     */
    public void testIndexShardCountExceedsAverageAllocation() {

        setUpIndex();




    }


    private boolean checkShardAssignment(
        RoutingNodes routingNodes,
        Index index,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        int shards
    ) {

        int firstDataNodeRealNumberOfShards = routingNodes.node(firstDataNodeId).numberOfOwningShardsForIndex(index);
        int secondDataNodeRealNumberOfShards = routingNodes.node(secondDataNodeId).numberOfOwningShardsForIndex(index);
        int thirdDataNodeRealNumberOfShards = routingNodes.node(thirdDataNodeId).numberOfOwningShardsForIndex(index);

        return firstDataNodeRealNumberOfShards + secondDataNodeRealNumberOfShards + thirdDataNodeRealNumberOfShards == shards;
    }


    private TestHarness setUpIndex() {

        Settings settings = Settings.builder().build();
        internalCluster().startMasterOnlyNode(settings);

        final var dataNodes = internalCluster().startDataOnlyNodes(3, settings);

        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

        final DiscoveryNode firstDiscoveryNode =  internalCluster().getInstance(TransportService.class, firstDataNodeName).getLocalNode();
        final DiscoveryNode secondDiscoveryNode = internalCluster().getInstance(TransportService.class, secondDataNodeName).getLocalNode();
        final DiscoveryNode thirdDiscoveryNode = internalCluster().getInstance(TransportService.class, thirdDataNodeName).getLocalNode();
        int randomNumberOfShards = randomIntBetween(10, 20);

        String indexName = "test1";
        int numberOfShards = 4;

        logger.info(
            "---> first node name "
                + firstDataNodeName
                + " and ID "
                + firstDataNodeId
                + "; second node name "
                + secondDataNodeName
                + " and ID "
                + secondDataNodeId
                + "; third node name "
                + thirdDataNodeName
                + " and ID "
                + thirdDataNodeId
        );

        var verifyShardAllocationListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            var indexRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName);
            if (indexRoutingTable == null) {
                return false;
            }
            return checkShardAssignment(clusterState.getRoutingNodes(), indexRoutingTable.getIndex(),
                firstDataNodeId, secondDataNodeId, thirdDataNodeId, numberOfShards);

        });

        createIndex(
            indexName,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);
        logger.info("---> wait for  [" + randomNumberOfShards + "] shards to be assigned to node ");

        safeAwait(verifyShardAllocationListener);
        return new TestHarness(indexName);
    }


    record TestHarness(
        String indexName
    ) {};



}
