/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShutdownEvacuationIT extends ESIntegTestCase {

    private static final Set<String> NOT_PREFERRED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class, NotPreferredPlugin.class);
    }

    @After
    public void clearNotPreferredNodes() {
        NOT_PREFERRED_NODES.clear();
    }

    public void testCanEvacuationToNotPreferredNodeDuringShutdown() {
        final var node1 = internalCluster().startNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        final var node2 = internalCluster().startNode();
        final var node2ID = getNodeId(node2);
        final var node1ID = getNodeId(node1);

        NOT_PREFERRED_NODES.add(node2ID);

        // Mark node 1 as shutting down
        assertAcked(
            internalCluster().client()
                .execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        node1ID,
                        SingleNodeShutdownMetadata.Type.SIGTERM,
                        "testing",
                        null,
                        null,
                        TimeValue.ZERO
                    )
                )
        );

        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                state -> state.routingTable(ProjectId.DEFAULT)
                    .index(indexName)
                    .allShards()
                    .flatMap(IndexShardRoutingTable::allShards)
                    .allMatch(shardRouting -> shardRouting.currentNodeId().equals(node2ID) && shardRouting.started())
            )
        );
    }

    public static class NotPreferredPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {

            return List.of(new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return NOT_PREFERRED_NODES.contains(node.nodeId()) ? Decision.NOT_PREFERRED : Decision.YES;
                }
            });
        }
    }
}
