/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.node;

import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.Matchers.*;

public class NodeStatsCollectorTests extends ESIntegTestCase {

    @Test
    public void testNodeStatsCollector() throws Exception {
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            logger.info("--> collecting node stats on node [{}]", node);
            Collection<MarvelDoc> results = newNodeStatsCollector(node).doCollect();
            assertThat(results, hasSize(1));

            MarvelDoc marvelDoc = results.iterator().next();
            assertNotNull(marvelDoc);
            assertThat(marvelDoc, instanceOf(NodeStatsMarvelDoc.class));

            NodeStatsMarvelDoc nodeStatsMarvelDoc = (NodeStatsMarvelDoc) marvelDoc;
            assertThat(nodeStatsMarvelDoc.clusterName(), equalTo(client().admin().cluster().prepareHealth().get().getClusterName()));
            assertThat(nodeStatsMarvelDoc.timestamp(), greaterThan(0L));
            assertThat(nodeStatsMarvelDoc.type(), equalTo(NodeStatsCollector.TYPE));

            NodeStatsMarvelDoc.Payload payload = nodeStatsMarvelDoc.payload();
            assertNotNull(payload);
            assertThat(payload.getNodeId(), equalTo(internalCluster().getInstance(DiscoveryService.class, node).localNode().id()));
            assertThat(payload.isNodeMaster(), equalTo(node.equals(internalCluster().getMasterName())));
            assertThat(payload.isMlockall(), equalTo(Bootstrap.isMemoryLocked()));
            assertNotNull(payload.isDiskThresholdDeciderEnabled());
            assertNotNull(payload.getDiskThresholdWaterMarkHigh());

            assertNotNull(payload.getNodeStats());
        }
    }

    private NodeStatsCollector newNodeStatsCollector(final String nodeId) {
        return new NodeStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(ClusterName.class, nodeId),
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(NodeService.class, nodeId),
                internalCluster().getInstance(DiscoveryService.class, nodeId),
                new Provider<DiskThresholdDecider>() {
                    @Override
                    public DiskThresholdDecider get() {
                        return internalCluster().getInstance(DiskThresholdDecider.class, nodeId);
                    }
                });
    }
}
