/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.After;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class MultiNodesStatsTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "-1")
                .put("marvel.agent.exporters.default_local.type", "local")
                .put("marvel.agent.exporters.default_local.template.settings.index.number_of_replicas", 0)
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testMultipleNodes() throws Exception {
        logger.debug("--> starting a master only node");
        internalCluster().startMasterOnlyNodeAsync();

        logger.debug("--> starting a data only node");
        internalCluster().startDataOnlyNodeAsync();

        logger.debug("--> starting a client node");
        internalCluster().startNodeClient(Settings.EMPTY);

        logger.debug("--> starting few other nodes");
        int extraNodes = randomIntBetween(2, 5);
        for (int i = 0; i < extraNodes; i++) {
            if (randomBoolean()) {
                internalCluster().startNodeAsync();
            } else {
                internalCluster().startNodeClient(Settings.EMPTY);
            }
        }

        final int nbNodes = 3 +  extraNodes;
        logger.debug("--> waiting for {} nodes to be available", nbNodes);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nbNodes)).get());
            }
        });

        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                securedFlush();

                for (String nodeName : internalCluster().getNodeNames()) {
                    SearchResponse response = client(nodeName).prepareSearch()
                            .setTypes(NodeStatsCollector.TYPE)
                            .setQuery(QueryBuilders.termQuery("node_stats.node_id", internalCluster().clusterService(nodeName).localNode().getId()))
                            .get();
                    assertThat(response.getHits().getTotalHits(), greaterThan(0L));
                }
            }
        });
    }

}
