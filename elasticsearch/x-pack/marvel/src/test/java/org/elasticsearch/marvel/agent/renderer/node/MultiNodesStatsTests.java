/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class MultiNodesStatsTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL_SETTING.getKey(), "-1")
                .put("marvel.agent.exporters.default_local.type", "local")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testMultipleNodes() throws Exception {
        int nodes = 0;

        int n = randomIntBetween(1, 2);
        logger.debug("--> starting {} master only nodes", n);
        InternalTestCluster.Async<List<String>> masterNodes = internalCluster().startMasterOnlyNodesAsync(n);
        masterNodes.get();
        nodes += n;

        n = randomIntBetween(2, 3);
        logger.debug("--> starting {} data only nodes", n);
        InternalTestCluster.Async<List<String>> dataNodes = internalCluster().startDataOnlyNodesAsync(n);
        dataNodes.get();
        nodes += n;

        n = randomIntBetween(1, 2);
        logger.debug("--> starting {} client only nodes", n);
        InternalTestCluster.Async<List<String>> clientNodes = internalCluster().startNodesAsync(n, settingsBuilder().put("node.client", true).build());
        clientNodes.get();
        nodes += n;

        n = randomIntBetween(1, 2);
        logger.debug("--> starting {} extra nodes", n);
        InternalTestCluster.Async<List<String>> extraNodes = internalCluster().startNodesAsync(n);
        extraNodes.get();
        nodes += n;

        final int nbNodes = nodes;
        logger.debug("--> waiting for {} nodes to be available", nbNodes);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(cluster().size(), equalTo(nbNodes));
                assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nbNodes)).get());
            }
        });

        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                securedFlush();

                SearchResponse response = client().prepareSearch()
                        .setTypes(NodeStatsCollector.TYPE)
                        .setSize(0)
                        .addAggregation(AggregationBuilders.terms("nodes_ids").field("node_stats.node_id"))
                        .get();

                for (Aggregation aggregation : response.getAggregations()) {
                    assertThat(aggregation, instanceOf(StringTerms.class));
                    assertThat(((StringTerms) aggregation).getBuckets().size(), equalTo(nbNodes));

                    for (String nodeName : internalCluster().getNodeNames()) {
                        StringTerms.Bucket bucket = (StringTerms.Bucket) ((StringTerms) aggregation).getBucketByKey(internalCluster().clusterService(nodeName).localNode().getId());
                        assertThat(bucket.getDocCount(), equalTo(1L));
                    }
                }
            }
        });
    }
}
