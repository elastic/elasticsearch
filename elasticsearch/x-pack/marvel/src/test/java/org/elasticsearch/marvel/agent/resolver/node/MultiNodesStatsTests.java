/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.node;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class MultiNodesStatsTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters.default_local.type", "local")
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
        InternalTestCluster.Async<List<String>> clientNodes = internalCluster().startNodesAsync(n,
                Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_MASTER_SETTING.getKey(), false)
                        .put(Node.NODE_INGEST_SETTING.getKey(), false).build());
        clientNodes.get();
        nodes += n;

        n = randomIntBetween(1, 2);
        logger.debug("--> starting {} extra nodes", n);
        InternalTestCluster.Async<List<String>> extraNodes = internalCluster().startNodesAsync(n);
        extraNodes.get();
        nodes += n;

        final int nbNodes = nodes;
        logger.debug("--> waiting for {} nodes to be available", nbNodes);
        assertBusy(() -> {
            assertThat(cluster().size(), equalTo(nbNodes));
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nbNodes)).get());
        });

        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();

        logger.debug("--> checking that every node correctly reported its own node stats");
        assertBusy(() -> {
            String indices = MONITORING_INDICES_PREFIX + "*";
            securedFlush(indices);
            securedRefresh();

            try {
                SearchResponse response = client().prepareSearch(indices)
                        .setTypes(NodeStatsResolver.TYPE)
                        .setSize(0)
                        .addAggregation(AggregationBuilders.terms("nodes_ids").field("node_stats.node_id"))
                        .get();

                for (Aggregation aggregation : response.getAggregations()) {
                    assertThat(aggregation, instanceOf(StringTerms.class));
                    assertThat(((StringTerms) aggregation).getBuckets().size(), equalTo(nbNodes));

                    for (String nodeName : internalCluster().getNodeNames()) {
                        StringTerms.Bucket bucket = (StringTerms.Bucket) ((StringTerms) aggregation)
                                .getBucketByKey(internalCluster().clusterService(nodeName).localNode().getId());
                        // At least 1 doc must exist per node, but it can be more than 1
                        // because the first node may have already collected many node stats documents
                        // whereas the last node just started to collect node stats.
                        assertThat(bucket.getDocCount(), greaterThanOrEqualTo(1L));
                    }
                }
            } catch (IndexNotFoundException e) {
                fail("Caught unexpected IndexNotFoundException");
            }
        });
    }
}
