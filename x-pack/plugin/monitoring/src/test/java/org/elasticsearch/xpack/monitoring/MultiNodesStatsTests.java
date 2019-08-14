/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;


import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class MultiNodesStatsTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.monitoring.exporters.default_local.type", "local")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringCollection();
        wipeMonitoringIndices();
    }

    public void testMultipleNodes() throws Exception {
        int nodes = 0;

        int n = randomIntBetween(1, 2);
        internalCluster().getPlugins();
        internalCluster().startMasterOnlyNodes(n);
        nodes += n;

        n = randomIntBetween(2, 3);
        internalCluster().startDataOnlyNodes(n);
        nodes += n;

        n = randomIntBetween(1, 2);
        internalCluster().startNodes(n,
                Settings.builder()
                        .put(Node.NODE_DATA_SETTING.getKey(), false)
                        .put(Node.NODE_MASTER_SETTING.getKey(), false)
                        .put(Node.NODE_INGEST_SETTING.getKey(), false).build());
        nodes += n;

        n = randomIntBetween(1, 2);
        // starting one by one to allow moving , for example, from a 2 node cluster to a 4 one while updating min_master_nodes
        for (int i=0;i<n;i++) {
            internalCluster().startNode();
        }
        nodes += n;

        final int nbNodes = nodes;
        assertBusy(() -> {
            assertThat(cluster().size(), equalTo(nbNodes));
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nbNodes)).get());
        });

        enableMonitoringCollection();
        waitForMonitoringIndices();

        assertBusy(() -> {
            flush(ALL_MONITORING_INDICES);
            refresh();

            SearchResponse response = client().prepareSearch(ALL_MONITORING_INDICES)
                    .setQuery(QueryBuilders.termQuery("type", NodeStatsMonitoringDoc.TYPE))
                    .setSize(0)
                    .addAggregation(AggregationBuilders.terms("nodes_ids").field("node_stats.node_id"))
                    .get();

            for (Aggregation aggregation : response.getAggregations()) {
                assertThat(aggregation, instanceOf(StringTerms.class));
                assertThat(((StringTerms) aggregation).getBuckets().size(), equalTo(nbNodes));

                for (String nodeName : internalCluster().getNodeNames()) {
                    StringTerms.Bucket bucket = ((StringTerms) aggregation)
                            .getBucketByKey(internalCluster().clusterService(nodeName).localNode().getId());
                    // At least 1 doc must exist per node, but it can be more than 1
                    // because the first node may have already collected many node stats documents
                    // whereas the last node just started to collect node stats.
                    assertThat(bucket.getDocCount(), greaterThanOrEqualTo(1L));
                }
            }
        });
    }
}
