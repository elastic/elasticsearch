/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.NodeRoles.noRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class MultiNodesStatsTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
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
        internalCluster().startNodes(n, noRoles());
        nodes += n;

        n = randomIntBetween(1, 2);
        // starting one by one to allow moving , for example, from a 2 node cluster to a 4 one while updating min_master_nodes
        for (int i = 0; i < n; i++) {
            internalCluster().startNode();
        }
        nodes += n;

        final int nbNodes = nodes;
        assertNoTimeout(safeGet(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes(Integer.toString(nbNodes)).execute()));

        enableMonitoringCollection();
        waitForMonitoringIndices();

        assertBusy(() -> {
            flush(ALL_MONITORING_INDICES);
            refresh();

            assertResponse(
                prepareSearch(ALL_MONITORING_INDICES).setQuery(QueryBuilders.termQuery("type", NodeStatsMonitoringDoc.TYPE))
                    .setSize(0)
                    .addAggregation(AggregationBuilders.terms("nodes_ids").field("node_stats.node_id")),
                response -> {
                    for (Aggregation aggregation : response.getAggregations()) {
                        final var stringTerms = asInstanceOf(StringTerms.class, aggregation);
                        assertThat(stringTerms.getBuckets().size(), equalTo(nbNodes));

                        for (String nodeName : internalCluster().getNodeNames()) {
                            StringTerms.Bucket bucket = stringTerms.getBucketByKey(getNodeId(nodeName));
                            // At least 1 doc must exist per node, but it can be more than 1
                            // because the first node may have already collected many node stats documents
                            // whereas the last node just started to collect node stats.
                            assertThat(bucket.getDocCount(), greaterThanOrEqualTo(1L));
                        }
                    }
                }
            );
        });
    }

    private void waitForMonitoringIndices() throws Exception {
        final var indexNameExpressionResolver = internalCluster().getCurrentMasterNodeInstance(IndexNameExpressionResolver.class);
        final var indicesOptions = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
            .build();
        awaitClusterState(cs -> {
            final var indices = indexNameExpressionResolver.concreteIndices(cs, indicesOptions, ".monitoring-es-*");
            if (indices.length == 0) {
                return false;
            }
            for (Index index : indices) {
                final var indexRoutingTable = cs.routingTable().index(index);
                if (indexRoutingTable.allPrimaryShardsActive() == false) {
                    return false;
                }
            }
            return true;
        });
    }
}
