/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A {@code bulk} with {@code refresh=true} that adds a field through dynamic mapping waits only for the nodes holding copies of the
 * written shards to apply the new mapping, unless one of those copies lives on the elected master, which applies each cluster state
 * after every other node has. A node holding none of the written shards can therefore still lack the field when the bulk returns, and
 * ES|QL resolves its columns from a single node's local mapping. The ES|QL yaml suites wait for a {@code LANGUID} cluster health check
 * before every query ({@code EsqlClientYamlTestCase}); these tests pin down the cluster behaviour that wait relies on.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDynamicMappingVisibilityIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(10);

    public void testLanguidHealthWaitCoversNodeHoldingOnlyUntouchedShard() throws Exception {
        internalCluster().startMasterOnlyNode();
        var dataNodes = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        var indexName = createIndexWithOnePrimaryPerNode(dataNodes);
        var writingNode = dataNodes.get(0);
        var laggingNode = dataNodes.get(1);
        var ids = idsRoutedToShard(indexName, primaryNodeByShard(indexName).indexOf(writingNode), 4);
        long versionBefore = appliedMappingVersion(laggingNode, indexName);

        var disruption = new BlockClusterStateProcessing(laggingNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        ActionFuture<ClusterHealthResponse> healthFuture;
        try {
            assertNoFailures(indexDocsWithNewField(writingNode, indexName, ids).actionGet(TIMEOUT));
            assertThat(appliedMappingVersion(writingNode, indexName), greaterThan(versionBefore));
            assertThat(appliedMappingVersion(laggingNode, indexName), equalTo(versionBefore));

            healthFuture = client(writingNode).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT)
                .setWaitForEvents(Priority.LANGUID)
                .execute();
            assertFalse("languid wait completed while a node was lagging", waitUntil(healthFuture::isDone, 2, TimeUnit.SECONDS));
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }

        assertFalse(healthFuture.actionGet(TIMEOUT).isTimedOut());
        assertThat(appliedMappingVersion(laggingNode, indexName), greaterThan(versionBefore));
        assertEsqlSeesField(indexName, ids.size());
    }

    public void testBulkWaitsForLaggingNodeWhenPrimaryIsOnMaster() throws Exception {
        var masterNode = internalCluster().startNode();
        var otherNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        var indexName = createIndexWithOnePrimaryPerNode(List.of(masterNode, otherNode));
        var ids = idsRoutedToShard(indexName, primaryNodeByShard(indexName).indexOf(masterNode), 4);
        long versionBefore = appliedMappingVersion(masterNode, indexName);

        var disruption = new BlockClusterStateProcessing(otherNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        ActionFuture<BulkResponse> bulkFuture;
        try {
            bulkFuture = indexDocsWithNewField(masterNode, indexName, ids);
            assertFalse("bulk completed while a node was lagging", waitUntil(bulkFuture::isDone, 2, TimeUnit.SECONDS));
            assertThat(
                "the master applies the new mapping only after every other node",
                appliedMappingVersion(masterNode, indexName),
                equalTo(versionBefore)
            );
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }

        assertNoFailures(bulkFuture.actionGet(TIMEOUT));
        assertEsqlSeesField(indexName, ids.size());
    }

    private String createIndexWithOnePrimaryPerNode(List<String> nodes) {
        var indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(nodes.size(), 0).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            )
        );
        ensureGreen(indexName);
        assertThat(primaryNodeByShard(indexName), containsInAnyOrder(nodes.toArray()));
        return indexName;
    }

    private static List<String> primaryNodeByShard(String indexName) {
        var state = internalCluster().clusterService().state();
        var indexRoutingTable = state.routingTable(ProjectId.DEFAULT).index(indexName);
        var nodes = new ArrayList<String>();
        for (int shard = 0; shard < indexRoutingTable.size(); shard++) {
            nodes.add(state.nodes().get(indexRoutingTable.shard(shard).primaryShard().currentNodeId()).getName());
        }
        return nodes;
    }

    private static List<String> idsRoutedToShard(String indexName, int shard, int count) {
        var indexRouting = IndexRouting.fromIndexMetadata(
            internalCluster().clusterService().state().metadata().getProject(ProjectId.DEFAULT).index(indexName)
        );
        var ids = new ArrayList<String>();
        for (int i = 0; ids.size() < count; i++) {
            var id = "doc-" + i;
            if (indexRouting.indexShard(new IndexRequest(indexName).id(id)) == shard) {
                ids.add(id);
            }
        }
        return ids;
    }

    private static long appliedMappingVersion(String node, String indexName) {
        return internalCluster().clusterService(node).state().metadata().getProject(ProjectId.DEFAULT).index(indexName).getMappingVersion();
    }

    private static ActionFuture<BulkResponse> indexDocsWithNewField(String node, String indexName, List<String> ids) {
        var bulk = client(node).prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (var id : ids) {
            bulk.add(new IndexRequest(indexName).id(id).source("@timestamp", "2025-05-31T00:00:00Z"));
        }
        return bulk.execute();
    }

    private void assertEsqlSeesField(String indexName, int expectedDocs) {
        try (var response = run("FROM " + indexName + " | KEEP @timestamp")) {
            var values = new ArrayList<>();
            response.column(0).forEachRemaining(values::add);
            assertThat(values, hasSize(expectedDocs));
            assertThat(values, everyItem(notNullValue()));
        }
    }
}
