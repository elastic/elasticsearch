/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.NodeRoles.dataNode;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleDataNodesIT extends ESIntegTestCase {

    private static final String SOURCE = """
        {"type1":{"id":"1","name":"test"}}""";

    public void testIndexingBeforeAndAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        indicesAdmin().create(new CreateIndexRequest("test").waitForActiveShards(ActiveShardCount.NONE)).actionGet();
        try {
            client().index(new IndexRequest("test").id("1").source(SOURCE, XContentType.JSON).timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        internalCluster().startNode(nonDataNode());
        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        // still no shard should be allocated
        try {
            client().index(new IndexRequest("test").id("1").source(SOURCE, XContentType.JSON).timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        internalCluster().startNode(dataNode());
        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        IndexResponse indexResponse = client().index(new IndexRequest("test").id("1").source(SOURCE, XContentType.JSON)).actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
    }

    public void testShardsAllocatedAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        indicesAdmin().create(
            new CreateIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .waitForActiveShards(ActiveShardCount.NONE)
        ).actionGet();
        final ClusterHealthResponse healthResponse1 = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode(dataNode());

        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setWaitForGreenStatus()
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );
    }

    public void testAutoExpandReplicasAdjustedWhenDataNodeJoins() {
        internalCluster().startNode(nonDataNode());
        indicesAdmin().create(
            new CreateIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
                .waitForActiveShards(ActiveShardCount.NONE)
        ).actionGet();
        final ClusterHealthResponse healthResponse1 = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode();
        internalCluster().startNode();
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
    }

}
