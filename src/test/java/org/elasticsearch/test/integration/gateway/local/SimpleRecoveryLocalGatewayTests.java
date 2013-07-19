/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.gateway.local;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.status.IndexShardStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleRecoveryLocalGatewayTests extends AbstractNodesTests {

    @After
    public void cleanAndCloseNodes() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (node("node" + i) != null) {
                node("node" + i).stop();
                // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
                ((InternalNode) node("node" + i)).injector().getInstance(Gateway.class).reset();
            }
        }
        closeAllNodes();
    }

    @Override
    protected Settings getClassDefaultSettings() {
        return settingsBuilder().put("gateway.type", "local").build();
    }

    @Test
    @Slow
    public void testX() throws Exception {
        buildNode("node1");
        cleanAndCloseNodes();

        Node node1 = startNode("node1", settingsBuilder().put("index.number_of_shards", 1).build());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("appAccountIds").field("type", "string").endObject().endObject()
                .endObject().endObject().string();
        node1.client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();

        node1.client().prepareIndex("test", "type1", "10990239").setSource(jsonBuilder().startObject()
                .field("_id", "10990239")
                .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        node1.client().prepareIndex("test", "type1", "10990473").setSource(jsonBuilder().startObject()
                .field("_id", "10990473")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        node1.client().prepareIndex("test", "type1", "10990513").setSource(jsonBuilder().startObject()
                .field("_id", "10990513")
                .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        node1.client().prepareIndex("test", "type1", "10990695").setSource(jsonBuilder().startObject()
                .field("_id", "10990695")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        node1.client().prepareIndex("test", "type1", "11026351").setSource(jsonBuilder().startObject()
                .field("_id", "11026351")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();

        node1.client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(node1.client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);

        closeNode("node1");
        node1 = startNode("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        node1.client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(node1.client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);

        closeNode("node1");
        node1 = startNode("node1");
        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = node1.client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        node1.client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(node1.client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
    }

    @Test
    @Slow
    public void testSingleNodeNoFlush() throws Exception {
        buildNode("node1");
        cleanAndCloseNodes();

        Node node1 = startNode("node1", settingsBuilder().put("index.number_of_shards", 1).build());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("field").field("type", "string").endObject().startObject("num").field("type", "integer").endObject().endObject()
                .endObject().endObject().string();
        node1.client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();

        for (int i = 0; i < 100; i++) {
            node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("_id", "1").field("field", "value1").startArray("num").value(14).value(179).endArray().endObject()).execute().actionGet();
            node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("_id", "2").field("field", "value2").startArray("num").value(14).endArray().endObject()).execute().actionGet();
        }

        node1.client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }

        closeNode("node1");
        node1 = startNode("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }

        closeNode("node1");
        node1 = startNode("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(node1.client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }
    }


    @Test
    @Slow
    public void testSingleNodeWithFlush() throws Exception {
        buildNode("node1");
        cleanAndCloseNodes();

        Node node1 = startNode("node1", settingsBuilder().put("index.number_of_shards", 1).build());
        node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareFlush().execute().actionGet();
        node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareRefresh().execute().actionGet();

        assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);

        closeNode("node1");
        node1 = startNode("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        closeNode("node1");
        node1 = startNode("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    @Test
    @Slow
    public void testTwoNodeFirstNodeCleared() throws Exception {
        // clean two nodes
        buildNode("node1");
        buildNode("node2");
        cleanAndCloseNodes();

        Node node1 = startNode("node1", settingsBuilder().put("index.number_of_shards", 1).build());
        Node node2 = startNode("node2", settingsBuilder().put("index.number_of_shards", 1).build());

        node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareFlush().execute().actionGet();
        node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        logger.info("--> closing nodes");
        closeNode("node1");
        closeNode("node2");

        logger.info("--> cleaning node1 gateway");
        buildNode("node1");
        cleanAndCloseNodes();

        node1 = startNode("node1", settingsBuilder().put("gateway.recover_after_nodes", 2).build());
        node2 = startNode("node2", settingsBuilder().put("gateway.recover_after_nodes", 2).build());

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    @Test
    @Slow
    public void testLatestVersionLoaded() throws Exception {
        // clean two nodes
        buildNode("node1");
        buildNode("node2");
        cleanAndCloseNodes();

        Node node1 = startNode("node1", settingsBuilder().put("index.number_of_shards", 1).put("gateway.recover_after_nodes", 2).build());
        Node node2 = startNode("node2", settingsBuilder().put("index.number_of_shards", 1).put("gateway.recover_after_nodes", 2).build());

        node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareFlush().execute().actionGet();
        node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        node1.client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        logger.info("--> closing first node, and indexing more data to the second node");
        closeNode("node1");

        node2.client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).execute().actionGet();
        node2.client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            assertHitCount(node2.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        logger.info("--> add some metadata, additional type and template");
        node2.client().admin().indices().preparePutMapping("test").setType("type2")
                .setSource(jsonBuilder().startObject().startObject("type1").startObject("_source").field("enabled", false).endObject().endObject().endObject())
                .execute().actionGet();
        node2.client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        node2.client().admin().indices().prepareAliases().addAlias("test", "test_alias", FilterBuilders.termFilter("field", "value")).execute().actionGet();


        logger.info("--> closing the second node");
        closeNode("node2");

        logger.info("--> starting two nodes back, verifying we got the latest version");

        node1 = startNode("node1", settingsBuilder().put("gateway.recover_after_nodes", 2).build());
        node2 = startNode("node2", settingsBuilder().put("gateway.recover_after_nodes", 2).build());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        ClusterState state = node1.client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metaData().index("test").mapping("type2"), notNullValue());
        assertThat(state.metaData().templates().get("template_1").template(), equalTo("te*"));
        assertThat(state.metaData().index("test").aliases().get("test_alias"), notNullValue());
        assertThat(state.metaData().index("test").aliases().get("test_alias").filter(), notNullValue());
    }

    @Test
    @Slow
    public void testReusePeerRecovery() throws Exception {
        buildNode("node1");
        buildNode("node2");
        buildNode("node3");
        buildNode("node4");
        cleanAndCloseNodes();


        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
                .put("action.admin.cluster.node.shutdown.delay", "10ms")
                .put("gateway.recover_after_nodes", 4)
                
                .put(BalancedShardsAllocator.SETTING_THRESHOLD, 1.1f); // use less agressive settings

        startNode("node1", settings);
        startNode("node2", settings);
        startNode("node3", settings);
        startNode("node4", settings);

        logger.info("--> indexing docs");
        for (int i = 0; i < 1000; i++) {
            client("node1").prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
            if ((i % 200) == 0) {
                client("node1").admin().indices().prepareFlush().execute().actionGet();
            }
        }
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForRelocatingShards(0)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down the nodes");
        // Disable allocations while we are closing nodes
        client("node1").admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)).execute().actionGet();
        for (int i = 1; i < 5; i++) {
            closeNode("node" + i);
        }

        logger.info("--> start the nodes back up");
        startNode("node1", settings);
        startNode("node2", settings);
        startNode("node3", settings);
        startNode("node4", settings);

        logger.info("Running Cluster Health");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(10)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down the nodes");
        // Disable allocations while we are closing nodes
        client("node1").admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)).execute().actionGet();
        for (int i = 1; i < 5; i++) {
            closeNode("node" + i);
        }

        logger.info("--> start the nodes back up");
        startNode("node1", settings);
        startNode("node2", settings);
        startNode("node3", settings);
        startNode("node4", settings);

        logger.info("Running Cluster Health");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(10)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        IndicesStatusResponse statusResponse = client("node1").admin().indices().prepareStatus("test").setRecovery(true).execute().actionGet();
        for (IndexShardStatus indexShardStatus : statusResponse.getIndex("test")) {
            for (ShardStatus shardStatus : indexShardStatus) {
                if (!shardStatus.getShardRouting().primary()) {
                    logger.info("--> shard {}, recovered {}, reuse {}", shardStatus.getShardId(), shardStatus.getPeerRecoveryStatus().getRecoveredIndexSize(), shardStatus.getPeerRecoveryStatus().getReusedIndexSize());
                    assertThat(shardStatus.getPeerRecoveryStatus().getRecoveredIndexSize().bytes(), greaterThan(0l));
                    assertThat(shardStatus.getPeerRecoveryStatus().getReusedIndexSize().bytes(), greaterThan(0l));
                    assertThat(shardStatus.getPeerRecoveryStatus().getReusedIndexSize().bytes(), greaterThan(shardStatus.getPeerRecoveryStatus().getRecoveredIndexSize().bytes()));
                }
            }
        }
    }

    @Test
    @Slow
    public void testRecoveryDifferentNodeOrderStartup() throws Exception {
        // we need different data paths so we make sure we start the second node fresh
        buildNode("node1", settingsBuilder().put("path.data", "data/data1").build());
        buildNode("node2", settingsBuilder().put("path.data", "data/data2").build());
        cleanAndCloseNodes();

        startNode("node1", settingsBuilder().put("path.data", "data/data1").build());

        client("node1").prepareIndex("test", "type1", "1").setSource("field", "value").execute().actionGet();

        startNode("node2", settingsBuilder().put("path.data", "data/data2").build());

        ClusterHealthResponse health = client("node2").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        closeNode("node1");
        closeNode("node2");

        startNode("node2", settingsBuilder().put("path.data", "data/data2").build());

        health = client("node2").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        assertThat(client("node2").admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        assertHitCount(client("node2").prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 1);
    }

}
