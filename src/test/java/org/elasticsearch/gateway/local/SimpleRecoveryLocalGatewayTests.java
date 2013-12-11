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

package org.elasticsearch.gateway.local;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.status.IndexShardStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.TestCluster.RestartCallback;
import org.junit.Test;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(numNodes = 0, scope = Scope.TEST)
public class SimpleRecoveryLocalGatewayTests extends ElasticsearchIntegrationTest {


    private ImmutableSettings.Builder settingsBuilder() {
        return ImmutableSettings.settingsBuilder().put("gateway.type", "local");
    }

    @Test
    @Slow
    public void testX() throws Exception {

        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).build());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("appAccountIds").field("type", "string").endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();

        client().prepareIndex("test", "type1", "10990239").setSource(jsonBuilder().startObject()
                .field("_id", "10990239")
                .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990473").setSource(jsonBuilder().startObject()
                .field("_id", "10990473")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990513").setSource(jsonBuilder().startObject()
                .field("_id", "10990513")
                .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990695").setSource(jsonBuilder().startObject()
                .field("_id", "10990695")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "11026351").setSource(jsonBuilder().startObject()
                .field("_id", "11026351")
                .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
        cluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);

        cluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareCount().setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
    }

    @Test
    @Slow
    public void testSingleNodeNoFlush() throws Exception {

        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).build());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("field").field("type", "string").endObject().startObject("num").field("type", "integer").endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("_id", "1").field("field", "value1").startArray("num").value(14).value(179).endArray().endObject()).execute().actionGet();
            client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("_id", "2").field("field", "value2").startArray("num").value(14).endArray().endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }

        cluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }

        cluster().fullRestart();


        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value1")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("field", "value2")).execute().actionGet(), 1);
            assertHitCount(client().prepareCount().setQuery(termQuery("num", 179)).execute().actionGet(), 1);
        }
    }


    @Test
    @Slow
    public void testSingleNodeWithFlush() throws Exception {

        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).build());
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);

        cluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        cluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    @Test
    @Slow
    public void testTwoNodeFirstNodeCleared() throws Exception {

        final String firstNode = cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).build());
        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).build());

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return settingsBuilder().put("gateway.recover_after_nodes", 2).build();
            }

            @Override
            public boolean clearData(String nodeName) {
                return firstNode.equals(nodeName);
            }

        });

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    @Test
    @Slow
    public void testLatestVersionLoaded() throws Exception {
        // clean two nodes

        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).put("gateway.recover_after_nodes", 2).build());
        cluster().startNode(settingsBuilder().put("index.number_of_shards", 1).put("gateway.recover_after_nodes", 2).build());

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        String metaDataUuid = client().admin().cluster().prepareState().execute().get().getState().getMetaData().uuid();
        assertThat(metaDataUuid, not(equalTo("_na_")));

        logger.info("--> closing first node, and indexing more data to the second node");
        cluster().fullRestart(new RestartCallback() {

            @Override
            public void doAfterNodes(int numNodes, Client client) throws Exception {
                if (numNodes == 1) {
                    logger.info("--> one node is closed - start indexing data into the second one");
                    client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).execute().actionGet();
                    client.admin().indices().prepareRefresh().execute().actionGet();

                    for (int i = 0; i < 10; i++) {
                        assertHitCount(client.prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 3);
                    }

                    logger.info("--> add some metadata, additional type and template");
                    client.admin().indices().preparePutMapping("test").setType("type2")
                            .setSource(jsonBuilder().startObject().startObject("type2").startObject("_source").field("enabled", false).endObject().endObject().endObject())
                            .execute().actionGet();
                    client.admin().indices().preparePutTemplate("template_1")
                            .setTemplate("te*")
                            .setOrder(0)
                            .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                                    .startObject("field1").field("type", "string").field("store", "yes").endObject()
                                    .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                                    .endObject().endObject().endObject())
                            .execute().actionGet();
                    client.admin().indices().prepareAliases().addAlias("test", "test_alias", FilterBuilders.termFilter("field", "value")).execute().actionGet();
                    logger.info("--> starting two nodes back, verifying we got the latest version");
                }

            }

        });

        logger.info("--> running cluster_health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(2)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        assertThat(client().admin().cluster().prepareState().execute().get().getState().getMetaData().uuid(), equalTo(metaDataUuid));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metaData().index("test").mapping("type2"), notNullValue());
        assertThat(state.metaData().templates().get("template_1").template(), equalTo("te*"));
        assertThat(state.metaData().index("test").aliases().get("test_alias"), notNullValue());
        assertThat(state.metaData().index("test").aliases().get("test_alias").filter(), notNullValue());
    }

    @Test
    @Slow
    public void testReusePeerRecovery() throws Exception {


        ImmutableSettings.Builder settings = settingsBuilder()
                .put("action.admin.cluster.node.shutdown.delay", "10ms")
                .put("gateway.recover_after_nodes", 4)

                .put(BalancedShardsAllocator.SETTING_THRESHOLD, 1.1f); // use less agressive settings

        cluster().startNode(settings);
        cluster().startNode(settings);
        cluster().startNode(settings);
        cluster().startNode(settings);

        logger.info("--> indexing docs");
        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
            if ((i % 200) == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForRelocatingShards(0)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down the nodes");
        // Disable allocations while we are closing nodes
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)).execute().actionGet();
        cluster().fullRestart();

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(10)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> shutting down the nodes");
        // Disable allocations while we are closing nodes
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)).execute().actionGet();
        cluster().fullRestart();


        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(10)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus("test").setRecovery(true).execute().actionGet();
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

        final String node_1 = cluster().startNode(settingsBuilder().put("path.data", "data/data1").build());

        client().prepareIndex("test", "type1", "1").setSource("field", "value").execute().actionGet();

        cluster().startNode(settingsBuilder().put("path.data", "data/data2").build());

        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        cluster().fullRestart(new RestartCallback() {

            @Override
            public boolean doRestart(String nodeName) {
                return !node_1.equals(nodeName);
            }
        });


        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 1);
    }

}
