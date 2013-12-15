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

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.TestCluster.RestartCallback;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ClusterScope(numNodes=0, scope=Scope.TEST)
public class QuorumLocalGatewayTests extends ElasticsearchIntegrationTest {

    @Test
    @Slow
    public void testChangeInitialShardsRecovery() throws Exception {
        logger.info("--> starting 3 nodes");
        final String[] nodes = new String[3];
        nodes[0] = cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        nodes[1] = cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        nodes[2] = cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());

        logger.info("--> indexing...");
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        assertNoFailures(client().admin().indices().prepareRefresh().execute().get());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
        
        final String nodeToRemove = nodes[between(0,2)];
        logger.info("--> restarting 1 nodes -- kill 2");
        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return settingsBuilder().put("gateway.type", "local").build();
            }
            
            @Override
            public boolean doRestart(String nodeName) {
                return nodeToRemove.equals(nodeName);
            }
        });
        if (randomBoolean()) {
            Thread.sleep(between(1, 400)); // wait a bit and give is a chance to try to allocate
        }
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForNodes("1")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.RED));  // nothing allocated yet
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                ClusterStateResponse clusterStateResponse = cluster().smartClient().admin().cluster().prepareState().setMasterNodeTimeout("500ms").get();
                return clusterStateResponse.getState() != null;
            }}), equalTo(true)); // wait until we get a cluster state - could be null if we quick enough.
        final ClusterStateResponse clusterStateResponse = cluster().smartClient().admin().cluster().prepareState().setMasterNodeTimeout("500ms").get();
        assertThat(clusterStateResponse.getState(), notNullValue());
        assertThat(clusterStateResponse.getState().routingTable().index("test"), notNullValue());
        assertThat(clusterStateResponse.getState().routingTable().index("test").allPrimaryShardsActive(), is(false));
        logger.info("--> change the recovery.initial_shards setting, and make sure its recovered");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("recovery.initial_shards", 1)).get();

        logger.info("--> running cluster_health (wait for the shards to startup), 2 shards since we only have 1 node");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(2)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
    }

    @Test
    @Slow
    public void testQuorumRecovery() throws Exception {

        logger.info("--> starting 3 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());

        logger.info("--> indexing...");
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        assertNoFailures(client().admin().indices().prepareRefresh().get());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 2)) // we are shutting down nodes - make sure we don't have 2 clusters if we test network
                .get();
        logger.info("--> restart all nodes");
        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return null;
            }

            @Override
            public void doAfterNodes(int numNodes, final Client activeClient) throws Exception {
                if (numNodes == 1) {
                    assertThat(awaitBusy(new Predicate<Object>() {
                        @Override
                        public boolean apply(Object input) {
                            logger.info("--> running cluster_health (wait for the shards to startup)");
                            ClusterHealthResponse clusterHealth = activeClient.admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("2").waitForActiveShards(4)).actionGet();
                            logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
                            return (!clusterHealth.isTimedOut()) && clusterHealth.getStatus() == ClusterHealthStatus.YELLOW;
                        }
                    }, 30, TimeUnit.SECONDS), equalTo(true));
                    logger.info("--> one node is closed -- index 1 document into the remaining nodes");
                    activeClient.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).get();
                    assertNoFailures(activeClient.admin().indices().prepareRefresh().get());
                    for (int i = 0; i < 10; i++) {
                        assertHitCount(activeClient.prepareCount().setQuery(matchAllQuery()).get(), 3l);
                    }
                }
            }
            
        });
        logger.info("--> all nodes are started back, verifying we got the latest version");
        logger.info("--> running cluster_health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 3l);
        }
    }
}
