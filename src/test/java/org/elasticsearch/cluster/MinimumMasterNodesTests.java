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

package org.elasticsearch.cluster;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numNodes=0)
public class MinimumMasterNodesTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleMinimumMasterNodes() throws Exception {

        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .put("index.number_of_shards", 1)
                .build();

        logger.info("--> start first node");
        cluster().startNode(settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> start second node, cluster should be formed");
        cluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(false));

        createIndex("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(2).execute().actionGet().getActiveShards(), equalTo(2));
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        cluster().stopCurrentMasterNode();
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState  state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            }
        });
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> starting the previous master node again...");
        cluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        cluster().stopRandomNonMasterNode();
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            }
        }), equalTo(true));

        logger.info("--> starting the previous master node again...");
        cluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }

    @Test
    public void multipleNodesShutdownNonMasterNodes() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 3)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first 2 nodes");
        cluster().startNode(settings);
        cluster().startNode(settings);

        ClusterState state;

        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            }
        });
        
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            }
        });

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> start two more nodes");
        cluster().startNode(settings);
        cluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));

        createIndex("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(10).execute().actionGet().isTimedOut(), equalTo(false));
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        cluster().stopRandomNonMasterNode();
        cluster().stopRandomNonMasterNode();

        logger.info("--> verify that there is no master anymore on remaining nodes");
        // spin here to wait till the state is set
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                boolean success = true;
                for(Client client : cluster()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                    success &= state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Checking for NO_MASTER_BLOCL on client: {} NO_MASTER_BLOCK: [{}]", client, state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK));
                    }
                }
                return success;
            }
        }, 20, TimeUnit.SECONDS), equalTo(true));

        logger.info("--> start back the 2 nodes ");
        cluster().startNode(settings);
        cluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }
}
