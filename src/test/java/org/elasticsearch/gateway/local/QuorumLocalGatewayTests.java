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
import org.elasticsearch.AbstractNodesTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class QuorumLocalGatewayTests extends AbstractNodesTests {

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

    @Test
    @Slow
    public void testChangeInitialShardsRecovery() throws Exception {
        // clean three nodes
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node3", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();


        logger.info("--> starting 3 nodes");
        Node node1 = startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        Node node2 = startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        Node node3 = startNode("node3", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());

        logger.info("--> indexing...");
        node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        node1.client().admin().indices().prepareFlush().get();
        node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        assertNoFailures(node1.client().admin().indices().prepareRefresh().execute().get());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }

        logger.info("--> closing nodes");
        closeAllNodes();

        logger.info("--> starting 2 nodes back, should not do any recovery (less than quorum)");

        node1 = startNode("node1", settingsBuilder().put("gateway.type", "local").build());
        node2 = startNode("node2", settingsBuilder().put("gateway.type", "local").build());

        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                ClusterStateResponse clusterStateResponse = client("node1").admin().cluster().prepareState().setMasterNodeTimeout("500ms").get();
                return !clusterStateResponse.getState().routingTable().index("test").allPrimaryShardsActive();
            }
        }, 30, TimeUnit.SECONDS), equalTo(true));

        logger.info("--> change the recovery.initial_shards setting, and make sure its recovered");
        client("node1").admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("recovery.initial_shards", 1)).get();

        logger.info("--> running cluster_health (wait for the shards to startup), 4 shards since we only have 2 nodes");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(4)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
    }

    @Test
    @Slow
    public void testQuorumRecovery() throws Exception {
        // clean three nodes
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node3", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 3 nodes");
        Node node1 = startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        Node node2 = startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());
        Node node3 = startNode("node3", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 2).build());

        logger.info("--> indexing...");
        node1.client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        node1.client().admin().indices().prepareFlush().get();
        node1.client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        assertNoFailures(node1.client().admin().indices().prepareRefresh().get());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }

        logger.info("--> closing first node, and indexing more data to the second node");
        closeNode("node1");

        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                logger.info("--> running cluster_health (wait for the shards to startup)");
                ClusterHealthResponse clusterHealth = client("node2").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("2").waitForActiveShards(4)).actionGet();
                logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
                return clusterHealth.isTimedOut() && clusterHealth.getStatus() == ClusterHealthStatus.YELLOW;
            }
        }, 30, TimeUnit.SECONDS), equalTo(false));

        node2.client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).get();
        assertNoFailures(node2.client().admin().indices().prepareRefresh().get());

        for (int i = 0; i < 10; i++) {
            assertHitCount(node2.client().prepareCount().setQuery(matchAllQuery()).get(), 3l);
        }

        logger.info("--> closing the second node and third node");
        closeNode("node2");
        closeNode("node3");

        logger.info("--> starting the nodes back, verifying we got the latest version");

        node1 = startNode("node1", settingsBuilder().put("gateway.type", "local").build());
        node2 = startNode("node2", settingsBuilder().put("gateway.type", "local").build());
        node3 = startNode("node3", settingsBuilder().put("gateway.type", "local").build());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForActiveShards(6)).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertHitCount(node1.client().prepareCount().setQuery(matchAllQuery()).get(), 3l);
        }
    }
}
