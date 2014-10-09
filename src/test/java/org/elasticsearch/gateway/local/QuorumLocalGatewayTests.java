/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(numDataNodes =0, scope= Scope.TEST)
public class QuorumLocalGatewayTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    @Test
    @Slow
    public void testChangeInitialShardsRecovery() throws Exception {
        logger.info("--> starting 3 nodes");
        final String[] nodes = internalCluster().startNodesAsync(3, settingsBuilder().put("gateway.type", "local").build()).get().toArray(new String[0]);

        createIndex("test");
        ensureGreen();
        NumShards test = getNumShards("test");

        logger.info("--> indexing...");
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        refresh();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
        
        final String nodeToRemove = nodes[between(0,2)];
        logger.info("--> restarting 1 nodes -- kill 2");
        internalCluster().fullRestart(new RestartCallback() {
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
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForNodes("1")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.RED));  // nothing allocated yet
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                ClusterStateResponse clusterStateResponse = internalCluster().smartClient().admin().cluster().prepareState().setMasterNodeTimeout("500ms").get();
                return clusterStateResponse.getState() != null && clusterStateResponse.getState().routingTable().index("test") != null;
            }}), equalTo(true)); // wait until we get a cluster state - could be null if we quick enough.
        final ClusterStateResponse clusterStateResponse = internalCluster().smartClient().admin().cluster().prepareState().setMasterNodeTimeout("500ms").get();
        assertThat(clusterStateResponse.getState(), notNullValue());
        assertThat(clusterStateResponse.getState().routingTable().index("test"), notNullValue());
        assertThat(clusterStateResponse.getState().routingTable().index("test").allPrimaryShardsActive(), is(false));
        logger.info("--> change the recovery.initial_shards setting, and make sure its recovered");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("recovery.initial_shards", 1)).get();

        logger.info("--> running cluster_health (wait for the shards to startup), primaries only since we only have 1 node");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(test.numPrimaries)).actionGet();
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
        internalCluster().startNodesAsync(3, settingsBuilder().put("gateway.type", "local").build()).get();
        // we are shutting down nodes - make sure we don't have 2 clusters if we test network
        setMinimumMasterNodes(2);

        createIndex("test");
        ensureGreen();
        final NumShards test = getNumShards("test");

        logger.info("--> indexing...");
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        refresh();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2l);
        }
        logger.info("--> restart all nodes");
        internalCluster().fullRestart(new RestartCallback() {
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
                            ClusterHealthResponse clusterHealth = activeClient.admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("2").waitForActiveShards(test.numPrimaries * 2)).actionGet();
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
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 3l);
        }
    }
}
