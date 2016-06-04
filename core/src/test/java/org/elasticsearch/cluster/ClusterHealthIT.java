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

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class ClusterHealthIT extends ESIntegTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void customBeforeClass() throws Exception {
        threadPool = new ThreadPool("ClusterHealthIT");
    }

    @AfterClass
    public static void customAfterClass() throws Exception {
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testSimpleLocalHealth() {
        createIndex("test");
        ensureGreen(); // master should thing it's green now.

        for (String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            ClusterHealthResponse health = client(node).admin().cluster().prepareHealth()
                                                                         .setLocal(true)
                                                                         .setWaitForEvents(Priority.LANGUID)
                                                                         .setTimeout("30s")
                                                                         .get("10s");
            assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(health.isTimedOut(), equalTo(false));
        }
    }

    public void testHealth() {
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("test1")
                                                                         .setWaitForYellowStatus()
                                                                         .setTimeout("1s")
                                                                         .execute()
                                                                         .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> running cluster wide health");
        healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> Creating index test1 with zero replicas");
        createIndex("test1");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = client().admin().cluster().prepareHealth("test1").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = client().admin().cluster().prepareHealth("test1", "test2")
                                                   .setWaitForYellowStatus()
                                                   .setTimeout("1s")
                                                   .execute()
                                                   .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().size(), equalTo(1));
    }

    public void testHealthOnIndexCreation() throws Exception {
        final int numNodes = randomIntBetween(2, 5);
        logger.info("--> starting {} nodes", numNodes);
        final Settings nodeSettings = Settings.builder().put(
            ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), numNodes
        ).build();
        internalCluster().ensureAtLeastNumDataNodes(numNodes, nodeSettings);

        ClusterHealthResponse healthResponse = client().admin().cluster()
                                                               .prepareHealth()
                                                               .setWaitForGreenStatus()
                                                               .setTimeout("10s")
                                                               .execute()
                                                               .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // first, register a cluster state observer that checks cluster health
        // upon index creation in the cluster state
        final String masterNode = internalCluster().getMasterName();
        final String indexName = "test-idx";
        final ClusterService clusterService = internalCluster().clusterService(masterNode);
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        final ClusterStateObserver.ChangePredicate validationPredicate = new ClusterStateObserver.ValidationPredicate() {
            @Override
            protected boolean validate(ClusterState newState) {
                return newState.status() == ClusterState.ClusterStateStatus.APPLIED
                           && newState.metaData().hasIndex(indexName);
            }
        };

        final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState clusterState) {
                // make sure we have inactive primaries
                // see if we can terminate observing on the cluster state
                final ClusterStateResponse csResponse = client().admin().cluster().prepareState().execute().actionGet();
                boolean inactivePrimaries = false;
                for (ShardRouting shardRouting : csResponse.getState().routingTable().allShards(indexName)) {
                    if (shardRouting.primary() == false) {
                        continue;
                    }
                    if (shardRouting.active() == false) {
                        inactivePrimaries = true;
                        break;
                    }
                }
                assertTrue(inactivePrimaries);
                // verify cluster health is YELLOW (even though primaries are still being allocated)
                final ClusterHealthResponse response = client().admin().cluster().prepareHealth(indexName).get();
                assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            }
            @Override
            public void onClusterServiceClose() {
                fail("cluster service should not have closed");
            }
            @Override
            public void onTimeout(TimeValue timeout) {
                fail("timeout on cluster state observer");
            }
        };
        observer.waitForNextChange(stateListener, validationPredicate, TimeValue.timeValueSeconds(30L));
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numNodes)
                                                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numNodes - 1)
                                                    .build();
        CreateIndexResponse response = client().admin().indices().prepareCreate(indexName)
                                                                 .setSettings(settings)
                                                                 .execute()
                                                                 .actionGet();
        assertTrue(response.isAcknowledged());

        // now, make sure we eventually get to the green state,
        // we have at least two nodes so this should happen
        ensureGreen(indexName);
    }

}
