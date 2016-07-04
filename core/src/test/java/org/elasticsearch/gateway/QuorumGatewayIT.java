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

package org.elasticsearch.gateway;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 *
 */
@ClusterScope(numDataNodes =0, scope= Scope.TEST)
public class QuorumGatewayIT extends ESIntegTestCase {
    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    public void testQuorumRecovery() throws Exception {
        logger.info("--> starting 3 nodes");
        // we are shutting down nodes - make sure we don't have 2 clusters if we test network
        internalCluster().startNodesAsync(3,
                Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2).build()).get();


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
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2L);
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
                    assertTrue(awaitBusy(() -> {
                        logger.info("--> running cluster_health (wait for the shards to startup)");
                        ClusterHealthResponse clusterHealth = activeClient.admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("2").waitForActiveShards(test.numPrimaries * 2)).actionGet();
                        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());
                        return (!clusterHealth.isTimedOut()) && clusterHealth.getStatus() == ClusterHealthStatus.YELLOW;
                    }, 30, TimeUnit.SECONDS));
                    logger.info("--> one node is closed -- index 1 document into the remaining nodes");
                    activeClient.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).get();
                    assertNoFailures(activeClient.admin().indices().prepareRefresh().get());
                    for (int i = 0; i < 10; i++) {
                        assertHitCount(activeClient.prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
                    }
                }
            }

        });
        logger.info("--> all nodes are started back, verifying we got the latest version");
        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
        }
    }
}
