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

package org.elasticsearch.bwcompat;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;


@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, minNumDataNodes = 0, maxNumDataNodes = 0)
public class UnicastBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("transport.tcp.port", "9380-9390")
                .put("discovery.zen.ping.multicast.enabled", false)
                        // we need to accommodate for the client node
                .put("discovery.zen.ping.unicast.hosts", "localhost:9381,localhost:9382,localhost:9383")
                .build();
    }

    @Override
    protected Settings externalNodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.externalNodeSettings(nodeOrdinal))
                .put("transport.tcp.port", "9380-9390")
                .put("discovery.zen.ping.multicast.enabled", false)
                        // we need to accommodate for the client node
                .put("discovery.zen.ping.unicast.hosts", "localhost:9381,localhost:9382,localhost:9383")
                .build();
    }

    protected int minExternalNodes() {
        return 0;
    }

    protected int maxExternalNodes() {
        return 0;
    }


    @Test
    @TestLogging("discovery.zen:TRACE")
    public void testUnicastDiscovery() throws ExecutionException, InterruptedException, IOException {
        // the backwards comparability infra will *sometimes* start one client node which will grab the first port
        // to make sure this always happens and gives us port stability start the node here.
        backwardsCluster().internalCluster().startNode(ImmutableSettings.builder()
                .put("node.client", "true")
                .put(DiscoveryService.SETTING_INITIAL_STATE_TIMEOUT, "10ms")); // don't wait there is nothing out there.


        // start cluster in a random order, making sure there is at least 1 external node
        final int numOfDataNodes = randomIntBetween(1, 4);
        final int totalNumberOfNodes = numOfDataNodes + 1;
        boolean[] external = new boolean[numOfDataNodes];

        boolean hadExternal = false;
        for (int i = 0; i < external.length; i++) {
            boolean b = randomBoolean();
            hadExternal = hadExternal || b;
            external[i] = b;
        }
        if (!hadExternal) {
            external[randomIntBetween(0, external.length - 1)] = true;
        }

        logger.info("starting [{}] nodes", external.length);
        for (int i = 0; i < external.length; i++) {
            if (external[i]) {
                backwardsCluster().startNewExternalNode();
            } else {
                backwardsCluster().startNewNode();
            }
        }

        logger.info("waiting for nodes to join");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("" + totalNumberOfNodes).get();
        assertThat(healthResponse.getNumberOfDataNodes(), equalTo(numOfDataNodes));


        boolean upgraded;
        do {
            logClusterState();
            upgraded = backwardsCluster().upgradeOneNode();
            healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("" + totalNumberOfNodes).get();
            assertThat(healthResponse.getNumberOfDataNodes(), equalTo(numOfDataNodes));
        } while (upgraded);
    }
}
