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

package org.elasticsearch.discovery;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope=Scope.TEST, numDataNodes =2)
public class ZenUnicastDiscoveryTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", "localhost")
                .put("transport.tcp.port", "25300-25400") // Need to use custom tcp port range otherwise we collide with the shared cluster
                .put(super.nodeSettings(nodeOrdinal)).build();
    }
    
    @Test
    public void testUnicastDiscovery() {
        for (Client client : clients()) {
            ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
            //client nodes might be added randomly
            int dataNodes = 0;
            for (DiscoveryNode discoveryNode : state.nodes()) {
                if (discoveryNode.isDataNode()) {
                    dataNodes++;
                }
            }
            assertThat(dataNodes, equalTo(2));
        }
    }
}
