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

package org.elasticsearch.client.transport;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0)
public class TransportClientRetryTests extends ElasticsearchIntegrationTest {

    @Test
    public void testRetry() throws IOException, ExecutionException, InterruptedException {

        Iterable<TransportService> instances = internalCluster().getInstances(TransportService.class);
        TransportAddress[] addresses = new TransportAddress[internalCluster().size()];
        int i = 0;
        for (TransportService instance : instances) {
            addresses[i++] = instance.boundAddress().publishAddress();
        }

        ImmutableSettings.Builder builder = settingsBuilder().put("client.transport.nodes_sampler_interval", "1s")
                .put("name", "transport_client_retry_test")
                .put("node.mode", InternalTestCluster.nodeMode())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .put(ClusterName.SETTING, internalCluster().getClusterName())
                .put("config.ignore_system_properties", true);

        try (TransportClient transportClient = new TransportClient(builder.build())) {
            transportClient.addTransportAddresses(addresses);
            assertThat(transportClient.connectedNodes().size(), equalTo(internalCluster().size()));

            int size = cluster().size();
            //kill all nodes one by one, leaving a single master/data node at the end of the loop
            for (int j = 1; j < size; j++) {
                internalCluster().stopRandomNode(new Predicate<Settings>() {
                    @Override
                    public boolean apply(Settings input) {
                        return true;
                    }
                });

                ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().local(true);
                ClusterState clusterState;
                //use both variants of execute method: with and without listener
                if (randomBoolean()) {
                    clusterState = transportClient.admin().cluster().state(clusterStateRequest).get().getState();
                } else {
                    PlainListenableActionFuture<ClusterStateResponse> future = new PlainListenableActionFuture<>(clusterStateRequest.listenerThreaded(), transportClient.threadPool());
                    transportClient.admin().cluster().state(clusterStateRequest, future);
                    clusterState = future.get().getState();
                }
                assertThat(clusterState.nodes().size(), greaterThanOrEqualTo(size - j));
                assertThat(transportClient.connectedNodes().size(), greaterThanOrEqualTo(size - j));
            }
        }
    }
}
