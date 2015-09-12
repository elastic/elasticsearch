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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0)
@TestLogging("discovery.zen:TRACE")
public class TransportClientRetryIT extends ESIntegTestCase {

    @Test
    public void testRetry() throws IOException, ExecutionException, InterruptedException {

        Iterable<TransportService> instances = internalCluster().getInstances(TransportService.class);
        TransportAddress[] addresses = new TransportAddress[internalCluster().size()];
        int i = 0;
        for (TransportService instance : instances) {
            addresses[i++] = instance.boundAddress().publishAddress();
        }

        Settings.Builder builder = settingsBuilder().put("client.transport.nodes_sampler_interval", "1s")
                .put("name", "transport_client_retry_test")
                .put("node.mode", internalCluster().getNodeMode())
                .put(ClusterName.SETTING, internalCluster().getClusterName())
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
                .put("path.home", createTempDir());

        try (TransportClient transportClient = TransportClient.builder().settings(builder.build()).build()) {
            transportClient.addTransportAddresses(addresses);
            assertThat(transportClient.connectedNodes().size(), equalTo(internalCluster().size()));

            int size = cluster().size();
            //kill all nodes one by one, leaving a single master/data node at the end of the loop
            for (int j = 1; j < size; j++) {
                internalCluster().stopRandomNode(input -> true);

                ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().local(true);
                ClusterState clusterState;
                //use both variants of execute method: with and without listener
                if (randomBoolean()) {
                    clusterState = transportClient.admin().cluster().state(clusterStateRequest).get().getState();
                } else {
                    PlainListenableActionFuture<ClusterStateResponse> future = new PlainListenableActionFuture<>(transportClient.threadPool());
                    transportClient.admin().cluster().state(clusterStateRequest, future);
                    clusterState = future.get().getState();
                }
                assertThat(clusterState.nodes().size(), greaterThanOrEqualTo(size - j));
                assertThat(transportClient.connectedNodes().size(), greaterThanOrEqualTo(size - j));
            }
        }
    }
}
