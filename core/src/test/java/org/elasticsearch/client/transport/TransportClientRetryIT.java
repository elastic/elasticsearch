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
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0, supportsDedicatedMasters = false)
public class TransportClientRetryIT extends ESIntegTestCase {
    public void testRetry() throws IOException, ExecutionException, InterruptedException {
        Iterable<TransportService> instances = internalCluster().getInstances(TransportService.class);
        TransportAddress[] addresses = new TransportAddress[internalCluster().size()];
        int i = 0;
        for (TransportService instance : instances) {
            addresses[i++] = instance.boundAddress().publishAddress();
        }

        Settings.Builder builder = Settings.builder().put("client.transport.nodes_sampler_interval", "1s")
                .put("node.name", "transport_client_retry_test")
                .put(Node.NODE_MODE_SETTING.getKey(), internalCluster().getNodeMode())
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), internalCluster().getClusterName())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir());

        try (TransportClient client = TransportClient.builder().settings(builder.build()).build()) {
            client.addTransportAddresses(addresses);
            assertThat(client.connectedNodes().size(), equalTo(internalCluster().size()));

            int size = cluster().size();
            //kill all nodes one by one, leaving a single master/data node at the end of the loop
            for (int j = 1; j < size; j++) {
                internalCluster().stopRandomNode(input -> true);

                ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().local(true);
                ClusterState clusterState;
                //use both variants of execute method: with and without listener
                if (randomBoolean()) {
                    clusterState = client.admin().cluster().state(clusterStateRequest).get().getState();
                } else {
                    PlainListenableActionFuture<ClusterStateResponse> future = new PlainListenableActionFuture<>(client.threadPool());
                    client.admin().cluster().state(clusterStateRequest, future);
                    clusterState = future.get().getState();
                }
                assertThat(clusterState.nodes().getSize(), greaterThanOrEqualTo(size - j));
                assertThat(client.connectedNodes().size(), greaterThanOrEqualTo(size - j));
            }
        }
    }
}
