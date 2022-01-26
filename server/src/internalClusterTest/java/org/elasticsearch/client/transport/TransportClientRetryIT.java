/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transport;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

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

        String transport = getTestTransportType();

        Settings.Builder builder = Settings.builder()
            .put("client.transport.nodes_sampler_interval", "1s")
            .put("node.name", "transport_client_retry_test")
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), internalCluster().getClusterName())
            .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), transport)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir());

        try (TransportClient client = new MockTransportClient(builder.build())) {
            client.addTransportAddresses(addresses);
            assertEquals(client.connectedNodes().size(), internalCluster().size());

            int size = cluster().size();
            // kill all nodes one by one, leaving a single master/data node at the end of the loop
            for (int j = 1; j < size; j++) {
                internalCluster().stopRandomNode(input -> true);

                ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().local(true);
                ClusterState clusterState;
                // use both variants of execute method: with and without listener
                if (randomBoolean()) {
                    clusterState = client.admin().cluster().state(clusterStateRequest).get().getState();
                } else {
                    PlainActionFuture<ClusterStateResponse> future = PlainActionFuture.newFuture();
                    client.admin().cluster().state(clusterStateRequest, future);
                    clusterState = future.get().getState();
                }
                assertThat(clusterState.nodes().getSize(), greaterThanOrEqualTo(size - j));
                assertThat(client.connectedNodes().size(), greaterThanOrEqualTo(size - j));
            }
        }
    }
}
