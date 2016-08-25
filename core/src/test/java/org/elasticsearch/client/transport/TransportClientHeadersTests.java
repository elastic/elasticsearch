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

import org.elasticsearch.Version;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.AbstractClientHeadersTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class TransportClientHeadersTests extends AbstractClientHeadersTestCase {

    private static final LocalTransportAddress address = new LocalTransportAddress("test");

    @Override
    protected Client buildClient(Settings headersSettings, GenericAction[] testedActions) {
        TransportClient client = new MockTransportClient(Settings.builder()
                .put("client.transport.sniff", false)
                .put("cluster.name", "cluster1")
                .put("node.name", "transport_client_" + this.getTestName())
                .put(headersSettings)
                .build(), InternalTransportService.TestPlugin.class);

        client.addTransportAddress(address);
        return client;
    }

    public void testWithSniffing() throws Exception {
        try (TransportClient client = new MockTransportClient(
                Settings.builder()
                        .put("client.transport.sniff", true)
                        .put("cluster.name", "cluster1")
                        .put("node.name", "transport_client_" + this.getTestName() + "_1")
                        .put("client.transport.nodes_sampler_interval", "1s")
                        .put(HEADER_SETTINGS)
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(),
                InternalTransportService.TestPlugin.class)) {
            client.addTransportAddress(address);

            InternalTransportService service = (InternalTransportService) client.injector.getInstance(TransportService.class);

            if (!service.clusterStateLatch.await(5, TimeUnit.SECONDS)) {
                fail("takes way too long to get the cluster state");
            }

            assertThat(client.connectedNodes().size(), is(1));
            assertThat(client.connectedNodes().get(0).getAddress(), is((TransportAddress) address));
        }
    }

    public static class InternalTransportService extends TransportService {

        public static class TestPlugin extends Plugin {
            public void onModule(NetworkModule transportModule) {
                transportModule.registerTransportService("internal", InternalTransportService.class);
            }
            @Override
            public Settings additionalSettings() {
                return Settings.builder().put(NetworkModule.TRANSPORT_SERVICE_TYPE_KEY, "internal").build();
            }
        }

        CountDownLatch clusterStateLatch = new CountDownLatch(1);

        @Inject
        public InternalTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }

        @Override @SuppressWarnings("unchecked")
        public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request,
                                                              TransportRequestOptions options, TransportResponseHandler<T> handler) {
            if (TransportLivenessAction.NAME.equals(action)) {
                assertHeaders(threadPool);
                ((TransportResponseHandler<LivenessResponse>) handler).handleResponse(new LivenessResponse(clusterName, node));
                return;
            }
            if (ClusterStateAction.NAME.equals(action)) {
                assertHeaders(threadPool);
                ClusterName cluster1 = new ClusterName("cluster1");
                ClusterState.Builder builder = ClusterState.builder(cluster1);
                //the sniffer detects only data nodes
                builder.nodes(DiscoveryNodes.builder().add(new DiscoveryNode("node_id", address, Collections.emptyMap(),
                        Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT)));
                ((TransportResponseHandler<ClusterStateResponse>) handler)
                        .handleResponse(new ClusterStateResponse(cluster1, builder.build()));
                clusterStateLatch.countDown();
                return;
            }

            handler.handleException(new TransportException("", new InternalException(action)));
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            assertThat(node.getAddress(), equalTo(address));
            return true;
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
            assertThat(node.getAddress(), equalTo(address));
        }
    }
}
