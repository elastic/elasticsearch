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

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.AbstractClientHeadersTests;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class TransportClientHeadersTests extends AbstractClientHeadersTests {

    private static final LocalTransportAddress address = new LocalTransportAddress("test");

    @Override
    protected Client buildClient(Settings headersSettings, GenericAction[] testedActions) {
        TransportClient client = new TransportClient(ImmutableSettings.builder()
                .put("client.transport.sniff", false)
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, InternalTransportService.class.getName())
                .put(HEADER_SETTINGS)
                .build());

        client.addTransportAddress(address);
        return client;
    }

    public static class InternalTransportService extends TransportService {

        @Inject
        public InternalTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }

        @Override @SuppressWarnings("unchecked")
        public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) {
            if (NodesInfoAction.NAME.equals(action)) {
                ((TransportResponseHandler<NodesInfoResponse>) handler).handleResponse(new NodesInfoResponse(ClusterName.DEFAULT, new NodeInfo[0]));
                return;
            }
            handler.handleException(new TransportException("", new InternalException(action, request)));
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            assertThat((LocalTransportAddress) node.getAddress(), equalTo(address));
            return true;
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
            assertThat((LocalTransportAddress) node.getAddress(), equalTo(address));
        }
    }

}
