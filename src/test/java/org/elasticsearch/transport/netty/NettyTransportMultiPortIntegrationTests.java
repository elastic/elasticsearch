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
package org.elasticsearch.transport.netty;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.transport.TransportModule;
import org.junit.Test;

import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class NettyTransportMultiPortIntegrationTests extends ElasticsearchIntegrationTest {

    private static int randomPort = -1;
    private static String randomPortRange;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (randomPort == -1) {
            randomPort = randomIntBetween(49152, 65525);
            randomPortRange = String.format(Locale.ROOT, "%s-%s", randomPort, randomPort+10);
        }
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("network.host", "127.0.0.1")
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettyTransport.class.getName())
                .put("node.mode", "network")
                .put("transport.profiles.client1.port", randomPortRange)
                .put("transport.profiles.client1.publish_host", "127.0.0.7")
                .put("transport.profiles.client1.publish_port", "4321")
                .put("transport.profiles.client1.reuse_address", true)
                .build();
    }

    @Test
    public void testThatTransportClientCanConnect() throws Exception {
        Settings settings = settingsBuilder()
                .put("cluster.name", internalCluster().getClusterName())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettyTransport.class.getName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", randomPort));
            ClusterHealthResponse response = transportClient.admin().cluster().prepareHealth().get();
            assertThat(response.getStatus(), is(ClusterHealthStatus.GREEN));
        }
    }

    @Test
    @Network
    public void testThatInfosAreExposed() throws Exception {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setTransport(true).get();
        for (NodeInfo nodeInfo : response.getNodes()) {
            assertThat(nodeInfo.getTransport().getProfileAddresses().keySet(), hasSize(1));
            assertThat(nodeInfo.getTransport().getProfileAddresses(), hasKey("client1"));
            assertThat(nodeInfo.getTransport().getProfileAddresses().get("client1").boundAddress(), instanceOf(InetSocketTransportAddress.class));

            // bound address
            InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) nodeInfo.getTransport().getProfileAddresses().get("client1").boundAddress();
            assertThat(inetSocketTransportAddress.address().getPort(), is(allOf(greaterThanOrEqualTo(randomPort), lessThanOrEqualTo(randomPort + 10))));

            // publish address
            assertThat(nodeInfo.getTransport().getProfileAddresses().get("client1").publishAddress(), instanceOf(InetSocketTransportAddress.class));
            InetSocketTransportAddress publishAddress = (InetSocketTransportAddress) nodeInfo.getTransport().getProfileAddresses().get("client1").publishAddress();
            assertThat(publishAddress.address().getHostName(), is("127.0.0.7"));
            assertThat(publishAddress.address().getPort(), is(4321));
        }
    }
}
