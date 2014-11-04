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
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, enableRandomBenchNodes = false)
public class NettyTransportMultiPortIntegrationTests extends ElasticsearchIntegrationTest {

    private static int randomPort = -1;
    private static String randomPortRange;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (randomPort == -1) {
            randomPort = randomIntBetween(1025, 65000);
            randomPortRange = String.format(Locale.ROOT, "%s-%s", randomPort, randomPort+10);
        }
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("network.host", "127.0.0.1")
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettyTransport.class.getName())
                .put("node.mode", "network")
                .put("transport.profiles.client1.port", randomPortRange)
                .put("transport.profiles.client1.reuse_address", true)
                .build();
    }

    @Test
    @Network
    public void testThatTransportClientCanConnect() throws Exception {
        Settings settings = settingsBuilder()
                .put("cluster.name", internalCluster().getClusterName())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettyTransport.class.getName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", randomPort));
            ClusterHealthResponse response = transportClient.admin().cluster().prepareHealth().get();
            assertThat(response.getStatus(), is(ClusterHealthStatus.GREEN));
        }
    }
}
