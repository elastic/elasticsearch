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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 1.0)
public class TransportClientIT extends ESIntegTestCase {

    public void testPickingUpChangesInDiscoveryNode() {
        String nodeName = internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false));

        TransportClient client = (TransportClient) internalCluster().client(nodeName);
        assertThat(client.connectedNodes().get(0).isDataNode(), equalTo(false));

    }

    public void testNodeVersionIsUpdated() throws IOException, NodeValidationException {
        TransportClient client = (TransportClient)  internalCluster().client();
        try (Node node = new MockNode(Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put("node.name", "testNodeVersionIsUpdated")
                .put("transport.type", getTestTransportType())
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .put("cluster.name", "foobar")
                .build(), Arrays.asList(getTestTransportPlugin(), TestZenDiscovery.TestPlugin.class)).start()) {
            TransportAddress transportAddress = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
            // since we force transport clients there has to be one node started that we connect to.
            assertThat(client.connectedNodes().size(), greaterThanOrEqualTo(1));
            // connected nodes have updated version
            for (DiscoveryNode discoveryNode : client.connectedNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT));
            }

            for (DiscoveryNode discoveryNode : client.listedNodes()) {
                assertThat(discoveryNode.getId(), startsWith("#transport#-"));
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }

            assertThat(client.filteredNodes().size(), equalTo(1));
            for (DiscoveryNode discoveryNode : client.filteredNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }
        }
    }

    public void testThatTransportClientSettingIsSet() {
        TransportClient client = (TransportClient)  internalCluster().client();
        Settings settings = client.injector.getInstance(Settings.class);
        assertThat(Client.CLIENT_TYPE_SETTING_S.get(settings), is("transport"));
    }

    public void testThatTransportClientSettingCannotBeChanged() {
        String transport = getTestTransportType();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), transport)
            .build();
        try (TransportClient client = new MockTransportClient(baseSettings)) {
            Settings settings = client.injector.getInstance(Settings.class);
            assertThat(Client.CLIENT_TYPE_SETTING_S.get(settings), is("transport"));
        }
    }
}
