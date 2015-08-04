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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 1.0)
public class TransportClientIT extends ESIntegTestCase {

    @Test
    public void testPickingUpChangesInDiscoveryNode() {
        String nodeName = internalCluster().startNode(Settings.builder().put("node.data", false));

        TransportClient client = (TransportClient) internalCluster().client(nodeName);
        assertThat(client.connectedNodes().get(0).dataNode(), equalTo(false));

    }

    @Test
    public void testNodeVersionIsUpdated() {
        TransportClient client = (TransportClient)  internalCluster().client();
        TransportClientNodesService nodeService = client.nodeService();
        Node node = nodeBuilder().data(false).settings(Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put("path.home", createTempDir())
                .put("node.name", "testNodeVersionIsUpdated")
                .put("http.enabled", false)
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true) // make sure we get what we set :)
                .build()).clusterName("foobar").build();
        node.start();
        try {
            TransportAddress transportAddress = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
            assertThat(nodeService.connectedNodes().size(), greaterThanOrEqualTo(1)); // since we force transport clients there has to be one node started that we connect to.
            for (DiscoveryNode discoveryNode : nodeService.connectedNodes()) {  // connected nodes have updated version
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT));
            }

            for (DiscoveryNode discoveryNode : nodeService.listedNodes()) {
                assertThat(discoveryNode.id(), startsWith("#transport#-"));
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }

            assertThat(nodeService.filteredNodes().size(), equalTo(1));
            for (DiscoveryNode discoveryNode : nodeService.filteredNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }
        } finally {
            node.close();
        }
    }

    @Test
    public void testThatTransportClientSettingIsSet() {
        TransportClient client = (TransportClient)  internalCluster().client();
        Settings settings = client.injector.getInstance(Settings.class);
        assertThat(settings.get(Client.CLIENT_TYPE_SETTING), is("transport"));
    }

    @Test
    public void testThatTransportClientSettingCannotBeChanged() {
        Settings baseSettings = settingsBuilder().put(Client.CLIENT_TYPE_SETTING, "anything").put("path.home", createTempDir()).build();
        try (TransportClient client = TransportClient.builder().settings(baseSettings).build()) {
            Settings settings = client.injector.getInstance(Settings.class);
            assertThat(settings.get(Client.CLIENT_TYPE_SETTING), is("transport"));
        }
    }
}
