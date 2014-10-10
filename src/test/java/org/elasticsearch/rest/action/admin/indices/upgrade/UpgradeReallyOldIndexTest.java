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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Before;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpgradeReallyOldIndexTest extends ElasticsearchTestCase {
    Node node;
    
    @Before
    public void setup() {
        
    }
    
    public void test() throws Exception {
        File dataDir = newTempDir();
        File oldIndex = new File(getClass().getResource("index-0.20.zip").toURI());
        TestUtil.unzip(oldIndex, dataDir);
        
        
        
        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        nodeBuilder.clusterName("oldindexcluster");
        Settings settings = ImmutableSettings.builder()
            .put("path.data", dataDir.getPath())
            .put("node.mode", "network")
            .put(InternalNode.HTTP_ENABLED, true)
            .build();
        Node node = nodeBuilder.settings(settings).node();
        
        try {
            node.start();
            NodeInfo info = nodeInfo(node.client());
            info.getHttp().address().boundAddress();
            TransportAddress publishAddress = info.getTransport().address().publishAddress();
            assertEquals(1, publishAddress.uniqueAddressTypeId());
            InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
            HttpRequestBuilder httpClient = new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
            HttpResponse rsp = httpClient.method("GET").path("/_upgrade").addParam("pretty", "true").execute();
            System.out.println("RESPONSE: \n" + rsp.getBody());
        } finally {
            node.stop();
        }
    }

    static NodeInfo nodeInfo(final Client client) {
        final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        assertEquals(1, nodes.length);
        return nodes[0];
    }

    /*HttpRequestBuilder httpClient(Node ) {
        InetSocketAddress[] addresses = cluster().httpAddresses();
        InetSocketAddress address = addresses[randomInt(addresses.length - 1)];
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }*/
}
