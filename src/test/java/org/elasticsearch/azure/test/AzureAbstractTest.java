/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.azure.test;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cloud.azure.AzureComputeService;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class AzureAbstractTest {

    protected static ESLogger logger = ESLoggerFactory.getLogger(AzureAbstractTest.class.getName());
    private static List<Node> nodes;
    private Class<? extends AzureComputeService> mock;

    public AzureAbstractTest(Class<? extends AzureComputeService> mock) {
        // We want to inject the Azure API Mock
        this.mock = mock;
    }

    @Before
    public void setUp() {
        nodes = new ArrayList<Node>();

        File dataDir = new File("./target/es/data");
        if(dataDir.exists()) {
            FileSystemUtils.deleteRecursively(dataDir, true);
        }
    }

    @After
    public void tearDown() {
        // Cleaning nodes after test
        for (Node node : nodes) {
            node.close();
        }
    }

    protected Client getTransportClient() {
        // Create a TransportClient on node 1 and 2
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "azure")
                .put("transport.tcp.connect_timeout", "1s")
                .build();

        TransportClient client = new TransportClient(settings);

        for (Node node : nodes) {
            NettyTransport nettyTransport = ((InternalNode) node).injector().getInstance(NettyTransport.class);
            TransportAddress transportAddress = nettyTransport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
        }

        return client;
    }

    protected Client getNodeClient() {
        for (Node node : nodes) {
            return node.client();
        }

        return null;
    }

    protected void checkNumberOfNodes(int expected, boolean fail) {
        NodesInfoResponse nodeInfos = null;

        try {
            nodeInfos = getTransportClient().admin().cluster().prepareNodesInfo().execute().actionGet();
        } catch (NoNodeAvailableException e) {
            // If we can't build a Transport Client, we are may be not connected to any network
            // Let's try a Node Client
            nodeInfos = getNodeClient().admin().cluster().prepareNodesInfo().execute().actionGet();
        }

        Assert.assertNotNull(nodeInfos);
        Assert.assertNotNull(nodeInfos.getNodes());

        if (fail) {
            Assert.assertEquals(expected, nodeInfos.getNodes().length);
        } else {
            if (nodeInfos.getNodes().length != expected) {
                logger.warn("expected {} node(s) but found {}. Could be due to no local IP address available.",
                        expected, nodeInfos.getNodes().length);
            }
        }
    }

    protected void checkNumberOfNodes(int expected) {
        checkNumberOfNodes(expected, true);
    }

    protected void nodeBuilder() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                //.put("gateway.type", "local")
                .put("path.data", "./target/es/data")
                .put("path.logs", "./target/es/logs")
                .put("path.work", "./target/es/work")
//                .put("discovery.zen.ping.timeout", "500ms")
//                .put("discovery.zen.fd.ping_retries",1)
//                .put("discovery.zen.fd.ping_timeout", "500ms")
//                .put("discovery.initial_state_timeout", "5s")
//                .put("transport.tcp.connect_timeout", "1s")
                .put("cloud.azure.api.impl", mock)
                .put("cloud.azure.refresh_interval", "5s")
                .put("node.name", (nodes.size()+1) + "#" + mock.getSimpleName());
        Node node = NodeBuilder.nodeBuilder().settings(builder).node();
        nodes.add(node);
    }
}
