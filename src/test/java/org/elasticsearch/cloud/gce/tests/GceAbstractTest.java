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
package org.elasticsearch.cloud.gce.tests;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.common.io.FileSystemUtils;
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

public abstract class GceAbstractTest {

    private static List<Node> nodes;
    private Class<? extends GceComputeService> mock;

    public GceAbstractTest(Class<? extends GceComputeService> mock) {
        // We want to inject the GCE API Mock
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

    protected Client getClient() {
        // Create a TransportClient on node 1 and 2
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "gce").build();

        TransportClient client = new TransportClient(settings);

        for (Node node : nodes) {
            NettyTransport nettyTransport = ((InternalNode) node).injector().getInstance(NettyTransport.class);
            TransportAddress transportAddress = nettyTransport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
        }

        return client;
    }

    protected void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = getClient().admin().cluster().prepareNodesInfo().execute().actionGet();

        Assert.assertNotNull(nodeInfos.getNodes());
        Assert.assertEquals(expected, nodeInfos.getNodes().length);
    }

    protected void nodeBuilder(String filteredTags) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                //.put("gateway.type", "local")
                .put("path.data", "./target/es/data")
                .put("path.logs", "./target/es/logs")
                .put("path.work", "./target/es/work")
                .put("cloud.gce.api.impl", mock)
                .put("node.name", (nodes.size()+1) + "#" + mock.getSimpleName());
        if (filteredTags != null) {
            builder.put("discovery.gce.tags", filteredTags);
        } else {
            builder.put("discovery.gce.tags", "");
        }

        Node node = NodeBuilder.nodeBuilder().settings(builder).node();
        nodes.add(node);
    }
}
