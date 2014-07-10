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
package org.elasticsearch.test;

import com.google.common.base.Predicate;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Simple helper class to start external nodes to be used within a test cluster
 */
final class ExternalNode implements Closeable {

    private final File path;
    private final Random random;
    private final NodeSettingsSource nodeSettingsSource;
    private Process process;
    private NodeInfo nodeInfo;
    private final String clusterName;
    private TransportClient client;

    ExternalNode(File path, long seed, NodeSettingsSource nodeSettingsSource) {
        this(path, null, seed, nodeSettingsSource);
    }

    ExternalNode(File path, String clusterName, long seed, NodeSettingsSource nodeSettingsSource) {
        if (!path.isDirectory()) {
            throw new IllegalArgumentException("path must be a directory");
        }
        this.path = path;
        this.clusterName = clusterName;
        this.random = new Random(seed);
        this.nodeSettingsSource = nodeSettingsSource;
    }

    synchronized ExternalNode start(Client localNode, Settings defaultSettings, String nodeName, String clusterName, int nodeOrdinal) throws IOException, InterruptedException {
        ExternalNode externalNode = new ExternalNode(path, clusterName, random.nextLong(), nodeSettingsSource);
        Settings settings = ImmutableSettings.builder().put(nodeSettingsSource.settings(nodeOrdinal)).put(defaultSettings).build();
        externalNode.startInternal(localNode, settings, nodeName, clusterName);
        return externalNode;
    }

    synchronized void startInternal(Client client, Settings settings, String nodeName, String clusterName) throws IOException, InterruptedException {
        if (process != null) {
            throw new IllegalStateException("Already started");
        }
        List<String> params = new ArrayList<>();

        if (!Constants.WINDOWS) {
            params.add("bin/elasticsearch");
        } else {
            params.add("bin/elasticsearch.bat");
        }
        params.add("-Des.cluster.name=" + clusterName);
        params.add("-Des.node.name=" + nodeName);
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            switch (entry.getKey()) {
                case "cluster.name":
                case "node.name":
                case "path.home":
                case "node.mode":
                case "gateway.type":
                case "config.ignore_system_properties":
                    continue;
                default:
                    params.add("-Des." + entry.getKey() + "=" + entry.getValue());

            }
        }

        params.add("-Des.gateway.type=local");
        params.add("-Des.path.home=" + new File("").getAbsolutePath());
        ProcessBuilder builder = new ProcessBuilder(params);
        builder.directory(path);
        builder.inheritIO();
        boolean success = false;
        try {
            process = builder.start();
            this.nodeInfo = null;
            if (waitForNode(client, nodeName)) {
                nodeInfo = nodeInfo(client, nodeName);
                assert nodeInfo != null;
            } else {
                throw new IllegalStateException("Node [" + nodeName + "] didn't join the cluster");
            }
            success = true;
        } finally {
            if (!success) {
                stop();
            }
        }
    }

    static boolean waitForNode(final Client client, final String name) throws InterruptedException {
        return ElasticsearchTestCase.awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
                final NodeInfo[] nodes = nodeInfos.getNodes();
                for (NodeInfo info : nodes) {
                    if (name.equals(info.getNode().getName())) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    static NodeInfo nodeInfo(final Client client, final String nodeName) {
        final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        for (NodeInfo info : nodes) {
            if (nodeName.equals(info.getNode().getName())) {
                return info;
            }
        }
        return null;
    }

    synchronized TransportAddress getTransportAddress() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        return nodeInfo.getTransport().getAddress().publishAddress();
    }

    TransportAddress address() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        return nodeInfo.getTransport().getAddress().publishAddress();
    }

    synchronized Client getClient() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        if (client == null) {
            TransportAddress addr = nodeInfo.getTransport().getAddress().publishAddress();
            TransportClient client = new TransportClient(settingsBuilder().put("client.transport.nodes_sampler_interval", "1s")
                    .put("name", "transport_client_" + nodeInfo.getNode().name())
                    .put(ClusterName.SETTING, clusterName).put("client.transport.sniff", false).build());
            client.addTransportAddress(addr);
            this.client = client;
        }
        return client;
    }

    synchronized void reset(long seed) {
        this.random.setSeed(seed);
    }

    synchronized void stop() {
        stop(false);
    }

    synchronized void stop(boolean forceKill) {
        if (running()) {
            try {
                if (forceKill == false && nodeInfo != null && random.nextBoolean()) {
                    // sometimes shut down gracefully
                    getClient().admin().cluster().prepareNodesShutdown(this.nodeInfo.getNode().id()).setExit(random.nextBoolean()).setDelay("0s").get();
                }
                if (this.client != null) {
                    client.close();
                }
            } finally {
                process.destroy();
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
                process = null;
                nodeInfo = null;

            }
        }
    }


    synchronized boolean running() {
        return process != null;
    }

    @Override
    public void close() {
        stop();
    }

    synchronized String getName() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        return nodeInfo.getNode().getName();
    }
}
