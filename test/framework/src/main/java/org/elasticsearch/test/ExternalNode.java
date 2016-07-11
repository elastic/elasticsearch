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

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Simple helper class to start external nodes to be used within a test cluster
 */
final class ExternalNode implements Closeable {

    public static final Settings REQUIRED_SETTINGS = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "zen")
            .put(Node.NODE_MODE_SETTING.getKey(), "network").build(); // we need network mode for this

    private final Path path;
    private final Random random;
    private final NodeConfigurationSource nodeConfigurationSource;
    private Process process;
    private NodeInfo nodeInfo;
    private final String clusterName;
    private TransportClient client;

    private final ESLogger logger = Loggers.getLogger(getClass());
    private Settings externalNodeSettings;


    ExternalNode(Path path, long seed, NodeConfigurationSource nodeConfigurationSource) {
        this(path, null, seed, nodeConfigurationSource);
    }

    ExternalNode(Path path, String clusterName, long seed, NodeConfigurationSource nodeConfigurationSource) {
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("path must be a directory");
        }
        this.path = path;
        this.clusterName = clusterName;
        this.random = new Random(seed);
        this.nodeConfigurationSource = nodeConfigurationSource;
    }

    synchronized ExternalNode start(Client localNode, Settings defaultSettings, String nodeName, String clusterName, int nodeOrdinal) throws IOException, InterruptedException {
        ExternalNode externalNode = new ExternalNode(path, clusterName, random.nextLong(), nodeConfigurationSource);
        Settings settings = Settings.builder().put(defaultSettings).put(nodeConfigurationSource.nodeSettings(nodeOrdinal)).build();
        externalNode.startInternal(localNode, settings, nodeName, clusterName);
        return externalNode;
    }

    @SuppressForbidden(reason = "needs java.io.File api to start a process")
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
        params.add("-Ecluster.name=" + clusterName);
        params.add("-Enode.name=" + nodeName);
        Settings.Builder externaNodeSettingsBuilder = Settings.builder();
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            switch (entry.getKey()) {
                case "cluster.name":
                case "node.name":
                case "path.home":
                case "node.mode":
                case "node.local":
                case NetworkModule.TRANSPORT_TYPE_KEY:
                case "discovery.type":
                case NetworkModule.TRANSPORT_SERVICE_TYPE_KEY:
                case "config.ignore_system_properties":
                    continue;
                default:
                    externaNodeSettingsBuilder.put(entry.getKey(), entry.getValue());

            }
        }
        this.externalNodeSettings = externaNodeSettingsBuilder.put(REQUIRED_SETTINGS).build();
        for (Map.Entry<String, String> entry : externalNodeSettings.getAsMap().entrySet()) {
            params.add("-E" + entry.getKey() + "=" + entry.getValue());
        }

        params.add("-Epath.home=" + PathUtils.get(".").toAbsolutePath());
        params.add("-Epath.conf=" + path + "/config");

        ProcessBuilder builder = new ProcessBuilder(params);
        builder.directory(path.toFile());
        builder.inheritIO();
        boolean success = false;
        try {
            logger.info("starting external node [{}] with: {}", nodeName, builder.command());
            process = builder.start();
            this.nodeInfo = null;
            if (waitForNode(client, nodeName)) {
                nodeInfo = nodeInfo(client, nodeName);
                assert nodeInfo != null;
                logger.info("external node {} found, version [{}], build {}", nodeInfo.getNode(), nodeInfo.getVersion(), nodeInfo.getBuild());
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
        return ESTestCase.awaitBusy(() -> {
            final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
            for (NodeInfo info : nodeInfos.getNodes()) {
                if (name.equals(info.getNode().getName())) {
                    return true;
                }
            }
            return false;
        }, 30, TimeUnit.SECONDS);
    }

    static NodeInfo nodeInfo(final Client client, final String nodeName) {
        final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        for (NodeInfo info : nodeInfos.getNodes()) {
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

    synchronized Client getClient() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        if (client == null) {
            TransportAddress addr = nodeInfo.getTransport().getAddress().publishAddress();
            // verify that the end node setting will have network enabled.

            Settings clientSettings = Settings.builder().put(externalNodeSettings)
                    .put("client.transport.nodes_sampler_interval", "1s")
                    .put("node.name", "transport_client_" + nodeInfo.getNode().getName())
                    .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName).put("client.transport.sniff", false).build();
            TransportClient client = TransportClient.builder().settings(clientSettings).build();
            client.addTransportAddress(addr);
            this.client = client;
        }
        return client;
    }

    synchronized void reset(long seed) {
        this.random.setSeed(seed);
    }

    synchronized void stop() throws InterruptedException {
        if (running()) {
            try {
                if (this.client != null) {
                    client.close();
                }
            } finally {
                process.destroy();
                process.waitFor();
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
        try {
            stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    synchronized String getName() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        return nodeInfo.getNode().getName();
    }
}
