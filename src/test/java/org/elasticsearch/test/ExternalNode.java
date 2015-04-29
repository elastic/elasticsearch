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
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.transport.TransportModule;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Simple helper class to start external nodes to be used within a test cluster
 */
final class ExternalNode implements Closeable {

    public static final Settings REQUIRED_SETTINGS = ImmutableSettings.builder()
            .put("config.ignore_system_properties", true)
            .put(DiscoveryModule.DISCOVERY_TYPE_KEY, "zen")
            .put("node.mode", "network").build(); // we need network mode for this

    private final Path path;
    private final Random random;
    private final SettingsSource settingsSource;
    private Process process;
    private NodeInfo nodeInfo;
    private final String clusterName;
    private TransportClient client;

    private final ESLogger logger = Loggers.getLogger(getClass());
    private Settings externalNodeSettings;


    ExternalNode(Path path, long seed, SettingsSource settingsSource) {
        this(path, null, seed, settingsSource);
    }

    ExternalNode(Path path, String clusterName, long seed, SettingsSource settingsSource) {
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("path must be a directory");
        }
        this.path = path;
        this.clusterName = clusterName;
        this.random = new Random(seed);
        this.settingsSource = settingsSource;
    }

    synchronized ExternalNode start(Client localNode, Settings defaultSettings, String nodeName, String clusterName, int nodeOrdinal) throws IOException, InterruptedException {
        ExternalNode externalNode = new ExternalNode(path, clusterName, random.nextLong(), settingsSource);
        Settings settings = ImmutableSettings.builder().put(defaultSettings).put(settingsSource.node(nodeOrdinal)).build();
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
        ImmutableSettings.Builder externaNodeSettingsBuilder = ImmutableSettings.builder();
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            switch (entry.getKey()) {
                case "cluster.name":
                case "node.name":
                case "path.home":
                case "node.mode":
                case "node.local":
                case TransportModule.TRANSPORT_TYPE_KEY:
                case DiscoveryModule.DISCOVERY_TYPE_KEY:
                case TransportModule.TRANSPORT_SERVICE_TYPE_KEY:
                case "config.ignore_system_properties":
                    continue;
                default:
                    externaNodeSettingsBuilder.put(entry.getKey(), entry.getValue());

            }
        }
        this.externalNodeSettings = externaNodeSettingsBuilder.put(REQUIRED_SETTINGS).build();
        for (Map.Entry<String, String> entry : externalNodeSettings.getAsMap().entrySet()) {
            params.add("-Des." + entry.getKey() + "=" + entry.getValue());
        }

        params.add("-Des.path.home=" + PathUtils.get(".").toAbsolutePath());
        params.add("-Des.path.conf=" + path + "/config");

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
        }, 30, TimeUnit.SECONDS);
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

    synchronized Client getClient() {
        if (nodeInfo == null) {
            throw new IllegalStateException("Node has not started yet");
        }
        if (client == null) {
            TransportAddress addr = nodeInfo.getTransport().getAddress().publishAddress();
            // verify that the end node setting will have network enabled.

            Settings clientSettings = settingsBuilder().put(externalNodeSettings)
                    .put("client.transport.nodes_sampler_interval", "1s")
                    .put("name", "transport_client_" + nodeInfo.getNode().name())
                    .put(ClusterName.SETTING, clusterName).put("client.transport.sniff", false).build();
            TransportClient client = new TransportClient(clientSettings);
            client.addTransportAddress(addr);
            this.client = client;
        }
        return client;
    }

    synchronized void reset(long seed) {
        this.random.setSeed(seed);
    }

    synchronized void stop() {
        if (running()) {
            try {
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
