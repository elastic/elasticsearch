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

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportModule;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * Simple helper class to start external nodes to be used within a test cluster
 */
final class ExternalNode implements Closeable {

    public static final Settings REQUIRED_SETTINGS = Settings.builder()
            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
            .put(DiscoveryModule.DISCOVERY_TYPE_KEY, "zen")
            .put("node.mode", "network").build(); // we need network mode for this

    private final String version;
    private final Random random;
    private final NodeConfigurationSource nodeConfigurationSource;
    private final ExternalNodeServiceClient nodeServiceClient;
    private int pid;
    private NodeInfo nodeInfo;
    private final String clusterName;
    private TransportClient client;
    private String nodeName;

    private final ESLogger logger = Loggers.getLogger(getClass());
    private Settings externalNodeSettings;


    ExternalNode(String version, long seed, ExternalNodeServiceClient nodeServiceClient, NodeConfigurationSource nodeConfigurationSource) {
        this(version, null, seed, nodeServiceClient, nodeConfigurationSource);
    }

    ExternalNode(String version, String clusterName, long seed, ExternalNodeServiceClient nodeServiceClient, NodeConfigurationSource nodeConfigurationSource) {
        this.version = version;
        this.clusterName = clusterName;
        this.random = new Random(seed);
        this.nodeConfigurationSource = nodeConfigurationSource;
        this.nodeServiceClient = nodeServiceClient;
    }

    synchronized ExternalNode start(Client localNode, Settings defaultSettings, String nodeName, String clusterName, int nodeOrdinal) throws IOException, InterruptedException {
        ExternalNode externalNode = new ExternalNode(version, clusterName, random.nextLong(), nodeServiceClient, nodeConfigurationSource);
        Settings settings = Settings.builder().put(defaultSettings).put(nodeConfigurationSource.nodeSettings(nodeOrdinal)).build();
        externalNode.startInternal(localNode, settings, nodeName, clusterName);
        return externalNode;
    }

    synchronized void startInternal(Client client, Settings settings, String nodeName, String clusterName) throws IOException, InterruptedException {
        if (running()) {
            throw new IllegalStateException("Already started");
        }
        this.nodeName = nodeName;
        StringBuilder args = new StringBuilder();
        args.append(version);

        Settings.Builder externaNodeSettingsBuilder = Settings.builder();
        externaNodeSettingsBuilder.put("cluster.name", clusterName);
        externaNodeSettingsBuilder.put("node.name", nodeName);
        externaNodeSettingsBuilder.put("path.data", dataPath());
        externaNodeSettingsBuilder.put("path.logs", logPath());
        externaNodeSettingsBuilder.put("pidfile", pidPath());

        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            String found = externaNodeSettingsBuilder.get(entry.getKey());
            if (found != null) {
                logger.warn("Suppressing setting for external node [{}={}] because its being overridden to [{}]", entry.getKey(),
                        entry.getValue(), found);
                continue;
            }
            switch (entry.getKey()) {
            case "path.home":
            case "node.local":
            case "path.shared_data":
            case TransportModule.TRANSPORT_TYPE_KEY:
            case DiscoveryModule.DISCOVERY_TYPE_KEY:
            case TransportModule.TRANSPORT_SERVICE_TYPE_KEY:
            case InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING:
                logger.warn("Suppressing setting for external node [{}={}]", entry.getKey(), entry.getValue());
                continue;
            default:
                externaNodeSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        this.externalNodeSettings = externaNodeSettingsBuilder.put(REQUIRED_SETTINGS).build();
        for (Map.Entry<String, String> entry : externalNodeSettings.getAsMap().entrySet()) {
            args.append(" -Des.").append(entry.getKey()).append('=').append(entry.getValue());
        }
        String tcpPortSetting = externalNodeSettings.get("transport.tcp.port");
        if (tcpPortSetting == null) {
            throw new IllegalArgumentException(
                "Settings didn't contains tcp port range which can cause tests to interfere with each other.");
        } else {
            if (tcpPortSetting.matches("[23]\\d\\d\\d\\d-[23]\\d\\d\\d\\d") == false) {
                throw new IllegalArgumentException(
                        LoggerMessageFormat.format("Trying to run an external node with a port range not reserved for external nodes [{}].",
                                new Object[] { tcpPortSetting }));
            }
        }
        if (externalNodeSettings.get(UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS) == null) {
            throw new IllegalArgumentException("Without unicast hosts the external cluster isn't likely to work!");
        }



        boolean success = false;
        try {
            logger.info("starting external node [{}] with: {}", nodeName, args);
            pid = nodeServiceClient.start(args.toString());
            if (waitForNode(client, nodeName)) {
                nodeInfo = nodeInfo(client, nodeName);
                assert nodeInfo != null;
                logger.info("external node {} found, version [{}], build {}", nodeInfo.getNode(), nodeInfo.getVersion(), nodeInfo.getBuild());
            } else {
                logger.error("Node [{}] didn't join the cluster.", nodeName);
                errorLogNodeLog();
                throw new IllegalStateException("Node [" + nodeName + "] didn't join the cluster.");
            }
            success = true;
        } finally {
            if (!success) {
                stop();
            }
        }
    }

    static boolean waitForNode(final Client client, final String name) throws InterruptedException {
        return ESTestCase.awaitBusy(new Predicate<Object>() {
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
                    .put("path.home", PathUtils.get(".").toAbsolutePath())
                    .put(ClusterName.SETTING, clusterName).put("client.transport.sniff", false).build();
            TransportClient.Builder builder = TransportClient.builder().settings(clientSettings);
            for (Class<? extends Plugin> pluginClass: nodeConfigurationSource.transportClientPlugins()) {
                builder.addPlugin(pluginClass);
            }
            TransportClient client = builder.build();
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
            logger.warn("Stopping client for {}", nodeName);
            nodeServiceClient.stop(pid);
            pid = 0;
            if (client != null) {
                client.close();
            }
        }
    }

    public Path rootPath() {
        return PathUtils.get(System.getProperty("java.io.tmpdir"), "external", clusterName, nodeName).toAbsolutePath();
    }

    public Path dataPath() {
        return rootPath().resolve("data");
    }

    public Path logPath() {
        return rootPath().resolve("logs");
    }

    public Path pidPath() {
        return rootPath().resolve("pid");
    }

    synchronized boolean running() {
        return pid != 0;
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

    public void errorLogNodeLog() {
        ESLogger unformattedLogger = ESLoggerFactory.getLogger("test.external");
        try (BufferedReader log = Files.newBufferedReader(logPath().resolve(clusterName + ".log"), StandardCharsets.UTF_8)) {
            String line;
            while ((line = log.readLine()) != null) {
                unformattedLogger.error("[{}] {}", nodeName, line);
            }
        } catch (IOException e) {
            logger.error("IOException trying to read node's log!", e);
        }
    }
}
