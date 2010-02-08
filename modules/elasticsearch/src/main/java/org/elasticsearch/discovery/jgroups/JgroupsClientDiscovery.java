/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.discovery.jgroups;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryException;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.io.HostResolver;
import org.elasticsearch.util.settings.Settings;
import org.jgroups.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.node.Nodes.*;

/**
 * A simplified discovery implementation based on JGroups that only works in client mode.
 *
 * @author kimchy (Shay Banon)
 */
public class JgroupsClientDiscovery extends AbstractComponent implements Discovery, Receiver {

    private final Lifecycle lifecycle = new Lifecycle();

    private final ClusterName clusterName;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final Channel channel;

    private volatile ScheduledFuture reconnectFuture;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private final Node localNode = new Node("#client#", null); // dummy local node

    @Inject public JgroupsClientDiscovery(Settings settings, Environment environment, ClusterName clusterName, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        String config = componentSettings.get("config", "udp");
        String actualConfig = config;
        if (!config.endsWith(".xml")) {
            actualConfig = "jgroups/" + config + ".xml";
        }
        URL configUrl = environment.resolveConfig(actualConfig);
        logger.debug("Using configuration [{}]", configUrl);

        Map<String, String> sysPropsSet = newHashMap();
        try {
            // prepare system properties to configure jgroups based on the settings
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("discovery.jgroups")) {
                    String jgroupsKey = entry.getKey().substring("discovery.".length());
                    if (System.getProperty(jgroupsKey) == null) {
                        sysPropsSet.put(jgroupsKey, entry.getValue());
                        System.setProperty(jgroupsKey, entry.getValue());
                    }
                }
            }

            if (System.getProperty("jgroups.bind_addr") == null) {
                // automatically set the bind address based on ElasticSearch default bindings...
                try {
                    InetAddress bindAddress = HostResolver.resultBindHostAddress(null, settings, HostResolver.LOCAL_IP);
                    if ((bindAddress instanceof Inet4Address && HostResolver.isIPv4()) || (bindAddress instanceof Inet6Address && !HostResolver.isIPv4())) {
                        sysPropsSet.put("jgroups.bind_addr", bindAddress.getHostAddress());
                        System.setProperty("jgroups.bind_addr", bindAddress.getHostAddress());
                    }
                } catch (IOException e) {
                    // ignore this
                }
            }

            channel = new JChannel(configUrl);
        } catch (ChannelException e) {
            throw new DiscoveryException("Failed to create jgroups channel with config [" + configUrl + "]", e);
        } finally {
            for (String keyToRemove : sysPropsSet.keySet()) {
                System.getProperties().remove(keyToRemove);
            }
        }
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public Discovery start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        channel.setReceiver(this);
        try {
            channel.connect(clusterName.value());
        } catch (ChannelException e) {
            throw new DiscoveryException("Failed to connect to cluster [" + clusterName.value() + "]", e);
        }
        connectTillMasterIfNeeded();
        sendInitialStateEventIfNeeded();
        return this;
    }

    @Override public Discovery stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        if (reconnectFuture != null) {
            reconnectFuture.cancel(true);
            reconnectFuture = null;
        }
        if (channel.isConnected()) {
            channel.disconnect();
        }
        return this;
    }

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        if (channel.isOpen()) {
            channel.close();
        }
    }

    @Override public void addListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.add(listener);
    }

    @Override public void removeListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.remove(listener);
    }

    @Override public void receive(Message msg) {
        if (msg.getSrc().equals(channel.getAddress())) {
            return; // my own message, ignore.
        }
        if (msg.getSrc().equals(channel.getView().getCreator())) {
            try {
                byte[] buffer = msg.getBuffer();
                final ClusterState origClusterState = ClusterState.Builder.fromBytes(buffer, settings, localNode);
                // remove the dummy local node
                final ClusterState clusterState = newClusterStateBuilder().state(origClusterState)
                        .nodes(newNodesBuilder().putAll(origClusterState.nodes()).remove(localNode.id())).build();
                System.err.println("Nodes: " + clusterState.nodes().prettyPrint());
                clusterService.submitStateUpdateTask("jgroups-disco-receive(from master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        return clusterState;
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } catch (Exception e) {
                logger.error("Received corrupted cluster state.", e);
            }
        }
    }

    @Override public void viewAccepted(View newView) {
        // we became master, reconnect
        if (channel.getAddress().equals(newView.getCreator())) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                // ignore
            }
            if (!lifecycle.started()) {
                return;
            }
            connectTillMasterIfNeeded();
        }
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    @Override public String nodeDescription() {
        return "clientNode";
    }

    @Override public void publish(ClusterState clusterState) {
        throw new ElasticSearchIllegalStateException("When in client mode, cluster state should not be published");
    }

    @Override public boolean firstMaster() {
        return false;
    }

    @Override public byte[] getState() {
        return new byte[0];
    }

    @Override public void setState(byte[] state) {
    }

    @Override public void suspect(Address suspectedMember) {
    }

    @Override public void block() {
        logger.warn("Blocked...");
    }

    private void connectTillMasterIfNeeded() {
        Runnable command = new Runnable() {
            @Override public void run() {
                try {
                    channel.connect(clusterName.value());
                    if (isMaster()) {
                        logger.debug("Act as master, reconnecting...");
                        channel.disconnect();
                        reconnectFuture = threadPool.schedule(this, 3, TimeUnit.SECONDS);
                    } else {
                        logger.debug("Reconnected not as master");
                        reconnectFuture = null;
                    }
                } catch (Exception e) {
                    logger.warn("Failed to connect to cluster", e);
                }
            }
        };

        if (channel.isConnected()) {
            if (!isMaster()) {
                logger.debug("Connected not as master");
                return;
            }
            channel.disconnect();
        }
        reconnectFuture = threadPool.schedule(command, 3, TimeUnit.SECONDS);
    }

    private boolean isMaster() {
        return channel.getAddress().equals(channel.getView().getCreator());
    }
}
