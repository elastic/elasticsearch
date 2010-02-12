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
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryException;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.env.Environment;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.io.ByteArrayDataInputStream;
import org.elasticsearch.util.io.ByteArrayDataOutputStream;
import org.elasticsearch.util.io.HostResolver;
import org.elasticsearch.util.settings.Settings;
import org.jgroups.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.*;
import static com.google.common.collect.Sets.*;
import static org.elasticsearch.cluster.ClusterState.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JgroupsDiscovery extends AbstractComponent implements Discovery, Receiver {

    static {
        System.setProperty("jgroups.logging.log_factory_class", JgroupsCustomLogFactory.class.getName());
    }

    private final Lifecycle lifecycle = new Lifecycle();

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final Channel channel;

    private volatile boolean addressSet = false;

    private Node localNode;

    private volatile boolean firstMaster = false;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    @Inject public JgroupsDiscovery(Settings settings, Environment environment, ClusterName clusterName,
                                    TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.clusterName = clusterName;
        this.transportService = transportService;
        this.clusterService = clusterService;

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

    @Override public void addListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.add(listener);
    }

    @Override public void removeListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.remove(listener);
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public Discovery start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        try {
            channel.connect(clusterName.value());
            channel.setReceiver(this);
            logger.debug("Connected to cluster [{}], address [{}]", channel.getClusterName(), channel.getAddress());
            this.localNode = new Node(settings.get("name"), settings.getAsBoolean("node.data", true), channel.getAddress().toString(), transportService.boundAddress().publishAddress());

            if (isMaster()) {
                clusterService.submitStateUpdateTask("jgroups-disco-initialconnect(master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        Nodes.Builder builder = new Nodes.Builder()
                                .localNodeId(localNode.id())
                                .masterNodeId(localNode.id())
                                        // put our local node
                                .put(localNode);
                        return newClusterStateBuilder().state(currentState).nodes(builder).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
                firstMaster = true;
                addressSet = true;
            } else {
                clusterService.submitStateUpdateTask("jgroups-disco-initialconnect", new ClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        Nodes.Builder builder = new Nodes.Builder()
                                .localNodeId(localNode.id())
                                .put(localNode);
                        return newClusterStateBuilder().state(currentState).nodes(builder).build();
                    }
                });
                try {
                    channel.send(new Message(channel.getView().getCreator(), channel.getAddress(), nodeMessagePayload()));
                    addressSet = true;
                    logger.debug("Sent address [{}] to master [{}]", transportService.boundAddress().publishAddress(), channel.getView().getCreator());
                } catch (Exception e) {
                    logger.warn("Can't send address to master [" + channel.getView().getCreator() + "] will try again later...", e);
                }
            }
        } catch (ChannelException e) {
            throw new DiscoveryException("Can't connect to group [" + clusterName + "]", e);
        }
        return this;
    }

    @Override public Discovery stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        initialStateSent.set(false);
        if (channel.isConnected()) {
            channel.disconnect();
        }
        return this;
    }

    @Override public void close() throws DiscoveryException {
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

    public String nodeDescription() {
        return channel.getClusterName() + "/" + channel.getAddress();
    }

    @Override public boolean firstMaster() {
        return firstMaster;
    }

    @Override public void publish(ClusterState clusterState) {
        if (!isMaster()) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        try {
            channel.send(new Message(null, null, ClusterState.Builder.toBytes(clusterState)));
        } catch (Exception e) {
            logger.error("Failed to send cluster state to nodes", e);
        }
    }

    @Override public void receive(Message msg) {
        if (msg.getSrc().equals(channel.getAddress())) {
            return; // my own message, ignore.
        }

        // message from the master, the cluster state has changed.
        if (msg.getSrc().equals(channel.getView().getCreator())) {
            try {
                byte[] buffer = msg.getBuffer();
                final ClusterState clusterState = ClusterState.Builder.fromBytes(buffer, settings, localNode);
                // ignore cluster state messages that do not include "me", not in the game yet...
                if (clusterState.nodes().localNode() != null) {
                    clusterService.submitStateUpdateTask("jgroups-disco-receive(from master)", new ProcessedClusterStateUpdateTask() {
                        @Override public ClusterState execute(ClusterState currentState) {
                            return clusterState;
                        }

                        @Override public void clusterStateProcessed(ClusterState clusterState) {
                            sendInitialStateEventIfNeeded();
                        }
                    });
                }
            } catch (Exception e) {
                logger.error("Received corrupted cluster state.", e);
            }

            return;
        }

        // direct message from a member that indicate his state has changed.
        if (isMaster()) {
            try {
                ByteArrayDataInputStream is = new ByteArrayDataInputStream(msg.getBuffer());
                final Node newNode = Node.readNode(is);
                is.close();
                clusterService.submitStateUpdateTask("jgroups-disco-receive(from node[" + newNode + "])", new ClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        if (currentState.nodes().nodeExists(newNode.id())) {
                            // no change, the node already exists in the cluster
                            logger.warn("Received an address [{}] for an existing node [{}]", newNode.address(), newNode);
                            return currentState;
                        }
                        return newClusterStateBuilder().state(currentState).nodes(currentState.nodes().newNode(newNode)).build();
                    }
                });
            } catch (Exception e) {
                logger.warn("Can't read address from cluster member, message [" + msg.getClass().getName() + "/" + msg + "]", e);
            }

            return;
        }

        logger.error("A message between two members that neither of them is the master is not allowed.");
    }

    private boolean isMaster() {
        return channel.getAddress().equals(channel.getView().getCreator());
    }

    @Override public byte[] getState() {
        return new byte[0];
    }

    @Override public void setState(byte[] state) {
    }

    @Override public void viewAccepted(final View newView) {
        if (!addressSet) {
            try {
                channel.send(new Message(newView.getCreator(), channel.getAddress(), nodeMessagePayload()));
                logger.debug("Sent address [{}] to master [{}]", localNode.address(), newView.getCreator());
                addressSet = true;
            } catch (Exception e) {
                logger.warn("Can't send address to master [" + newView.getCreator() + "] will try again later...", e);
            }
        }
        // I am the master
        if (channel.getAddress().equals(newView.getCreator())) {
            final Set<String> newMembers = newHashSet();
            for (Address address : newView.getMembers()) {
                newMembers.add(address.toString());
            }

            clusterService.submitStateUpdateTask("jgroups-disco-view", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    Nodes newNodes = currentState.nodes().removeDeadMembers(newMembers, newView.getCreator().toString());
                    Nodes.Delta delta = newNodes.delta(currentState.nodes());
                    if (delta.added()) {
                        logger.warn("No new nodes should be created when a new discovery view is accepted");
                    }
                    // we want to send a new cluster state any how on view change (that's why its commented)
                    // for cases where we have client node joining (and it needs the cluster state)
//                    if (!delta.removed()) {
//                        // no nodes were removed, return the current state
//                        return currentState;
//                    }
                    return newClusterStateBuilder().state(currentState).nodes(newNodes).build();
                }
            });
        } else {
            // check whether I have been removed due to temporary disconnect
            final String me = channel.getAddress().toString();
            boolean foundMe = false;
            for (Node node : clusterService.state().nodes()) {
                if (node.id().equals(me)) {
                    foundMe = true;
                    break;
                }
            }

            if (!foundMe) {
                logger.warn("Disconnected from cluster, resending address [{}] to master [{}]", localNode.address(), newView.getCreator());
                try {
                    channel.send(new Message(newView.getCreator(), channel.getAddress(), nodeMessagePayload()));
                    addressSet = true;
                } catch (Exception e) {
                    addressSet = false;
                    logger.warn("Can't send address to master [" + newView.getCreator() + "] will try again later...", e);
                }
            }
        }
    }

    private byte[] nodeMessagePayload() throws IOException {
        ByteArrayDataOutputStream os = new ByteArrayDataOutputStream();
        localNode.writeTo(os);
        os.close();
        return os.unsafeByteArray();
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }


    @Override public void suspect(Address suspectedMember) {
    }

    @Override public void block() {
        logger.warn("Blocked...");
    }
}
