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

package org.elasticsearch.discovery.local;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.settings.Settings;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Sets.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class LocalDiscovery extends AbstractComponent implements Discovery {

    private final Lifecycle lifecycle = new Lifecycle();

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ClusterName clusterName;

    private Node localNode;

    private volatile boolean master = false;

    private volatile boolean firstMaster = false;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private static final ConcurrentMap<ClusterName, ClusterGroup> clusterGroups = newConcurrentMap();

    private static final AtomicLong nodeIdGenerator = new AtomicLong();

    @Inject public LocalDiscovery(Settings settings, ClusterName clusterName, TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public Discovery start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                clusterGroup = new ClusterGroup();
                clusterGroups.put(clusterName, clusterGroup);
            }
            logger.debug("Connected to cluster [{}]", clusterName);
            this.localNode = new Node(settings.get("name"), settings.getAsBoolean("node.data", true), Long.toString(nodeIdGenerator.incrementAndGet()), transportService.boundAddress().publishAddress());

            clusterGroup.members().add(this);
            if (clusterGroup.members().size() == 1) {
                // we are the first master (and the master)
                master = true;
                firstMaster = true;
                clusterService.submitStateUpdateTask("local-disco-initialconnect(master)", new ProcessedClusterStateUpdateTask() {
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
            } else {
                // we are not the master, tell the master to send it
                LocalDiscovery master = clusterGroup.members().peek();
                master.clusterService.submitStateUpdateTask("local-disco-receive(from node[" + localNode + "])", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        if (currentState.nodes().nodeExists(localNode.id())) {
                            // no change, the node already exists in the cluster
                            logger.warn("Received an address [{}] for an existing node [{}]", localNode.address(), localNode);
                            return currentState;
                        }
                        return newClusterStateBuilder().state(currentState).nodes(currentState.nodes().newNode(localNode)).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            }
        }
        return this;
    }

    @Override public Discovery stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                logger.warn("Illegal state, should not have an empty cluster group when stopping, I should be there at teh very least...");
                return this;
            }
            clusterGroup.members().remove(this);
            if (clusterGroup.members().isEmpty()) {
                // no more members, remove and return
                clusterGroups.remove(clusterName);
                return this;
            }

            final LocalDiscovery masterDiscovery = clusterGroup.members().peek();
            // if the removed node is the master, make the next one as the master
            if (master) {
                masterDiscovery.master = true;
            }

            final Set<String> newMembers = newHashSet();
            for (LocalDiscovery discovery : clusterGroup.members()) {
                newMembers.add(discovery.localNode.id());
            }

            masterDiscovery.clusterService.submitStateUpdateTask("local-disco-update", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    Nodes newNodes = currentState.nodes().removeDeadMembers(newMembers, masterDiscovery.localNode.id());
                    Nodes.Delta delta = newNodes.delta(currentState.nodes());
                    if (delta.added()) {
                        logger.warn("No new nodes should be created when a new discovery view is accepted");
                    }
                    return newClusterStateBuilder().state(currentState).nodes(newNodes).build();
                }
            });
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
    }

    @Override public void addListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.add(listener);
    }

    @Override public void removeListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.remove(listener);
    }

    @Override public String nodeDescription() {
        return clusterName.value() + "/" + localNode.id();
    }

    @Override public boolean firstMaster() {
        return firstMaster;
    }

    @Override public void publish(ClusterState clusterState) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        ClusterGroup clusterGroup = clusterGroups.get(clusterName);
        if (clusterGroup == null) {
            // nothing to publish to
            return;
        }
        try {
            // we do the marshaling intentionally, to check it works well...
            final byte[] clusterStateBytes = Builder.toBytes(clusterState);
            for (LocalDiscovery discovery : clusterGroup.members()) {
                if (discovery.master) {
                    continue;
                }
                final ClusterState nodeSpecificClusterState = ClusterState.Builder.fromBytes(clusterStateBytes, discovery.settings, discovery.localNode);
                // ignore cluster state messages that do not include "me", not in the game yet...
                if (nodeSpecificClusterState.nodes().localNode() != null) {
                    discovery.clusterService.submitStateUpdateTask("local-disco-receive(from master)", new ProcessedClusterStateUpdateTask() {
                        @Override public ClusterState execute(ClusterState currentState) {
                            return nodeSpecificClusterState;
                        }

                        @Override public void clusterStateProcessed(ClusterState clusterState) {
                            sendInitialStateEventIfNeeded();
                        }
                    });
                }
            }
        } catch (Exception e) {
            // failure to marshal or unmarshal
            throw new ElasticSearchIllegalStateException("Cluster state failed to serialize", e);
        }
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    private class ClusterGroup {

        private ConcurrentLinkedQueue<LocalDiscovery> members = new ConcurrentLinkedQueue<LocalDiscovery>();

        Queue<LocalDiscovery> members() {
            return members;
        }
    }
}
