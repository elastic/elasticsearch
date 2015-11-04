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

package org.elasticsearch.discovery.local;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.*;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterState.Builder;

/**
 *
 */
public class LocalDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery {

    private static final LocalDiscovery[] NO_MEMBERS = new LocalDiscovery[0];

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final DiscoveryNodeService discoveryNodeService;
    private RoutingService routingService;
    private final ClusterName clusterName;
    private final Version version;

    private final DiscoverySettings discoverySettings;

    private DiscoveryNode localNode;

    private volatile boolean master = false;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<>();

    private static final ConcurrentMap<ClusterName, ClusterGroup> clusterGroups = ConcurrentCollections.newConcurrentMap();

    private volatile ClusterState lastProcessedClusterState;

    @Inject
    public LocalDiscovery(Settings settings, ClusterName clusterName, TransportService transportService, ClusterService clusterService,
                          DiscoveryNodeService discoveryNodeService, Version version, DiscoverySettings discoverySettings) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.discoveryNodeService = discoveryNodeService;
        this.version = version;
        this.discoverySettings = discoverySettings;
    }

    @Override
    public void setNodeService(@Nullable NodeService nodeService) {
        // nothing to do here
    }

    @Override
    public void setRoutingService(RoutingService routingService) {
        this.routingService = routingService;
    }

    @Override
    protected void doStart() {
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                clusterGroup = new ClusterGroup();
                clusterGroups.put(clusterName, clusterGroup);
            }
            logger.debug("Connected to cluster [{}]", clusterName);
            this.localNode = new DiscoveryNode(settings.get("name"), DiscoveryService.generateNodeId(settings), transportService.boundAddress().publishAddress(),
                    discoveryNodeService.buildAttributes(), version);

            clusterGroup.members().add(this);

            LocalDiscovery firstMaster = null;
            for (LocalDiscovery localDiscovery : clusterGroup.members()) {
                if (localDiscovery.localNode().masterNode()) {
                    firstMaster = localDiscovery;
                    break;
                }
            }

            if (firstMaster != null && firstMaster.equals(this)) {
                // we are the first master (and the master)
                master = true;
                final LocalDiscovery master = firstMaster;
                clusterService.submitStateUpdateTask("local-disco-initial_connect(master)", new ClusterStateUpdateTask() {

                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
                        for (LocalDiscovery discovery : clusterGroups.get(clusterName).members()) {
                            nodesBuilder.put(discovery.localNode);
                        }
                        nodesBuilder.localNodeId(master.localNode().id()).masterNodeId(master.localNode().id());
                        // remove the NO_MASTER block in this case
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(discoverySettings.getNoMasterBlock());
                        return ClusterState.builder(currentState).nodes(nodesBuilder).blocks(blocks).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } else if (firstMaster != null) {
                // update as fast as we can the local node state with the new metadata (so we create indices for example)
                final ClusterState masterState = firstMaster.clusterService.state();
                clusterService.submitStateUpdateTask("local-disco(detected_master)", new ClusterStateUpdateTask() {
                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // make sure we have the local node id set, we might need it as a result of the new metadata
                        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentState.nodes()).put(localNode).localNodeId(localNode.id());
                        return ClusterState.builder(currentState).metaData(masterState.metaData()).nodes(nodesBuilder).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }
                });

                // tell the master to send the fact that we are here
                final LocalDiscovery master = firstMaster;
                firstMaster.clusterService.submitStateUpdateTask("local-disco-receive(from node[" + localNode + "])", new ClusterStateUpdateTask() {
                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
                        for (LocalDiscovery discovery : clusterGroups.get(clusterName).members()) {
                            nodesBuilder.put(discovery.localNode);
                        }
                        nodesBuilder.localNodeId(master.localNode().id()).masterNodeId(master.localNode().id());
                        return ClusterState.builder(currentState).nodes(nodesBuilder).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();
                        // we reroute not in the same cluster state update since in certain areas we rely on
                        // the node to be in the cluster state (sampled from ClusterService#state) to be there, also
                        // shard transitions need to better be handled in such cases
                        master.routingService.reroute("post_node_add");
                    }
                });
            }
        } // else, no master node, the next node that will start will fill things in...
    }

    @Override
    protected void doStop() {
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                logger.warn("Illegal state, should not have an empty cluster group when stopping, I should be there at teh very least...");
                return;
            }
            clusterGroup.members().remove(this);
            if (clusterGroup.members().isEmpty()) {
                // no more members, remove and return
                clusterGroups.remove(clusterName);
                return;
            }

            LocalDiscovery firstMaster = null;
            for (LocalDiscovery localDiscovery : clusterGroup.members()) {
                if (localDiscovery.localNode().masterNode()) {
                    firstMaster = localDiscovery;
                    break;
                }
            }

            if (firstMaster != null) {
                // if the removed node is the master, make the next one as the master
                if (master) {
                    firstMaster.master = true;
                }

                final Set<String> newMembers = new HashSet<>();
                for (LocalDiscovery discovery : clusterGroup.members()) {
                    newMembers.add(discovery.localNode.id());
                }

                final LocalDiscovery master = firstMaster;
                master.clusterService.submitStateUpdateTask("local-disco-update", new ClusterStateUpdateTask() {
                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes newNodes = currentState.nodes().removeDeadMembers(newMembers, master.localNode.id());
                        DiscoveryNodes.Delta delta = newNodes.delta(currentState.nodes());
                        if (delta.added()) {
                            logger.warn("No new nodes should be created when a new discovery view is accepted");
                        }
                        // reroute here, so we eagerly remove dead nodes from the routing
                        ClusterState updatedState = ClusterState.builder(currentState).nodes(newNodes).build();
                        RoutingAllocation.Result routingResult = master.routingService.getAllocationService().reroute(
                                ClusterState.builder(updatedState).build(), "elected as master");
                        return ClusterState.builder(updatedState).routingResult(routingResult).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }
                });
            }
        }
    }

    @Override
    protected void doClose() {
    }

    @Override
    public DiscoveryNode localNode() {
        return localNode;
    }

    @Override
    public void addListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.add(listener);
    }

    @Override
    public void removeListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.remove(listener);
    }

    @Override
    public String nodeDescription() {
        return clusterName.value() + "/" + localNode.id();
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, final Discovery.AckListener ackListener) {
        if (!master) {
            throw new IllegalStateException("Shouldn't publish state when not master");
        }
        LocalDiscovery[] members = members();
        if (members.length > 0) {
            Set<DiscoveryNode> nodesToPublishTo = new HashSet<>(members.length);
            for (LocalDiscovery localDiscovery : members) {
                if (localDiscovery.master) {
                    continue;
                }
                nodesToPublishTo.add(localDiscovery.localNode);
            }
            publish(members, clusterChangedEvent, new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener));
        }
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(null);
    }

    private LocalDiscovery[] members() {
        ClusterGroup clusterGroup = clusterGroups.get(clusterName);
        if (clusterGroup == null) {
            return NO_MEMBERS;
        }
        Queue<LocalDiscovery> members = clusterGroup.members();
        return members.toArray(new LocalDiscovery[members.size()]);
    }

    private void publish(LocalDiscovery[] members, ClusterChangedEvent clusterChangedEvent, final BlockingClusterStatePublishResponseHandler publishResponseHandler) {

        try {
            // we do the marshaling intentionally, to check it works well...
            byte[] clusterStateBytes = null;
            byte[] clusterStateDiffBytes = null;

            ClusterState clusterState = clusterChangedEvent.state();
            for (final LocalDiscovery discovery : members) {
                if (discovery.master) {
                    continue;
                }
                ClusterState newNodeSpecificClusterState = null;
                synchronized (this) {
                    // we do the marshaling intentionally, to check it works well...
                    // check if we publsihed cluster state at least once and node was in the cluster when we published cluster state the last time
                    if (discovery.lastProcessedClusterState != null && clusterChangedEvent.previousState().nodes().nodeExists(discovery.localNode.id())) {
                        // both conditions are true - which means we can try sending cluster state as diffs
                        if (clusterStateDiffBytes == null) {
                            Diff diff = clusterState.diff(clusterChangedEvent.previousState());
                            BytesStreamOutput os = new BytesStreamOutput();
                            diff.writeTo(os);
                            clusterStateDiffBytes = os.bytes().toBytes();
                        }
                        try {
                            newNodeSpecificClusterState = discovery.lastProcessedClusterState.readDiffFrom(StreamInput.wrap(clusterStateDiffBytes)).apply(discovery.lastProcessedClusterState);
                            logger.trace("sending diff cluster state version [{}] with size {} to [{}]", clusterState.version(), clusterStateDiffBytes.length, discovery.localNode.getName());
                        } catch (IncompatibleClusterStateVersionException ex) {
                            logger.warn("incompatible cluster state version [{}] - resending complete cluster state", ex, clusterState.version());
                        }
                    }
                    if (newNodeSpecificClusterState == null) {
                        if (clusterStateBytes == null) {
                            clusterStateBytes = Builder.toBytes(clusterState);
                        }
                        newNodeSpecificClusterState = ClusterState.Builder.fromBytes(clusterStateBytes, discovery.localNode);
                    }
                    discovery.lastProcessedClusterState = newNodeSpecificClusterState;
                }
                final ClusterState nodeSpecificClusterState = newNodeSpecificClusterState;

                nodeSpecificClusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
                // ignore cluster state messages that do not include "me", not in the game yet...
                if (nodeSpecificClusterState.nodes().localNode() != null) {
                    assert nodeSpecificClusterState.nodes().masterNode() != null : "received a cluster state without a master";
                    assert !nodeSpecificClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock()) : "received a cluster state with a master block";

                    discovery.clusterService.submitStateUpdateTask("local-disco-receive(from master)", new ClusterStateUpdateTask() {
                        @Override
                        public boolean runOnlyOnMaster() {
                            return false;
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            if (currentState.supersedes(nodeSpecificClusterState)) {
                                return currentState;
                            }

                            if (currentState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock())) {
                                // its a fresh update from the master as we transition from a start of not having a master to having one
                                logger.debug("got first state from fresh master [{}]", nodeSpecificClusterState.nodes().masterNodeId());
                                return nodeSpecificClusterState;
                            }

                            ClusterState.Builder builder = ClusterState.builder(nodeSpecificClusterState);
                            // if the routing table did not change, use the original one
                            if (nodeSpecificClusterState.routingTable().version() == currentState.routingTable().version()) {
                                builder.routingTable(currentState.routingTable());
                            }
                            if (nodeSpecificClusterState.metaData().version() == currentState.metaData().version()) {
                                builder.metaData(currentState.metaData());
                            }

                            return builder.build();
                        }

                        @Override
                        public void onFailure(String source, Throwable t) {
                            logger.error("unexpected failure during [{}]", t, source);
                            publishResponseHandler.onFailure(discovery.localNode, t);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            sendInitialStateEventIfNeeded();
                            publishResponseHandler.onResponse(discovery.localNode);
                        }
                    });
                } else {
                    publishResponseHandler.onResponse(discovery.localNode);
                }
            }

            TimeValue publishTimeout = discoverySettings.getPublishTimeout();
            if (publishTimeout.millis() > 0) {
                try {
                    boolean awaited = publishResponseHandler.awaitAllNodes(publishTimeout);
                    if (!awaited) {
                        DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes();
                        // everyone may have just responded
                        if (pendingNodes.length > 0) {
                            logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})", clusterState.version(), publishTimeout, pendingNodes);
                        }
                    }
                } catch (InterruptedException e) {
                    // ignore & restore interrupt
                    Thread.currentThread().interrupt();
                }
            }


        } catch (Exception e) {
            // failure to marshal or un-marshal
            throw new IllegalStateException("Cluster state failed to serialize", e);
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

        private Queue<LocalDiscovery> members = ConcurrentCollections.newQueue();

        Queue<LocalDiscovery> members() {
            return members;
        }
    }
}
