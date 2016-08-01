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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.BlockingClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;

import java.util.HashSet;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.cluster.ClusterState.Builder;

/**
 *
 */
public class LocalDiscovery extends AbstractLifecycleComponent implements Discovery {

    private static final LocalDiscovery[] NO_MEMBERS = new LocalDiscovery[0];

    private final ClusterService clusterService;
    private AllocationService allocationService;
    private final ClusterName clusterName;

    private final DiscoverySettings discoverySettings;

    private volatile boolean master = false;

    private static final ConcurrentMap<ClusterName, ClusterGroup> clusterGroups = ConcurrentCollections.newConcurrentMap();

    private volatile ClusterState lastProcessedClusterState;

    @Inject
    public LocalDiscovery(Settings settings, ClusterService clusterService, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterName = clusterService.getClusterName();
        this.clusterService = clusterService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    protected void doStart() {

    }

    @Override
    public void startInitialJoin() {
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                clusterGroup = new ClusterGroup();
                clusterGroups.put(clusterName, clusterGroup);
            }
            logger.debug("Connected to cluster [{}]", clusterName);

            Optional<LocalDiscovery> current = clusterGroup.members().stream().filter(other -> (
                other.localNode().equals(this.localNode()) || other.localNode().getId().equals(this.localNode().getId())
            )).findFirst();
            if (current.isPresent()) {
                throw new IllegalStateException("current cluster group already contains a node with the same id. current "
                    + current.get().localNode() + ", this node " + localNode());
            }

            clusterGroup.members().add(this);

            LocalDiscovery firstMaster = null;
            for (LocalDiscovery localDiscovery : clusterGroup.members()) {
                if (localDiscovery.localNode().isMasterNode()) {
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
                            nodesBuilder.add(discovery.localNode());
                        }
                        nodesBuilder.localNodeId(master.localNode().getId()).masterNodeId(master.localNode().getId());
                        // remove the NO_MASTER block in this case
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(discoverySettings.getNoMasterBlock());
                        return ClusterState.builder(currentState).nodes(nodesBuilder).blocks(blocks).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("unexpected failure during [{}]", e, source);
                    }
                });
            } else if (firstMaster != null) {
                // tell the master to send the fact that we are here
                final LocalDiscovery master = firstMaster;
                firstMaster.clusterService.submitStateUpdateTask("local-disco-receive(from node[" + localNode() + "])", new ClusterStateUpdateTask() {
                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
                        for (LocalDiscovery discovery : clusterGroups.get(clusterName).members()) {
                            nodesBuilder.add(discovery.localNode());
                        }
                        nodesBuilder.localNodeId(master.localNode().getId()).masterNodeId(master.localNode().getId());
                        currentState = ClusterState.builder(currentState).nodes(nodesBuilder).build();
                        RoutingAllocation.Result result =  master.allocationService.reroute(currentState, "node_add");
                        if (result.changed()) {
                            currentState = ClusterState.builder(currentState).routingResult(result).build();
                        }
                        return currentState;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("unexpected failure during [{}]", e, source);
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
                if (localDiscovery.localNode().isMasterNode()) {
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
                    newMembers.add(discovery.localNode().getId());
                }

                final LocalDiscovery master = firstMaster;
                master.clusterService.submitStateUpdateTask("local-disco-update", new ClusterStateUpdateTask() {
                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes newNodes = currentState.nodes().removeDeadMembers(newMembers, master.localNode().getId());
                        DiscoveryNodes.Delta delta = newNodes.delta(currentState.nodes());
                        if (delta.added()) {
                            logger.warn("No new nodes should be created when a new discovery view is accepted");
                        }
                        // reroute here, so we eagerly remove dead nodes from the routing
                        ClusterState updatedState = ClusterState.builder(currentState).nodes(newNodes).build();
                        RoutingAllocation.Result routingResult = master.allocationService.deassociateDeadNodes(
                                ClusterState.builder(updatedState).build(), true, "node stopped");
                        return ClusterState.builder(updatedState).routingResult(routingResult).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("unexpected failure during [{}]", e, source);
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
        return clusterService.localNode();
    }

    @Override
    public String nodeDescription() {
        return clusterName.value() + "/" + localNode().getId();
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
                nodesToPublishTo.add(localDiscovery.localNode());
            }
            publish(members, clusterChangedEvent, new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener));
        }
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(null);
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public int getMinimumMasterNodes() {
        return -1;
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
                    // check if we published cluster state at least once and node was in the cluster when we published cluster state the last time
                    if (discovery.lastProcessedClusterState != null && clusterChangedEvent.previousState().nodes().nodeExists(discovery.localNode())) {
                        // both conditions are true - which means we can try sending cluster state as diffs
                        if (clusterStateDiffBytes == null) {
                            Diff diff = clusterState.diff(clusterChangedEvent.previousState());
                            BytesStreamOutput os = new BytesStreamOutput();
                            diff.writeTo(os);
                            clusterStateDiffBytes = BytesReference.toBytes(os.bytes());
                        }
                        try {
                            newNodeSpecificClusterState = discovery.lastProcessedClusterState.readDiffFrom(StreamInput.wrap(clusterStateDiffBytes)).apply(discovery.lastProcessedClusterState);
                            logger.trace("sending diff cluster state version [{}] with size {} to [{}]", clusterState.version(), clusterStateDiffBytes.length, discovery.localNode().getName());
                        } catch (IncompatibleClusterStateVersionException ex) {
                            logger.warn("incompatible cluster state version [{}] - resending complete cluster state", ex, clusterState.version());
                        }
                    }
                    if (newNodeSpecificClusterState == null) {
                        if (clusterStateBytes == null) {
                            clusterStateBytes = Builder.toBytes(clusterState);
                        }
                        newNodeSpecificClusterState = ClusterState.Builder.fromBytes(clusterStateBytes, discovery.localNode());
                    }
                    discovery.lastProcessedClusterState = newNodeSpecificClusterState;
                }
                final ClusterState nodeSpecificClusterState = newNodeSpecificClusterState;

                nodeSpecificClusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
                // ignore cluster state messages that do not include "me", not in the game yet...
                if (nodeSpecificClusterState.nodes().getLocalNode() != null) {
                    assert nodeSpecificClusterState.nodes().getMasterNode() != null : "received a cluster state without a master";
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
                                logger.debug("got first state from fresh master [{}]", nodeSpecificClusterState.nodes().getMasterNodeId());
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
                        public void onFailure(String source, Exception e) {
                            logger.error("unexpected failure during [{}]", e, source);
                            publishResponseHandler.onFailure(discovery.localNode(), e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            publishResponseHandler.onResponse(discovery.localNode());
                        }
                    });
                } else {
                    publishResponseHandler.onResponse(discovery.localNode());
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

    private class ClusterGroup {

        private Queue<LocalDiscovery> members = ConcurrentCollections.newQueue();

        Queue<LocalDiscovery> members() {
            return members;
        }
    }
}
