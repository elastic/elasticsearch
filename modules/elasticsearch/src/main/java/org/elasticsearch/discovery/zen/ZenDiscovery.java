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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.MasterFaultDetection;
import org.elasticsearch.discovery.zen.fd.NodesFaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.node.DiscoveryNode.*;
import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class ZenDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ClusterName clusterName;

    private final ZenPingService pingService;

    private final MasterFaultDetection masterFD;

    private final NodesFaultDetection nodesFD;

    private final PublishClusterStateAction publishClusterState;

    private final MembershipAction membership;


    private final TimeValue initialPingTimeout;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;


    private DiscoveryNode localNode;

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    @Inject public ZenDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                TransportService transportService, ClusterService clusterService,
                                ZenPingService pingService) {
        super(settings);
        this.clusterName = clusterName;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.pingService = pingService;

        this.initialPingTimeout = componentSettings.getAsTime("ping_timeout", componentSettings.getAsTime("initial_ping_timeout", timeValueSeconds(3)));
        this.sendLeaveRequest = componentSettings.getAsBoolean("send_leave_request", true);

        logger.debug("using initial_ping_timeout [{}]", initialPingTimeout);

        this.electMaster = new ElectMasterService(settings);

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService);
        this.nodesFD.addListener(new NodeFailureListener());

        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
        this.pingService.setNodesProvider(this);
        this.membership = new MembershipAction(settings, transportService, this, new MembershipListener());
    }

    @Override protected void doStart() throws ElasticSearchException {
        Map<String, String> nodeAttributes = buildCommonNodesAttributes(settings);
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        String nodeId = UUID.randomBase64UUID();
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes);
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        nodesFD.updateNodes(latestDiscoNodes);
        pingService.start();

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster();
    }

    @Override protected void doStop() throws ElasticSearchException {
        pingService.stop();
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        initialStateSent.set(false);
        if (sendLeaveRequest) {
            if (!master && latestDiscoNodes.masterNode() != null) {
                try {
                    membership.sendLeaveRequestBlocking(latestDiscoNodes.masterNode(), localNode, TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug("failed to send leave request to master [{}]", e, latestDiscoNodes.masterNode());
                }
            } else {
                DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(latestDiscoNodes.nodes().values(), 5);
                for (DiscoveryNode possibleMaster : possibleMasters) {
                    if (localNode.equals(possibleMaster)) {
                        continue;
                    }
                    try {
                        membership.sendLeaveRequest(latestDiscoNodes.masterNode(), possibleMaster);
                    } catch (Exception e) {
                        logger.debug("failed to send leave request from master [{}] to possible master [{}]", e, latestDiscoNodes.masterNode(), possibleMaster);
                    }
                }
            }
        }
        master = false;
        if (currentJoinThread != null) {
            try {
                currentJoinThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
        masterFD.close();
        nodesFD.close();
        publishClusterState.close();
        membership.close();
        pingService.close();
    }

    @Override public DiscoveryNode localNode() {
        return localNode;
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

    @Override public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return newNodesBuilder().put(localNode).localNodeId(localNode.id()).build();
    }

    @Override public void publish(ClusterState clusterState) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        latestDiscoNodes = clusterState.nodes();
        nodesFD.updateNodes(clusterState.nodes());
        publishClusterState.publish(clusterState);
    }

    private void asyncJoinCluster() {
        threadPool.cached().execute(new Runnable() {
            @Override public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innterJoinCluster();
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    private void innterJoinCluster() {
        boolean retry = true;
        while (retry) {
            if (lifecycle.stoppedOrClosed()) {
                return;
            }
            retry = false;
            DiscoveryNode masterNode = findMaster();
            if (masterNode == null) {
                retry = true;
                continue;
            }
            if (localNode.equals(masterNode)) {
                this.master = true;
                nodesFD.start(); // start the nodes FD
                clusterService.submitStateUpdateTask("zen-disco-join (elected_as_master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                                .localNodeId(localNode.id())
                                .masterNodeId(localNode.id())
                                        // put our local node
                                .put(localNode);
                        // update the fact that we are the master...
                        latestDiscoNodes = builder.build();
                        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK).build();
                        return newClusterStateBuilder().state(currentState).nodes(builder).blocks(clusterBlocks).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } else {
                this.master = false;
                try {
                    // first, make sure we can connect to the master
                    transportService.connectToNode(masterNode);
                } catch (Exception e) {
                    logger.warn("failed to connect to master [{}], retrying...", e, masterNode);
                    retry = true;
                    continue;
                }
                // send join request
                ClusterState clusterState;
                try {
                    clusterState = membership.sendJoinRequestBlocking(masterNode, localNode, initialPingTimeout);
                } catch (Exception e) {
                    if (e instanceof ElasticSearchException) {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, ((ElasticSearchException) e).getDetailedMessage());
                    } else {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, e.getMessage());
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace("detailed failed reason", e);
                    }
                    // failed to send the join request, retry
                    retry = true;
                    continue;
                }
                masterFD.start(masterNode, "initial_join");

                // we update the metadata once we managed to join, so we pre-create indices and so on (no shards allocation)
                final MetaData metaData = clusterState.metaData();
                // sync also the version with the version the master currently has, so the next update will be applied
                final long version = clusterState.version();
                clusterService.submitStateUpdateTask("zen-disco-join (detected master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK).build();
                        // make sure we have the local node id set, we might need it as a result of the new metadata
                        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.newNodesBuilder().putAll(currentState.nodes()).put(localNode).localNodeId(localNode.id());
                        return newClusterStateBuilder().state(currentState).nodes(nodesBuilder).blocks(clusterBlocks).metaData(metaData).version(version).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        // don't send initial state event, since we want to get the cluster state from the master that includes us first
//                        sendInitialStateEventIfNeeded();
                    }
                });
            }
        }
    }

    private void handleNodeFailure(final DiscoveryNode node, String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (!master) {
            // nothing to do here...
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-node_failed(" + node + "), reason " + reason, new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                        .putAll(currentState.nodes())
                        .remove(node.id());
                latestDiscoNodes = builder.build();
                return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }
        });
    }

    private void handleMasterGone(final DiscoveryNode masterNode, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a master failure
            return;
        }
        if (master) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info("master_left [{}], reason [{}]", masterNode, reason);

        clusterService.submitStateUpdateTask("zen-disco-master_failed (" + masterNode + ")", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                if (!masterNode.id().equals(currentState.nodes().masterNodeId())) {
                    // master got switched on us, no need to send anything
                    return currentState;
                }

                ClusterBlocks clusterBlocks = currentState.blocks();
                MetaData metaData = currentState.metaData();
                RoutingTable routingTable = currentState.routingTable();
                List<DiscoveryNode> nodes = newArrayList(currentState.nodes().nodes().values());
                nodes.remove(masterNode); // remove the master node from the list, it has failed
                final DiscoveryNode electedMaster = electMaster.electMaster(nodes); // elect master
                if (localNode.equals(electedMaster)) {
                    master = true;
                    masterFD.stop("got elected as new master since master left (reason = " + reason + ")");
                    nodesFD.start();
                    DiscoveryNodes.Builder builder = DiscoveryNodes.newNodesBuilder()
                            .putAll(currentState.nodes())
                                    // make sure the old master node, which has failed, is not part of the nodes we publish
                            .remove(masterNode.id())
                            .masterNodeId(localNode.id());
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                } else {
                    nodesFD.stop();
                    DiscoveryNodes.Builder builder = DiscoveryNodes.newNodesBuilder()
                            .putAll(currentState.nodes()).remove(masterNode.id());
                    if (electedMaster != null) {
                        builder.masterNodeId(electedMaster.id());
                        masterFD.restart(electedMaster, "possible elected master since master left (reason = " + reason + ")");
                    } else {
                        logger.warn("master_left and no other node elected to become master, current nodes: {}", nodes);
                        builder.masterNodeId(null);
                        clusterBlocks = ClusterBlocks.builder().blocks(clusterBlocks).addGlobalBlock(NO_MASTER_BLOCK).build();
                        // if this is a data node, clean the metadata and routing, since we want to recreate the indices and shards
                        if (currentState.nodes().localNode().dataNode()) {
                            metaData = MetaData.newMetaDataBuilder().build();
                            routingTable = RoutingTable.newRoutingTableBuilder().build();
                        }
                        masterFD.stop("no master elected since master left (reason = " + reason + ")");
                        asyncJoinCluster();
                    }
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState)
                            .blocks(clusterBlocks)
                            .nodes(latestDiscoNodes)
                            .metaData(metaData)
                            .routingTable(routingTable)
                            .build();
                }
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }

        });
    }

    void handleNewClusterStateFromMaster(final ClusterState clusterState) {
        if (master) {
            logger.warn("master should not receive new cluster state from [{}]", clusterState.nodes().masterNode());
        } else {
            if (clusterState.nodes().localNode() == null) {
                logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", clusterState.nodes().masterNode());
            } else {
                clusterService.submitStateUpdateTask("zen-disco-receive(from master [" + clusterState.nodes().masterNode() + "])", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        latestDiscoNodes = clusterState.nodes();

                        // check to see that we monitor the correct master of the cluster
                        if (masterFD.masterNode() == null || !masterFD.masterNode().equals(latestDiscoNodes.masterNode())) {
                            masterFD.restart(latestDiscoNodes.masterNode(), "new cluster stare received and we monitor the wrong master [" + masterFD.masterNode() + "]");
                        }
                        return clusterState;
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            }
        }
    }

    private void handleLeaveRequest(final DiscoveryNode node) {
        if (master) {
            clusterService.submitStateUpdateTask("zen-disco-node_left(" + node + ")", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                            .putAll(currentState.nodes())
                            .remove(node.id());
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                }
            });
        } else {
            handleMasterGone(node, "shut_down");
        }
    }

    private ClusterState handleJoinRequest(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
        }

        ClusterState state = clusterService.state();
        if (!transportService.addressSupported(node.address().getClass())) {
            // TODO, what should we do now? Maybe inform that node that its crap?
            logger.warn("received a wrong address type from [{}], ignoring...", node);
        } else {
            // try and connect to the node, if it fails, we can raise an exception back to the client...
            transportService.connectToNode(node);
            state = clusterService.state();

            clusterService.submitStateUpdateTask("zen-disco-receive(join from node[" + node + "])", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    if (currentState.nodes().nodeExists(node.id())) {
                        // the node already exists in the cluster
                        logger.warn("received a join request for an existing node [{}]", node);
                        // still send a new cluster state, so it will be re published and possibly update the other node
                        return ClusterState.builder().state(currentState).build();
                    }
                    return newClusterStateBuilder().state(currentState).nodes(currentState.nodes().newNode(node)).build();
                }
            });
        }
        return state;
    }

    private DiscoveryNode findMaster() {
        ZenPing.PingResponse[] pingResponses = pingService.pingAndWait(initialPingTimeout);
        if (pingResponses == null) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("ping responses:");
            if (pingResponses.length == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : pingResponses) {
                    sb.append("\n\t--> ").append("target [").append(pingResponse.target()).append("], master [").append(pingResponse.master()).append("]");
                }
            }
            logger.debug(sb.toString());
        }
        List<DiscoveryNode> pingMasters = newArrayList();
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            if (pingResponse.master() != null) {
                pingMasters.add(pingResponse.master());
            }
        }
        if (pingMasters.isEmpty()) {
            // lets tie break between discovered nodes
            List<DiscoveryNode> possibleMasterNodes = newArrayList();
            possibleMasterNodes.add(localNode);
            for (ZenPing.PingResponse pingResponse : pingResponses) {
                possibleMasterNodes.add(pingResponse.target());
            }
            DiscoveryNode electedMaster = electMaster.electMaster(possibleMasterNodes);
            if (localNode.equals(electedMaster)) {
                return localNode;
            }
        } else {
            DiscoveryNode electedMaster = electMaster.electMaster(pingMasters);
            if (electedMaster != null) {
                return electedMaster;
            }
        }
        return null;
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    private class NewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {
        @Override public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override public ClusterState onJoin(DiscoveryNode node) {
            return handleJoinRequest(node);
        }

        @Override public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFailureListener implements NodesFaultDetection.Listener {

        @Override public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override public void onMasterFailure(DiscoveryNode masterNode, String reason) {
            handleMasterGone(masterNode, reason);
        }

        @Override public void onDisconnectedFromMaster() {
            // got disconnected from the master, send a join request
            DiscoveryNode masterNode = latestDiscoNodes.masterNode();
            try {
                membership.sendJoinRequest(masterNode, localNode);
            } catch (Exception e) {
                logger.warn("failed to send join request on disconnection from master [{}]", masterNode);
            }
        }
    }
}
