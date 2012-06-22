/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class ZenDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ClusterName clusterName;

    private final DiscoveryNodeService discoveryNodeService;

    private final ZenPingService pingService;

    private final MasterFaultDetection masterFD;

    private final NodesFaultDetection nodesFD;

    private final PublishClusterStateAction publishClusterState;

    private final MembershipAction membership;


    private final TimeValue pingTimeout;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;

    private final boolean masterElectionFilterClientNodes;
    private final boolean masterElectionFilterDataNodes;


    private DiscoveryNode localNode;

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    @Nullable
    private NodeService nodeService;

    @Inject
    public ZenDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                        TransportService transportService, ClusterService clusterService, NodeSettingsService nodeSettingsService,
                        DiscoveryNodeService discoveryNodeService, ZenPingService pingService) {
        super(settings);
        this.clusterName = clusterName;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.discoveryNodeService = discoveryNodeService;
        this.pingService = pingService;

        // also support direct discovery.zen settings, for cases when it gets extended
        this.pingTimeout = settings.getAsTime("discovery.zen.ping.timeout", settings.getAsTime("discovery.zen.ping_timeout", componentSettings.getAsTime("ping_timeout", componentSettings.getAsTime("initial_ping_timeout", timeValueSeconds(3)))));
        this.sendLeaveRequest = componentSettings.getAsBoolean("send_leave_request", true);

        this.masterElectionFilterClientNodes = settings.getAsBoolean("discovery.zen.master_election.filter_client", true);
        this.masterElectionFilterDataNodes = settings.getAsBoolean("discovery.zen.master_election.filter_data", false);

        logger.debug("using ping.timeout [{}], master_election.filter_client [{}], master_election.filter_data [{}]", pingTimeout, masterElectionFilterClientNodes, masterElectionFilterDataNodes);

        this.electMaster = new ElectMasterService(settings, nodeSettingsService);

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService);
        this.nodesFD.addListener(new NodeFailureListener());

        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
        this.pingService.setNodesProvider(this);
        this.membership = new MembershipAction(settings, transportService, this, new MembershipListener());

        transportService.registerHandler(RejoinClusterRequestHandler.ACTION, new RejoinClusterRequestHandler());
    }

    @Override
    public void setNodeService(@Nullable NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        Map<String, String> nodeAttributes = discoveryNodeService.buildAttributes();
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        String nodeId = UUID.randomBase64UUID();
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes);
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        nodesFD.updateNodes(latestDiscoNodes);
        pingService.start();

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster();
    }

    @Override
    protected void doStop() throws ElasticSearchException {
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

    @Override
    protected void doClose() throws ElasticSearchException {
        masterFD.close();
        nodesFD.close();
        publishClusterState.close();
        membership.close();
        pingService.close();
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
    public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return newNodesBuilder().put(localNode).localNodeId(localNode.id()).build();
    }

    @Override
    public NodeService nodeService() {
        return this.nodeService;
    }

    @Override
    public void publish(ClusterState clusterState) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        latestDiscoNodes = clusterState.nodes();
        nodesFD.updateNodes(clusterState.nodes());
        publishClusterState.publish(clusterState);
    }

    private void asyncJoinCluster() {
        if (currentJoinThread != null) {
            // we are already joining, ignore...
            return;
        }
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
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
                    @Override
                    public ClusterState execute(ClusterState currentState) {
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

                    @Override
                    public void clusterStateProcessed(ClusterState clusterState) {
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
                ClusterState joinClusterStateX;
                try {
                    joinClusterStateX = membership.sendJoinRequestBlocking(masterNode, localNode, pingTimeout);
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
                // no need to submit the received cluster state, we will get it from the master when it publishes
                // the fact that we joined
            }
        }
    }

    private void handleLeaveRequest(final DiscoveryNode node) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (master) {
            clusterService.submitStateUpdateTask("zen-disco-node_left(" + node + ")", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                            .putAll(currentState.nodes())
                            .remove(node.id());
                    latestDiscoNodes = builder.build();
                    currentState = newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                    // check if we have enough master nodes, if not, we need to move into joining the cluster again
                    if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                        return rejoin(currentState, "not enough master nodes");
                    }
                    return currentState;
                }
            });
        } else {
            handleMasterGone(node, "shut_down");
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
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                        .putAll(currentState.nodes())
                        .remove(node.id());
                latestDiscoNodes = builder.build();
                currentState = newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes");
                }
                return currentState;
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
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
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!masterNode.id().equals(currentState.nodes().masterNodeId())) {
                    // master got switched on us, no need to send anything
                    return currentState;
                }

                DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.newNodesBuilder()
                        .putAll(currentState.nodes())
                                // make sure the old master node, which has failed, is not part of the nodes we publish
                        .remove(masterNode.id())
                        .masterNodeId(null);

                if (!electMaster.hasEnoughMasterNodes(nodesBuilder.build())) {
                    return rejoin(ClusterState.builder().state(currentState).nodes(nodesBuilder).build(), "not enough master nodes after master left (reason = " + reason + ")");
                }

                final DiscoveryNode electedMaster = electMaster.electMaster(nodesBuilder.build()); // elect master
                if (localNode.equals(electedMaster)) {
                    master = true;
                    masterFD.stop("got elected as new master since master left (reason = " + reason + ")");
                    nodesFD.start();
                    nodesBuilder.masterNodeId(localNode.id());
                    latestDiscoNodes = nodesBuilder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                } else {
                    nodesFD.stop();
                    if (electedMaster != null) {
                        nodesBuilder.masterNodeId(electedMaster.id());
                        masterFD.restart(electedMaster, "possible elected master since master left (reason = " + reason + ")");
                        latestDiscoNodes = nodesBuilder.build();
                        return newClusterStateBuilder().state(currentState)
                                .nodes(latestDiscoNodes)
                                .build();
                    } else {
                        return rejoin(newClusterStateBuilder().state(currentState).nodes(nodesBuilder.build()).build(), "master_left and no other node elected to become master");
                    }
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }

        });
    }

    void handleNewClusterStateFromMaster(final ClusterState newState) {
        if (master) {
            clusterService.submitStateUpdateTask("zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (newState.version() > currentState.version()) {
                        logger.warn("received cluster state from [{}] which is also master but with a newer cluster_state, rejoining to cluster...", newState.nodes().masterNode());
                        return rejoin(currentState, "zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]");
                    } else {
                        logger.warn("received cluster state from [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster", newState.nodes().masterNode(), newState.nodes().masterNode());
                        transportService.sendRequest(newState.nodes().masterNode(), RejoinClusterRequestHandler.ACTION, new RejoinClusterRequest(currentState.nodes().localNodeId()), new VoidTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override
                            public void handleException(TransportException exp) {
                                logger.warn("failed to send rejoin request to [{}]", exp, newState.nodes().masterNode());
                            }
                        });
                        return currentState;
                    }
                }
            });
        } else {
            if (newState.nodes().localNode() == null) {
                logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", newState.nodes().masterNode());
            } else {
                if (currentJoinThread != null) {
                    logger.debug("got a new state from master node, though we are already trying to rejoin the cluster");
                }

                clusterService.submitStateUpdateTask("zen-disco-receive(from master [" + newState.nodes().masterNode() + "])", new ProcessedClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {

                        // we don't need to do this, since we ping the master, and get notified when it has moved from being a master
                        // because it doesn't have enough master nodes...
                        //if (!electMaster.hasEnoughMasterNodes(newState.nodes())) {
                        //    return disconnectFromCluster(newState, "not enough master nodes on new cluster state received from [" + newState.nodes().masterNode() + "]");
                        //}

                        latestDiscoNodes = newState.nodes();

                        // check to see that we monitor the correct master of the cluster
                        if (masterFD.masterNode() == null || !masterFD.masterNode().equals(latestDiscoNodes.masterNode())) {
                            masterFD.restart(latestDiscoNodes.masterNode(), "new cluster stare received and we monitor the wrong master [" + masterFD.masterNode() + "]");
                        }

                        ClusterState.Builder builder = ClusterState.builder().state(newState);
                        // if the routing table did not change, use the original one
                        if (newState.routingTable().version() == currentState.routingTable().version()) {
                            builder.routingTable(currentState.routingTable());
                        }
                        // same for metadata
                        if (newState.metaData().version() == currentState.metaData().version()) {
                            builder.metaData(currentState.metaData());
                        } else {
                            // if its not the same version, only copy over new indices or ones that changed the version
                            MetaData.Builder metaDataBuilder = MetaData.builder().metaData(newState.metaData()).removeAllIndices();
                            for (IndexMetaData indexMetaData : newState.metaData()) {
                                IndexMetaData currentIndexMetaData = currentState.metaData().index(indexMetaData.index());
                                if (currentIndexMetaData == null || currentIndexMetaData.version() != indexMetaData.version()) {
                                    metaDataBuilder.put(indexMetaData, false);
                                } else {
                                    metaDataBuilder.put(currentIndexMetaData, false);
                                }
                            }
                            builder.metaData(metaDataBuilder);
                        }

                        return builder.build();
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            }
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

            // validate the join request, will throw a failure if it fails, which will get back to the
            // node calling the join request
            membership.sendValidateJoinRequestBlocking(node, state, pingTimeout);

            clusterService.submitStateUpdateTask("zen-disco-receive(join from node[" + node + "])", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
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
        ZenPing.PingResponse[] fullPingResponses = pingService.pingAndWait(pingTimeout);
        if (fullPingResponses == null) {
            return null;
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("full ping responses:");
            if (fullPingResponses.length == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : fullPingResponses) {
                    sb.append("\n\t--> ").append("target [").append(pingResponse.target()).append("], master [").append(pingResponse.master()).append("]");
                }
            }
            logger.trace(sb.toString());
        }

        // filter responses
        List<ZenPing.PingResponse> pingResponses = Lists.newArrayList();
        for (ZenPing.PingResponse pingResponse : fullPingResponses) {
            DiscoveryNode node = pingResponse.target();
            if (masterElectionFilterClientNodes && (node.clientNode() || (!node.masterNode() && !node.dataNode()))) {
                // filter out the client node, which is a client node, or also one that is not data and not master (effectively, client)
            } else if (masterElectionFilterDataNodes && (!node.masterNode() && node.dataNode())) {
                // filter out data node that is not also master
            } else {
                pingResponses.add(pingResponse);
            }
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("filtered ping responses: (filter_client[").append(masterElectionFilterClientNodes).append("], filter_data[").append(masterElectionFilterDataNodes).append("])");
            if (pingResponses.isEmpty()) {
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

        Set<DiscoveryNode> possibleMasterNodes = Sets.newHashSet();
        possibleMasterNodes.add(localNode);
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            possibleMasterNodes.add(pingResponse.target());
        }
        // if we don't have enough master nodes, we bail, even if we get a response that indicates
        // there is a master by other node, we don't see enough...
        if (!electMaster.hasEnoughMasterNodes(possibleMasterNodes)) {
            return null;
        }

        if (pingMasters.isEmpty()) {
            // lets tie break between discovered nodes
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

    private ClusterState rejoin(ClusterState clusterState, String reason) {
        logger.warn(reason + ", current nodes: {}", clusterState.nodes());
        nodesFD.stop();
        masterFD.stop(reason);
        master = false;

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(NO_MASTER_BLOCK)
                .addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                .build();

        // clear the routing table, we have no master, so we need to recreate the routing when we reform the cluster
        RoutingTable routingTable = RoutingTable.builder().build();
        // we also clean the metadata, since we are going to recover it if we become master
        MetaData metaData = MetaData.builder().build();

        // clean the nodes, we are now not connected to anybody, since we try and reform the cluster
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();

        asyncJoinCluster();

        return newClusterStateBuilder().state(clusterState)
                .blocks(clusterBlocks)
                .nodes(latestDiscoNodes)
                .routingTable(routingTable)
                .metaData(metaData)
                .build();
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    private class NewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {
        @Override
        public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public ClusterState onJoin(DiscoveryNode node) {
            return handleJoinRequest(node);
        }

        @Override
        public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFailureListener implements NodesFaultDetection.Listener {

        @Override
        public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override
        public void onMasterFailure(DiscoveryNode masterNode, String reason) {
            handleMasterGone(masterNode, reason);
        }

        @Override
        public void onDisconnectedFromMaster() {
            // got disconnected from the master, send a join request
            DiscoveryNode masterNode = latestDiscoNodes.masterNode();
            try {
                membership.sendJoinRequest(masterNode, localNode);
            } catch (Exception e) {
                logger.warn("failed to send join request on disconnection from master [{}]", masterNode);
            }
        }
    }

    static class RejoinClusterRequest implements Streamable {

        private String fromNodeId;

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            fromNodeId = in.readOptionalUTF();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalUTF(fromNodeId);
        }
    }

    class RejoinClusterRequestHandler extends BaseTransportRequestHandler<RejoinClusterRequest> {

        static final String ACTION = "discovery/zen/rejoin";

        @Override
        public RejoinClusterRequest newInstance() {
            return new RejoinClusterRequest();
        }

        @Override
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel) throws Exception {
            clusterService.submitStateUpdateTask("received a request to rejoin the cluster from [" + request.fromNodeId + "]", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    try {
                        channel.sendResponse(VoidStreamable.INSTANCE);
                    } catch (Exception e) {
                        logger.warn("failed to send response on rejoin cluster request handling", e);
                    }
                    return rejoin(currentState, "received a request to rejoin the cluster from [" + request.fromNodeId + "]");
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

}
