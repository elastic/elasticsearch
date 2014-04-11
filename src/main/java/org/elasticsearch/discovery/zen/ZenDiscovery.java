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

package org.elasticsearch.discovery.zen;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.discovery.DiscoverySettings;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class ZenDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private AllocationService allocationService;
    private final ClusterName clusterName;
    private final DiscoveryNodeService discoveryNodeService;
    private final DiscoverySettings discoverySettings;
    private final ZenPingService pingService;
    private final MasterFaultDetection masterFD;
    private final NodesFaultDetection nodesFD;
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership;
    private final Version version;


    private final TimeValue pingTimeout;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;

    private final boolean masterElectionFilterClientNodes;
    private final boolean masterElectionFilterDataNodes;


    private DiscoveryNode localNode;

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<>();

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();


    @Nullable
    private NodeService nodeService;

    @Inject
    public ZenDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                        TransportService transportService, ClusterService clusterService, NodeSettingsService nodeSettingsService,
                        DiscoveryNodeService discoveryNodeService, ZenPingService pingService, Version version, DiscoverySettings discoverySettings) {
        super(settings);
        this.clusterName = clusterName;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.discoveryNodeService = discoveryNodeService;
        this.discoverySettings = discoverySettings;
        this.pingService = pingService;
        this.version = version;

        // also support direct discovery.zen settings, for cases when it gets extended
        this.pingTimeout = settings.getAsTime("discovery.zen.ping.timeout", settings.getAsTime("discovery.zen.ping_timeout", componentSettings.getAsTime("ping_timeout", componentSettings.getAsTime("initial_ping_timeout", timeValueSeconds(3)))));
        this.sendLeaveRequest = componentSettings.getAsBoolean("send_leave_request", true);

        this.masterElectionFilterClientNodes = settings.getAsBoolean("discovery.zen.master_election.filter_client", true);
        this.masterElectionFilterDataNodes = settings.getAsBoolean("discovery.zen.master_election.filter_data", false);

        logger.debug("using ping.timeout [{}], master_election.filter_client [{}], master_election.filter_data [{}]", pingTimeout, masterElectionFilterClientNodes, masterElectionFilterDataNodes);

        this.electMaster = new ElectMasterService(settings);
        nodeSettingsService.addListener(new ApplySettings());

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService);
        this.nodesFD.addListener(new NodeFailureListener());

        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener(), discoverySettings);
        this.pingService.setNodesProvider(this);
        this.membership = new MembershipAction(settings, transportService, this, new MembershipListener());

        transportService.registerHandler(RejoinClusterRequestHandler.ACTION, new RejoinClusterRequestHandler());
    }

    @Override
    public void setNodeService(@Nullable NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        Map<String, String> nodeAttributes = discoveryNodeService.buildAttributes();
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        final String nodeId = DiscoveryService.generateNodeId(settings);
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes, version);
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        nodesFD.updateNodes(latestDiscoNodes);
        pingService.start();

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
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
    protected void doClose() throws ElasticsearchException {
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
        return DiscoveryNodes.builder().put(localNode).localNodeId(localNode.id()).build();
    }

    @Override
    public NodeService nodeService() {
        return this.nodeService;
    }

    @Override
    public void publish(ClusterState clusterState, AckListener ackListener) {
        if (!master) {
            throw new ElasticsearchIllegalStateException("Shouldn't publish state when not master");
        }
        latestDiscoNodes = clusterState.nodes();
        nodesFD.updateNodes(clusterState.nodes());
        publishClusterState.publish(clusterState, ackListener);
    }

    private void asyncJoinCluster() {
        if (currentJoinThread != null) {
            // we are already joining, ignore...
            logger.trace("a join thread already running");
            return;
        }
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innerJoinCluster();
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    private void innerJoinCluster() {
        boolean retry = true;
        while (retry) {
            if (lifecycle.stoppedOrClosed()) {
                return;
            }
            retry = false;
            DiscoveryNode masterNode = findMaster();
            if (masterNode == null) {
                logger.trace("no masterNode returned");
                retry = true;
                continue;
            }
            if (localNode.equals(masterNode)) {
                this.master = true;
                nodesFD.start(); // start the nodes FD
                clusterService.submitStateUpdateTask("zen-disco-join (elected_as_master)", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                                .localNodeId(localNode.id())
                                .masterNodeId(localNode.id())
                                        // put our local node
                                .put(localNode);
                        // update the fact that we are the master...
                        latestDiscoNodes = builder.build();
                        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(discoverySettings.getNoMasterBlock()).build();
                        return ClusterState.builder(currentState).nodes(latestDiscoNodes).blocks(clusterBlocks).build();
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
                try {
                    membership.sendJoinRequestBlocking(masterNode, localNode, pingTimeout);
                } catch (Exception e) {
                    if (e instanceof ElasticsearchException) {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, ((ElasticsearchException) e).getDetailedMessage());
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
            clusterService.submitStateUpdateTask("zen-disco-node_left(" + node + ")", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes()).remove(node.id());
                    latestDiscoNodes = builder.build();
                    currentState = ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                    // check if we have enough master nodes, if not, we need to move into joining the cluster again
                    if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                        return rejoin(currentState, "not enough master nodes");
                    }
                    // eagerly run reroute to remove dead nodes from routing table
                    RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(currentState).build());
                    return ClusterState.builder(currentState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
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
        clusterService.submitStateUpdateTask("zen-disco-node_failed(" + node + "), reason " + reason, Priority.IMMEDIATE, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes())
                        .remove(node.id());
                latestDiscoNodes = builder.build();
                currentState = ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes");
                }
                // eagerly run reroute to remove dead nodes from routing table
                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(currentState).build());
                return ClusterState.builder(currentState).routingResult(routingResult).build();
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
    }

    private void handleMinimumMasterNodesChanged(final int minimumMasterNodes) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        final int prevMinimumMasterNode = ZenDiscovery.this.electMaster.minimumMasterNodes();
        ZenDiscovery.this.electMaster.minimumMasterNodes(minimumMasterNodes);
        if (!master) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-minimum_master_nodes_changed", Priority.IMMEDIATE, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" + minimumMasterNodes + "]");
                }
                return currentState;
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

        clusterService.submitStateUpdateTask("zen-disco-master_failed (" + masterNode + ")", Priority.IMMEDIATE, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!masterNode.id().equals(currentState.nodes().masterNodeId())) {
                    // master got switched on us, no need to send anything
                    return currentState;
                }

                DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(currentState.nodes())
                        // make sure the old master node, which has failed, is not part of the nodes we publish
                        .remove(masterNode.id())
                        .masterNodeId(null).build();

                if (!electMaster.hasEnoughMasterNodes(discoveryNodes)) {
                    return rejoin(ClusterState.builder(currentState).nodes(discoveryNodes).build(), "not enough master nodes after master left (reason = " + reason + ")");
                }

                final DiscoveryNode electedMaster = electMaster.electMaster(discoveryNodes); // elect master
                if (localNode.equals(electedMaster)) {
                    master = true;
                    masterFD.stop("got elected as new master since master left (reason = " + reason + ")");
                    nodesFD.start();
                    discoveryNodes = DiscoveryNodes.builder(discoveryNodes).masterNodeId(localNode.id()).build();
                    latestDiscoNodes = discoveryNodes;
                    return ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                } else {
                    nodesFD.stop();
                    if (electedMaster != null) {
                        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).masterNodeId(electedMaster.id()).build();
                        masterFD.restart(electedMaster, "possible elected master since master left (reason = " + reason + ")");
                        latestDiscoNodes = discoveryNodes;
                        return ClusterState.builder(currentState)
                                .nodes(latestDiscoNodes)
                                .build();
                    } else {
                        return rejoin(ClusterState.builder(currentState).nodes(discoveryNodes).build(), "master_left and no other node elected to become master");
                    }
                }
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
    }

    static class ProcessClusterState {
        final ClusterState clusterState;
        final PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed;
        volatile boolean processed;

        ProcessClusterState(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
            this.clusterState = clusterState;
            this.newStateProcessed = newStateProcessed;
        }
    }

    private final BlockingQueue<ProcessClusterState> processNewClusterStates = ConcurrentCollections.newBlockingQueue();

    void handleNewClusterStateFromMaster(ClusterState newClusterState, final PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
        final ClusterName incomingClusterName = newClusterState.getClusterName();
        /* The cluster name can still be null if the state comes from a node that is prev 1.1.1*/
        if (incomingClusterName != null && !incomingClusterName.equals(this.clusterName)) {
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]", newClusterState.nodes().masterNode(), incomingClusterName);
            newStateProcessed.onNewClusterStateFailed(new ElasticsearchIllegalStateException("received state from a node that is not part of the cluster"));
            return;
        }
        if (master) {
            logger.debug("received cluster state from [{}] which is also master but with cluster name [{}]",  newClusterState.nodes().masterNode(), incomingClusterName);
            final ClusterState newState = newClusterState;
            clusterService.submitStateUpdateTask("zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (newState.version() > currentState.version()) {
                        logger.warn("received cluster state from [{}] which is also master but with a newer cluster_state, rejoining to cluster...", newState.nodes().masterNode());
                        return rejoin(currentState, "zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]");
                    } else {
                        logger.warn("received cluster state from [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster", newState.nodes().masterNode(), newState.nodes().masterNode());
                        transportService.sendRequest(newState.nodes().masterNode(), RejoinClusterRequestHandler.ACTION, new RejoinClusterRequest(currentState.nodes().localNodeId()), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override
                            public void handleException(TransportException exp) {
                                logger.warn("failed to send rejoin request to [{}]", exp, newState.nodes().masterNode());
                            }
                        });
                        return currentState;
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    newStateProcessed.onNewClusterStateProcessed();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                    newStateProcessed.onNewClusterStateFailed(t);
                }

            });
        } else {
            if (newClusterState.nodes().localNode() == null) {
                logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", newClusterState.nodes().masterNode());
                newStateProcessed.onNewClusterStateFailed(new ElasticsearchIllegalStateException("received state from a node that is not part of the cluster"));
            } else {
                if (currentJoinThread != null) {
                    logger.debug("got a new state from master node, though we are already trying to rejoin the cluster");
                }

                final ProcessClusterState processClusterState = new ProcessClusterState(newClusterState, newStateProcessed);
                processNewClusterStates.add(processClusterState);

                clusterService.submitStateUpdateTask("zen-disco-receive(from master [" + newClusterState.nodes().masterNode() + "])", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // we already processed it in a previous event
                        if (processClusterState.processed) {
                            return currentState;
                        }

                        // TODO: once improvement that we can do is change the message structure to include version and masterNodeId
                        // at the start, this will allow us to keep the "compressed bytes" around, and only parse the first page
                        // to figure out if we need to use it or not, and only once we picked the latest one, parse the whole state


                        // try and get the state with the highest version out of all the ones with the same master node id
                        ProcessClusterState stateToProcess = processNewClusterStates.poll();
                        if (stateToProcess == null) {
                            return currentState;
                        }
                        stateToProcess.processed = true;
                        while (true) {
                            ProcessClusterState potentialState = processNewClusterStates.peek();
                            // nothing else in the queue, bail
                            if (potentialState == null) {
                                break;
                            }
                            // if its not from the same master, then bail
                            if (!Objects.equal(stateToProcess.clusterState.nodes().masterNodeId(), potentialState.clusterState.nodes().masterNodeId())) {
                                break;
                            }

                            // we are going to use it for sure, poll (remove) it
                            potentialState = processNewClusterStates.poll();
                            potentialState.processed = true;

                            if (potentialState.clusterState.version() > stateToProcess.clusterState.version()) {
                                // we found a new one
                                stateToProcess = potentialState;
                            }
                        }

                        ClusterState updatedState = stateToProcess.clusterState;

                        // if the new state has a smaller version, and it has the same master node, then no need to process it
                        if (updatedState.version() < currentState.version() && Objects.equal(updatedState.nodes().masterNodeId(), currentState.nodes().masterNodeId())) {
                            return currentState;
                        }

                        // we don't need to do this, since we ping the master, and get notified when it has moved from being a master
                        // because it doesn't have enough master nodes...
                        //if (!electMaster.hasEnoughMasterNodes(newState.nodes())) {
                        //    return disconnectFromCluster(newState, "not enough master nodes on new cluster state received from [" + newState.nodes().masterNode() + "]");
                        //}

                        latestDiscoNodes = updatedState.nodes();

                        // check to see that we monitor the correct master of the cluster
                        if (masterFD.masterNode() == null || !masterFD.masterNode().equals(latestDiscoNodes.masterNode())) {
                            masterFD.restart(latestDiscoNodes.masterNode(), "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
                        }

                        ClusterState.Builder builder = ClusterState.builder(updatedState);
                        // if the routing table did not change, use the original one
                        if (updatedState.routingTable().version() == currentState.routingTable().version()) {
                            builder.routingTable(currentState.routingTable());
                        }
                        // same for metadata
                        if (updatedState.metaData().version() == currentState.metaData().version()) {
                            builder.metaData(currentState.metaData());
                        } else {
                            // if its not the same version, only copy over new indices or ones that changed the version
                            MetaData.Builder metaDataBuilder = MetaData.builder(updatedState.metaData()).removeAllIndices();
                            for (IndexMetaData indexMetaData : updatedState.metaData()) {
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
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                        newStateProcessed.onNewClusterStateFailed(t);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();
                        newStateProcessed.onNewClusterStateProcessed();
                    }
                });
            }
        }
    }

    private ClusterState handleJoinRequest(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticsearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
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

            clusterService.submitStateUpdateTask("zen-disco-receive(join from node[" + node + "])", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.nodes().nodeExists(node.id())) {
                        // the node already exists in the cluster
                        logger.warn("received a join request for an existing node [{}]", node);
                        // still send a new cluster state, so it will be re published and possibly update the other node
                        return ClusterState.builder(currentState).build();
                    }
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes());
                    for (DiscoveryNode existingNode : currentState.nodes()) {
                        if (node.address().equals(existingNode.address())) {
                            builder.remove(existingNode.id());
                            logger.warn("received join request from node [{}], but found existing node {} with same address, removing existing node", node, existingNode);
                        }
                    }
                    latestDiscoNodes = builder.build();
                    // add the new node now (will update latestDiscoNodes on publish)
                    return ClusterState.builder(currentState).nodes(latestDiscoNodes.newNode(node)).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
        }
        return state;
    }

    private DiscoveryNode findMaster() {
        ZenPing.PingResponse[] fullPingResponses = pingService.pingAndWait(pingTimeout);
        if (fullPingResponses == null) {
            logger.trace("No full ping responses");
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
                .addGlobalBlock(discoverySettings.getNoMasterBlock())
                .build();

        // clean the nodes, we are now not connected to anybody, since we try and reform the cluster
        latestDiscoNodes = new DiscoveryNodes.Builder(latestDiscoNodes).masterNodeId(null).build();

        asyncJoinCluster();

        return ClusterState.builder(clusterState)
                .blocks(clusterBlocks)
                .nodes(latestDiscoNodes)
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
        public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
            handleNewClusterStateFromMaster(clusterState, newStateProcessed);
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

    static class RejoinClusterRequest extends TransportRequest {

        private String fromNodeId;

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromNodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(fromNodeId);
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
            clusterService.submitStateUpdateTask("received a request to rejoin the cluster from [" + request.fromNodeId + "]", Priority.URGENT, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Exception e) {
                        logger.warn("failed to send response on rejoin cluster request handling", e);
                    }
                    return rejoin(currentState, "received a request to rejoin the cluster from [" + request.fromNodeId + "]");
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int minimumMasterNodes = settings.getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES,
                    ZenDiscovery.this.electMaster.minimumMasterNodes());
            if (minimumMasterNodes != ZenDiscovery.this.electMaster.minimumMasterNodes()) {
                logger.info("updating {} from [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES,
                        ZenDiscovery.this.electMaster.minimumMasterNodes(), minimumMasterNodes);
                handleMinimumMasterNodesChanged(minimumMasterNodes);
            }
        }
    }
}
