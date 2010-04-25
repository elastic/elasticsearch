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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.UUID;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.util.gcommon.collect.Lists.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.node.DiscoveryNode.*;
import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.util.TimeValue.*;

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

    private final ElectMasterService electMaster;


    private DiscoveryNode localNode;

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private volatile boolean master = false;

    private volatile boolean firstMaster = false;

    private volatile DiscoveryNodes latestDiscoNodes;

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

        this.initialPingTimeout = componentSettings.getAsTime("initial_ping_timeout", timeValueSeconds(3));

        this.electMaster = new ElectMasterService(settings);

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService);
        this.nodesFD.addListener(new NodeFailureListener());

        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
        this.pingService.setNodesProvider(this);
        this.membership = new MembershipAction(settings, transportService, new MembershipListener());
    }

    @Override protected void doStart() throws ElasticSearchException {
        Map<String, String> nodeAttributes = buildCommonNodesAttributes(settings);
        Boolean zenMaster = componentSettings.getAsBoolean("master", null);
        if (zenMaster != null) {
            if (zenMaster.equals(Boolean.FALSE)) {
                nodeAttributes.put("zen.master", "false");
            }
        } else if (nodeAttributes.containsKey("client")) {
            if (nodeAttributes.get("client").equals("true")) {
                nodeAttributes.put("zen.master", "false");
            }
        }
        localNode = new DiscoveryNode(settings.get("name"), UUID.randomUUID().toString(), transportService.boundAddress().publishAddress(), nodeAttributes);
        pingService.start();

        if (nodeAttributes.containsKey("zen.master") && nodeAttributes.get("zen.master").equals("false")) {
            // do the join on a different thread
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    initialJoin();
                }
            });
        } else {
            initialJoin();
        }
    }

    @Override protected void doStop() throws ElasticSearchException {
        pingService.stop();
        if (masterFD.masterNode() != null) {
            masterFD.stop();
        }
        nodesFD.stop();
        initialStateSent.set(false);
        if (!master) {
            try {
                membership.sendLeaveRequestBlocking(latestDiscoNodes.masterNode(), localNode, TimeValue.timeValueSeconds(1));
            } catch (Exception e) {
                logger.debug("Failed to send leave request to master [{}]", e, latestDiscoNodes.masterNode());
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
                    logger.debug("Failed to send leave request from master [{}] to possible master [{}]", e, latestDiscoNodes.masterNode(), possibleMaster);
                }
            }
        }
        master = false;
    }

    @Override protected void doClose() throws ElasticSearchException {
        masterFD.close();
        nodesFD.close();
        publishClusterState.close();
        membership.close();
        pingService.close();
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

    private void initialJoin() {
        boolean retry = true;
        while (retry) {
            retry = false;
            DiscoveryNode masterNode = broadPingTillMasterResolved();
            if (localNode.equals(masterNode)) {
                // we are the master (first)
                this.firstMaster = true;
                this.master = true;
                nodesFD.start(); // start the nodes FD
                clusterService.submitStateUpdateTask("zen-disco-initial_connect(master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                                .localNodeId(localNode.id())
                                .masterNodeId(localNode.id())
                                        // put our local node
                                .put(localNode);
                        // update the fact that we are the master...
                        latestDiscoNodes = builder.build();
                        return newClusterStateBuilder().state(currentState).nodes(builder).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } else {
                this.firstMaster = false;
                this.master = false;
                try {
                    // first, make sure we can connect to the master
                    transportService.connectToNode(masterNode);
                } catch (Exception e) {
                    logger.warn("Failed to connect to master [{}], retrying...", e, masterNode);
                    retry = true;
                    continue;
                }
                // send join request
                try {
                    membership.sendJoinRequestBlocking(masterNode, localNode, initialPingTimeout);
                } catch (Exception e) {
                    logger.warn("Failed to send join request to master [{}], retrying...", e, masterNode);
                    // failed to send the join request, retry
                    retry = true;
                    continue;
                }
                // cool, we found a master, start an FD on it
                masterFD.start(masterNode);
            }
            if (retry) {
                if (!lifecycle.started()) {
                    return;
                }
            }
        }
    }

    private void handleNodeFailure(final DiscoveryNode node) {
        if (!master) {
            // nothing to do here...
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-node_failed(" + node + ")", new ProcessedClusterStateUpdateTask() {
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

    private void handleMasterGone(final DiscoveryNode masterNode, String reason) {
        if (master) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info("Master [{}] left, reason [{}]", masterNode, reason);
        List<DiscoveryNode> nodes = newArrayList(latestDiscoNodes.nodes().values());
        nodes.remove(masterNode); // remove the master node from the list, it has failed
        // sort then
        DiscoveryNode electedMaster = electMaster.electMaster(nodes);
        if (localNode.equals(electedMaster)) {
            this.master = true;
            masterFD.stop();
            nodesFD.start();
            clusterService.submitStateUpdateTask("zen-disco-elected_as_master(old master [" + masterNode + "])", new ProcessedClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                            .putAll(currentState.nodes())
                                    // make sure the old master node, which has failed, is not part of the nodes we publish
                            .remove(masterNode.id())
                            .masterNodeId(localNode.id());
                    // update the fact that we are the master...
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                }

                @Override public void clusterStateProcessed(ClusterState clusterState) {
                    sendInitialStateEventIfNeeded();
                }
            });
        } else {
            nodesFD.stop();
            if (electedMaster != null) {
                // we are not the master, start FD against the possible master
                masterFD.restart(electedMaster);
            } else {
                masterFD.stop();
            }
        }
    }

    void handleNewClusterState(final ClusterState clusterState) {
        if (master) {
            logger.warn("Master should not receive new cluster state from [{}]", clusterState.nodes().masterNode());
        } else {
            latestDiscoNodes = clusterState.nodes();

            // check to see that we monitor the correct master of the cluster
            if (masterFD.masterNode() != null && masterFD.masterNode().equals(latestDiscoNodes.masterNode())) {
                masterFD.restart(latestDiscoNodes.masterNode());
            }

            if (clusterState.nodes().localNode() == null) {
                logger.warn("Received a cluster state from [{}] and not part of the cluster, should not happen", clusterState.nodes().masterNode());
            } else {
                clusterService.submitStateUpdateTask("zen-disco-receive(from [" + clusterState.nodes().masterNode() + "])", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
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
            clusterService.submitStateUpdateTask("zen-disco-node_failed(" + node + ")", new ClusterStateUpdateTask() {
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

    private void handleJoinRequest(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
        }
        if (!transportService.addressSupported(node.address().getClass())) {
            // TODO, what should we do now? Maybe inform that node that its crap?
            logger.warn("Received a wrong address type from [{}], ignoring...", node);
        } else {
            clusterService.submitStateUpdateTask("zen-disco-receive(from node[" + node + "])", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    if (currentState.nodes().nodeExists(node.id())) {
                        // no change, the node already exists in the cluster
                        logger.warn("Received an existing node [{}]", node);
                        return currentState;
                    }
                    return newClusterStateBuilder().state(currentState).nodes(currentState.nodes().newNode(node)).build();
                }
            });
        }
    }

    private DiscoveryNode broadPingTillMasterResolved() {
        while (true) {
            ZenPing.PingResponse[] pingResponses = pingService.pingAndWait(initialPingTimeout);
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
        }
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
            handleNewClusterState(clusterState);
        }
    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override public void onJoin(DiscoveryNode node) {
            handleJoinRequest(node);
        }

        @Override public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFailureListener implements NodesFaultDetection.Listener {

        @Override public void onNodeFailure(DiscoveryNode node) {
            handleNodeFailure(node);
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override public void onMasterFailure(DiscoveryNode masterNode) {
            handleMasterGone(masterNode, "failure");
        }

        @Override public void onDisconnectedFromMaster() {
            // got disconnected from the master, send a join request
            membership.sendJoinRequest(latestDiscoNodes.masterNode(), localNode);
        }
    }
}
