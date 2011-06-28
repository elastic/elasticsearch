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

package org.elasticsearch.discovery.zookeeper;

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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.zookeeper.AbstractNodeListener;
import org.elasticsearch.zookeeper.ZooKeeperClient;
import org.elasticsearch.zookeeper.ZooKeeperClientSessionExpiredException;
import org.elasticsearch.zookeeper.ZooKeeperEnvironment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNode.buildCommonNodesAttributes;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;

/**
 * @author imotov
 */
public class ZooKeeperDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {
    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ClusterName clusterName;

    private final ThreadPool threadPool;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private final ZooKeeperClient zooKeeperClient;

    private DiscoveryNode localNode;

    private String localNodePath;

    private final PublishClusterStateAction publishClusterState;

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final Lock updateNodeListLock = new ReentrantLock();

    private final MasterNodeListChangedListener masterNodeListChangedListener = new MasterNodeListChangedListener();

    private final SessionResetListener sessionResetListener = new SessionResetListener();

    private final ZooKeeperEnvironment environment;

    @Inject public ZooKeeperDiscovery(Settings settings, ZooKeeperEnvironment environment, ClusterName clusterName, ThreadPool threadPool,
                                      TransportService transportService, ClusterService clusterService,
                                      ZooKeeperClient zooKeeperClient) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.zooKeeperClient = zooKeeperClient;
        this.threadPool = threadPool;
        this.environment = environment;
        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
    }

    @Override protected void doStart() throws ElasticSearchException {
        Map<String, String> nodeAttributes = buildCommonNodesAttributes(settings);
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        String nodeId = UUID.randomBase64UUID();
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes);
        localNodePath = nodePath(localNode.id());
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        initialStateSent.set(false);
        zooKeeperClient.start();
        zooKeeperClient.addSessionResetListener(sessionResetListener);
        try {
            zooKeeperClient.createPersistentNode(environment.clusterNodePath());
            zooKeeperClient.createPersistentNode(environment.nodesNodePath());
        } catch (InterruptedException ex) {
            // Ignore
        }

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster(true);
    }

    @Override protected void doStop() throws ElasticSearchException {
        zooKeeperClient.removeSessionResetListener(sessionResetListener);
        logger.trace("Stopping zooKeeper client");
        zooKeeperClient.stop();
        logger.trace("Stopped zooKeeper client");
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
        zooKeeperClient.close();
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


    @Override public void publish(ClusterState clusterState) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        if (!lifecycle.started()) {
            return;
        }
        try {
            latestDiscoNodes = clusterState.nodes();
            publishClusterState.publish(clusterState);
        } catch (ZooKeeperClientSessionExpiredException ex) {
            // Ignore
        } catch (Exception ex) {
            logger.error("Cannot publish state", ex);
        }
    }

    @Override public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return newNodesBuilder().put(localNode).localNodeId(localNode.id()).build();
    }

    private void asyncJoinCluster(final boolean initial) {
        threadPool.cached().execute(new Runnable() {
            @Override public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innerJoinCluster(initial);
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    private void innerJoinCluster(boolean initial) {
        try {
            if (!initial || register()) {
                // Check if node should propose itself as a master
                if (localNode.isMasterNode()) {
                    electMaster();
                } else {
                    findMaster(initial);
                }
            }
        } catch (InterruptedException ex) {
            // Ignore
        }
    }

    private boolean register() {
        if (lifecycle.stoppedOrClosed()) {
            return false;
        }
        try {
            // Create an ephemeral node that contains our nodeInfo
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            localNode.writeTo(streamOutput);
            byte[] buf = streamOutput.copiedByteArray();
            zooKeeperClient.setOrCreateTransientNode(localNodePath, buf);
            return true;
        } catch (Exception ex) {
            restartDiscovery();
            return false;
        }
    }

    private void findMaster(final boolean initial) throws InterruptedException {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        ZooKeeperClient.NodeListener nodeListener = new AbstractNodeListener() {
            @Override public void onNodeCreated(String id) {
                handleMasterAppeared(initial);
            }

            @Override public void onNodeDeleted(String id) {
                handleMasterGone();
            }
        };

        if (zooKeeperClient.getNode(environment.masterNodePath(), nodeListener) == null) {
            if (!initial) {
                removeMaster();
            }
        } else {
            addMaster();
        }
    }

    private void electMaster() {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        ZooKeeperClient.NodeListener nodeListener = new AbstractNodeListener() {
            @Override public void onNodeDeleted(String id) {
                handleMasterGone();
            }
        };
        byte[] masterId = localNode().id().getBytes();

        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        try {
            byte[] electedMasterId = zooKeeperClient.getOrCreateTransientNode(environment.masterNodePath(), masterId, nodeListener);
            String electedMasterIdStr = new String(electedMasterId);
            if (localNode.id().equals(electedMasterIdStr)) {
                becomeMaster();
            } else {
                addMaster();
            }
        } catch (Exception ex) {
            logger.error("Couldn't elect master. Restarting discovery.", ex);
            restartDiscovery();
        }
    }

    private void addMaster() {
        master = false;
    }

    private void removeMaster() {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-no-master (no_master_found)", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                RoutingTable routingTable = currentState.routingTable();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).addGlobalBlock(NO_MASTER_BLOCK).build();
                // if this is a data node, clean the metadata and routing, since we want to recreate the indices and shards
                if (currentState.nodes().localNode() != null && currentState.nodes().localNode().dataNode()) {
                    metaData = MetaData.newMetaDataBuilder().build();
                    routingTable = RoutingTable.newRoutingTableBuilder().build();
                }
                DiscoveryNodes.Builder builder = DiscoveryNodes.newNodesBuilder()
                        .putAll(currentState.nodes());
                DiscoveryNode masterNode = currentState.nodes().masterNode();
                if (masterNode != null) {
                    builder = builder.remove(masterNode.id());
                }
                latestDiscoNodes = builder.build();
                return newClusterStateBuilder().state(currentState)
                        .blocks(clusterBlocks)
                        .nodes(latestDiscoNodes)
                        .metaData(metaData)
                        .routingTable(routingTable)
                        .build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }
        });
    }

    private void becomeMaster() throws InterruptedException {
        this.master = true;
        clusterService.submitStateUpdateTask("zen-disco-join (elected_as_master)", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder();
                // Make sure that the current node is present
                builder.putAll(currentState.nodes());
                if (currentState.nodes().localNode() == null) {
                    builder.put(localNode);
                }
                // update the fact that we are the master...
                builder.localNodeId(localNode.id()).masterNodeId(localNode.id());
                latestDiscoNodes = builder.build();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK).build();
                return newClusterStateBuilder().state(currentState).nodes(builder).blocks(clusterBlocks).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }
        });

        handleUpdateNodeList();
    }


    private void restartDiscovery() {
        if (!lifecycle.started()) {
            return;
        }
        master = false;
        asyncJoinCluster(true);
    }

    private void processDeletedNode(final String nodeId) {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-node_left(" + nodeId + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                if (currentState.nodes().nodeExists(nodeId)) {
                    DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                            .putAll(currentState.nodes())
                            .remove(nodeId);
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                } else {
                    logger.warn("Trying to deleted a node that doesn't exist {}", nodeId);
                    return currentState;
                }
            }
        });

    }

    private void processAddedNode(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
        }

        if (!transportService.addressSupported(node.address().getClass())) {
            logger.warn("received a wrong address type from [{}], ignoring...", node);
        } else {
            clusterService.submitStateUpdateTask("zoo-keeper-disco-receive(join from node[" + node + "])", new ClusterStateUpdateTask() {
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
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    private void handleNewClusterStateFromMaster(final ClusterState clusterState) {
        if (!lifecycle.started()) {
            return;
        }
        if (!master) {
            // Make sure that we are part of the state
            if (clusterState.nodes().localNode() != null) {
                clusterService.submitStateUpdateTask("zoo-keeper-disco-receive(from master [" + clusterState.nodes().masterNode() + "])", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        latestDiscoNodes = clusterState.nodes();
                        return clusterState;
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } else {
                logger.trace("Received new state, but not part of the state");
            }
        } else {
            logger.warn("Received new state, but node is master");
        }
    }

    private void handleUpdateNodeList() {
        if (!lifecycle.started()) {
            return;
        }
        if (!master) {
            return;
        }
        logger.trace("Updating node list");
        boolean restart = false;
        updateNodeListLock.lock();
        try {
            Set<String> currentNodes = latestDiscoNodes.nodes().keySet();
            Set<String> nodes = zooKeeperClient.listNodes(environment.nodesNodePath(), masterNodeListChangedListener);
            Set<String> deleted = new HashSet<String>(currentNodes);
            deleted.removeAll(nodes);
            Set<String> added = new HashSet<String>(nodes);
            added.removeAll(currentNodes);
            for (String nodeId : deleted) {
                processDeletedNode(nodeId);
            }
            for (String nodeId : added) {
                if (!nodeId.equals(localNode.id())) {
                    DiscoveryNode node = nodeInfo(nodeId);
                    if (node != null) {
                        processAddedNode(node);
                    }
                }
            }
        } catch (ZooKeeperClientSessionExpiredException ex) {
            restart = true;
        } catch (Exception ex) {
            restart = true;
            logger.error("Couldn't update node list.", ex);
        } finally {
            updateNodeListLock.unlock();
        }
        if (restart) {
            restartDiscovery();
        }
    }

    public DiscoveryNode nodeInfo(final String id) throws ElasticSearchException, InterruptedException {
        try {
            byte[] buf = zooKeeperClient.getNode(nodePath(id), null);
            if (buf != null) {
                return DiscoveryNode.readNode(new BytesStreamInput(buf));
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Cannot get node info " + id, e);
        }
    }

    private String nodePath(String id) {
        return environment.nodesNodePath() + "/" + id;
    }


    private void handleMasterGone() {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("Master is gone");
        asyncJoinCluster(false);
    }

    private void handleMasterAppeared(boolean initial) {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("New master appeared");
        asyncJoinCluster(initial);
    }

    private class MasterNodeListChangedListener implements ZooKeeperClient.NodeListChangedListener {

        @Override public void onNodeListChanged() {
            handleUpdateNodeList();
        }
    }

    private class NewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {
        @Override public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

    private class SessionResetListener implements ZooKeeperClient.SessionResetListener {

        @Override public void sessionReset() {
            restartDiscovery();
        }
    }

}
