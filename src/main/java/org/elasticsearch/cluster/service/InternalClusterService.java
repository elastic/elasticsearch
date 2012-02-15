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

package org.elasticsearch.cluster.service;

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.elasticsearch.cluster.ClusterState.Builder;
import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 *
 */
public class InternalClusterService extends AbstractLifecycleComponent<ClusterService> implements ClusterService {

    private final ThreadPool threadPool;

    private final DiscoveryService discoveryService;

    private final OperationRouting operationRouting;

    private final TransportService transportService;

    private final NodeSettingsService nodeSettingsService;

    private final TimeValue reconnectInterval;

    private volatile ExecutorService updateTasksExecutor;

    private final List<ClusterStateListener> priorityClusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();
    private final List<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();
    private final List<ClusterStateListener> lastClusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();

    private final Queue<NotifyTimeout> onGoingTimeouts = new LinkedTransferQueue<NotifyTimeout>();

    private volatile ClusterState clusterState = newClusterStateBuilder().build();

    private final ClusterBlocks.Builder initialBlocks = ClusterBlocks.builder().addGlobalBlock(Discovery.NO_MASTER_BLOCK);

    private volatile ScheduledFuture reconnectToNodes;

    @Inject
    public InternalClusterService(Settings settings, DiscoveryService discoveryService, OperationRouting operationRouting, TransportService transportService,
                                  NodeSettingsService nodeSettingsService, ThreadPool threadPool) {
        super(settings);
        this.operationRouting = operationRouting;
        this.transportService = transportService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;
        this.nodeSettingsService = nodeSettingsService;

        this.nodeSettingsService.setClusterService(this);

        this.reconnectInterval = componentSettings.getAsTime("reconnect_interval", TimeValue.timeValueSeconds(10));
    }

    public NodeSettingsService settingsService() {
        return this.nodeSettingsService;
    }

    public void addInitialStateBlock(ClusterBlock block) throws ElasticSearchIllegalStateException {
        if (lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("can't set initial block when started");
        }
        initialBlocks.addGlobalBlock(block);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        this.clusterState = newClusterStateBuilder().blocks(initialBlocks).build();
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "clusterService#updateTask"));
        this.reconnectToNodes = threadPool.schedule(reconnectInterval, ThreadPool.Names.CACHED, new ReconnectToNodes());
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        this.reconnectToNodes.cancel(true);
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.cancel();
            onGoingTimeout.listener.onClose();
        }
        updateTasksExecutor.shutdown();
        try {
            updateTasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    @Override
    public DiscoveryNode localNode() {
        return discoveryService.localNode();
    }

    @Override
    public OperationRouting operationRouting() {
        return operationRouting;
    }

    public ClusterState state() {
        return this.clusterState;
    }

    public void addFirst(ClusterStateListener listener) {
        priorityClusterStateListeners.add(listener);
    }

    public void addLast(ClusterStateListener listener) {
        lastClusterStateListeners.add(listener);
    }

    public void add(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void remove(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
        priorityClusterStateListeners.remove(listener);
        lastClusterStateListeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) {
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    public void add(TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
        notifyTimeout.future = threadPool.schedule(timeout, ThreadPool.Names.CACHED, notifyTimeout);
        onGoingTimeouts.add(notifyTimeout);
        clusterStateListeners.add(listener);
        // call the post added notification on the same event thread
        updateTasksExecutor.execute(new Runnable() {
            @Override
            public void run() {
                listener.postAdded();
            }
        });
    }

    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
        if (!lifecycle.started()) {
            return;
        }
        updateTasksExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (!lifecycle.started()) {
                    logger.debug("processing [{}]: ignoring, cluster_service not started", source);
                    return;
                }
                logger.debug("processing [{}]: execute", source);
                ClusterState previousClusterState = clusterState;
                ClusterState newClusterState;
                try {
                    newClusterState = updateTask.execute(previousClusterState);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder("failed to execute cluster state update, state:\nversion [").append(previousClusterState.version()).append("], source [").append(source).append("]\n");
                    sb.append(previousClusterState.nodes().prettyPrint());
                    sb.append(previousClusterState.routingTable().prettyPrint());
                    sb.append(previousClusterState.readOnlyRoutingNodes().prettyPrint());
                    logger.warn(sb.toString(), e);
                    return;
                }

                if (previousClusterState == newClusterState) {
                    logger.debug("processing [{}]: no change in cluster_state", source);
                    return;
                }

                try {
                    if (newClusterState.nodes().localNodeMaster()) {
                        // only the master controls the version numbers
                        Builder builder = ClusterState.builder().state(newClusterState).version(newClusterState.version() + 1);
                        if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                            builder.routingTable(RoutingTable.builder().routingTable(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1));
                        }
                        if (previousClusterState.metaData() != newClusterState.metaData()) {
                            builder.metaData(MetaData.builder().metaData(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
                        }
                        newClusterState = builder.build();
                    } else {
                        if (previousClusterState.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) && !newClusterState.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
                            // force an update, its a fresh update from the master as we transition from a start of not having a master to having one
                            // have a fresh instances of routing and metadata to remove the chance that version might be the same
                            Builder builder = ClusterState.builder().state(newClusterState);
                            builder.routingTable(RoutingTable.builder().routingTable(newClusterState.routingTable()));
                            builder.metaData(MetaData.builder().metaData(newClusterState.metaData()));
                            newClusterState = builder.build();
                            logger.debug("got first state from fresh master [{}]", newClusterState.nodes().masterNodeId());
                        } else if (newClusterState.version() < previousClusterState.version()) {
                            // we got this cluster state from the master, filter out based on versions (don't call listeners)
                            logger.debug("got old cluster state [" + newClusterState.version() + "<" + previousClusterState.version() + "] from source [" + source + "], ignoring");
                            return;
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("cluster state updated:\nversion [").append(newClusterState.version()).append("], source [").append(source).append("]\n");
                        sb.append(newClusterState.nodes().prettyPrint());
                        sb.append(newClusterState.routingTable().prettyPrint());
                        sb.append(newClusterState.readOnlyRoutingNodes().prettyPrint());
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), source);
                    }

                    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, newClusterState, previousClusterState);
                    // new cluster state, notify all listeners
                    final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                    if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                        String summary = nodesDelta.shortSummary();
                        if (summary.length() > 0) {
                            logger.info("{}, reason: {}", summary, source);
                        }
                    }

                    // TODO, do this in parallel (and wait)
                    for (DiscoveryNode node : nodesDelta.addedNodes()) {
                        if (!nodeRequiresConnection(node)) {
                            continue;
                        }
                        try {
                            transportService.connectToNode(node);
                        } catch (Exception e) {
                            // the fault detection will detect it as failed as well
                            logger.warn("failed to connect to node [" + node + "]", e);
                        }
                    }

                    // if we are the master, publish the new state to all nodes
                    // we publish here before we send a notification to all the listeners, since if it fails
                    // we don't want to notify
                    if (newClusterState.nodes().localNodeMaster()) {
                        discoveryService.publish(newClusterState);
                    }

                    // update the current cluster state
                    clusterState = newClusterState;

                    for (ClusterStateListener listener : priorityClusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }
                    for (ClusterStateListener listener : clusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }
                    for (ClusterStateListener listener : lastClusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }

                    if (!nodesDelta.removedNodes().isEmpty()) {
                        threadPool.cached().execute(new Runnable() {
                            @Override
                            public void run() {
                                for (DiscoveryNode node : nodesDelta.removedNodes()) {
                                    transportService.disconnectFromNode(node);
                                }
                            }
                        });
                    }


                    if (updateTask instanceof ProcessedClusterStateUpdateTask) {
                        ((ProcessedClusterStateUpdateTask) updateTask).clusterStateProcessed(newClusterState);
                    }

                    logger.debug("processing [{}]: done applying updated cluster_state", source);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder("failed to apply updated cluster state:\nversion [").append(newClusterState.version()).append("], source [").append(source).append("]\n");
                    sb.append(newClusterState.nodes().prettyPrint());
                    sb.append(newClusterState.routingTable().prettyPrint());
                    sb.append(newClusterState.readOnlyRoutingNodes().prettyPrint());
                    logger.warn(sb.toString(), e);
                }
            }
        });
    }

    class NotifyTimeout implements Runnable {
        final TimeoutClusterStateListener listener;
        final TimeValue timeout;
        ScheduledFuture future;

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            future.cancel(false);
        }

        @Override
        public void run() {
            if (future.isCancelled()) {
                return;
            }
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout);
            }
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }

    private class ReconnectToNodes implements Runnable {
        @Override
        public void run() {
            // master node will check against all nodes if its alive with certain discoveries implementations,
            // but we can't rely on that, so we check on it as well
            for (DiscoveryNode node : clusterState.nodes()) {
                if (lifecycle.stoppedOrClosed()) {
                    return;
                }
                if (!nodeRequiresConnection(node)) {
                    continue;
                }
                if (clusterState.nodes().nodeExists(node.id())) { // we double check existence of node since connectToNode might take time...
                    if (!transportService.nodeConnected(node)) {
                        try {
                            transportService.connectToNode(node);
                        } catch (Exception e) {
                            if (lifecycle.stoppedOrClosed()) {
                                return;
                            }
                            if (clusterState.nodes().nodeExists(node.id())) { // double check here as well, maybe its gone?
                                logger.warn("failed to reconnect to node {}", e, node);
                            }
                        }
                    }
                }
            }
            if (lifecycle.started()) {
                reconnectToNodes = threadPool.schedule(reconnectInterval, ThreadPool.Names.CACHED, this);
            }
        }
    }

    private boolean nodeRequiresConnection(DiscoveryNode node) {
        return localNode().shouldConnectTo(node);
    }
}