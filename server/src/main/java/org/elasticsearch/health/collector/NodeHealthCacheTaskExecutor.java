/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.collector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.health.collector.NodeHealthCache.NODE_HEALTH_STATE_COLLECTOR;

/**
 * Persistent task executor that is responsible for starting {@link NodeHealthCache} after task is allocated by a health node.
 * Also bootstraps node health cache task on a clean cluster and updates the selected health node of the cluster.
 */
public final class NodeHealthCacheTaskExecutor extends PersistentTasksExecutor<NodeHealthCacheTaskParams> implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NodeHealthCache.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final PersistentTasksService persistentTasksService;
    private final AtomicReference<NodeHealthCache> currentTask = new AtomicReference<>();

    public NodeHealthCacheTaskExecutor(Client client, ClusterService clusterService, ThreadPool threadPool) {
        super(NODE_HEALTH_STATE_COLLECTOR, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        clusterService.addListener(this);
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, NodeHealthCacheTaskParams params, PersistentTaskState state) {
        NodeHealthCache nodeHealthCache = (NodeHealthCache) task;
        currentTask.set(nodeHealthCache);
        nodeHealthCache.run();
    }

    @Override
    protected NodeHealthCache createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<NodeHealthCacheTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new NodeHealthCache(
            clusterService,
            threadPool,
            settings,
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers
        );
    }

    /**
     * Returns the node id from the eligible health nodes and updates the cluster state with the chosen health node
     */
    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        NodeHealthCacheTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        DiscoveryNode discoveryNode = selectLeastLoadedNode(clusterState, candidateNodes, DiscoveryNode::isHealthNode);
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new PersistentTasksCustomMetadata.Assignment(discoveryNode.getId(), "");
        }
    }

    private void startTask() {
        persistentTasksService.sendStartRequest(
            NODE_HEALTH_STATE_COLLECTOR,
            NODE_HEALTH_STATE_COLLECTOR,
            new NodeHealthCacheTaskParams(),
            ActionListener.wrap(r -> logger.debug("Started node health cache task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create node health cache task", e);
                }
            })
        );
    }

    public NodeHealthCache getCurrentTask() {
        return currentTask.get();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }

        startTask();
    }
}
