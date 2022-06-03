/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.health.node.selection.HealthNodeSelector.TASK_NAME;

/**
 * Persistent task executor that is managing the {@link HealthNodeSelector}.
 */
public final class HealthNodeSelectorTaskExecutor extends PersistentTasksExecutor<HealthNodeSelectorTaskParams> {

    private static final Logger logger = LogManager.getLogger(HealthNodeSelector.class);

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final AtomicReference<HealthNodeSelector> currentTask = new AtomicReference<>();
    private final ClusterStateListener taskStarter;

    public HealthNodeSelectorTaskExecutor(ClusterService clusterService, PersistentTasksService persistentTasksService) {
        super(TASK_NAME, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.taskStarter = this::startTask;
        clusterService.addListener(taskStarter);
        clusterService.addListener(this::abortTaskIfApplicable);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, HealthNodeSelectorTaskParams params, PersistentTaskState state) {
        HealthNodeSelector healthNodeSelector = (HealthNodeSelector) task;
        currentTask.set(healthNodeSelector);
        String nodeId = clusterService.localNode().getId();
        logger.info("Node [" + nodeId + "] is selected as the current health node.");
    }

    @Override
    protected HealthNodeSelector createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<HealthNodeSelectorTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new HealthNodeSelector(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    /**
     * Returns the node id from the eligible health nodes
     */
    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        HealthNodeSelectorTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        DiscoveryNode discoveryNode = selectLeastLoadedNode(clusterState, candidateNodes, DiscoveryNode::canContainData);
        logger.info("Assigning health node selector to {}", discoveryNode);
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new PersistentTasksCustomMetadata.Assignment(discoveryNode.getId(), "");
        }
    }

    // visible for testing
    void startTask(ClusterChangedEvent event) {
        // Wait until every node in the cluster is upgraded to 8.4.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)) {
            if (event.localNodeMaster()) {
                clusterService.removeListener(taskStarter);
                if (HealthNodeSelector.findTask(event.state()) == null) {
                    persistentTasksService.sendStartRequest(
                        TASK_NAME,
                        TASK_NAME,
                        new HealthNodeSelectorTaskParams(),
                        ActionListener.wrap(r -> logger.debug("Created the health node selector task"), e -> {
                            Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                            if (t instanceof ResourceAlreadyExistsException == false) {
                                logger.error("Failed to create health node selector task", e);
                            }
                        })
                    );
                }
            } else if (event.state().nodes().getLocalNode().isMasterNode() == false) {
                clusterService.removeListener(taskStarter);
            }
        }
    }

    // visible for testing
    void abortTaskIfApplicable(ClusterChangedEvent event) {
        String nodeId = clusterService.localNode().getId();
        HealthNodeSelector task = currentTask.get();
        if (task != null && task.isCancelled() == false && isNodeShuttingDown(event, nodeId)) {
            logger.info("Node [" + nodeId + "] is releasing health node selector task due to shutdown");
            task.markAsLocallyAborted("Node [" + nodeId + "] is shutting down.");
            currentTask.set(null);
        }
    }

    private boolean isNodeShuttingDown(ClusterChangedEvent event, String nodeId) {
        return NodesShutdownMetadata.isNodeShuttingDown(event.previousState(), nodeId) == false
            && NodesShutdownMetadata.isNodeShuttingDown(event.state(), nodeId);
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(TASK_NAME),
                HealthNodeSelectorTaskParams::fromXContent
            )
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TASK_NAME, HealthNodeSelectorTaskParams::new));
    }
}
