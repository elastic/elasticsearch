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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
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

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.node.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final AtomicReference<HealthNodeSelector> currentTask = new AtomicReference<>();
    private final ClusterStateListener taskStarter;
    private final ClusterStateListener shutdownListener;

    public HealthNodeSelectorTaskExecutor(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        super(TASK_NAME, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.taskStarter = this::startTask;
        this.shutdownListener = this::shuttingDown;
        if (ENABLED_SETTING.get(settings)) {
            clusterService.addListener(taskStarter);
            clusterService.addListener(shutdownListener);
        }
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
    }

    private void enable(boolean enabled) {
        if (enabled) {
            clusterService.addListener(taskStarter);
            clusterService.addListener(shutdownListener);
        } else {
            clusterService.removeListener(taskStarter);
            clusterService.removeListener(shutdownListener);
            abortTaskIfApplicable("disabling health node via '" + ENABLED_SETTING.getKey() + "'");
        }
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
            // If the node is not a master node or if the task already exists this node should not start the task anymore
            if (event.state().nodes().getLocalNode().isMasterNode() == false || HealthNodeSelector.findTask(event.state()) != null) {
                clusterService.removeListener(taskStarter);
            } else if (event.localNodeMaster()) {
                clusterService.removeListener(taskStarter);
                persistentTasksService.sendStartRequest(
                    TASK_NAME,
                    TASK_NAME,
                    new HealthNodeSelectorTaskParams(),
                    ActionListener.wrap(r -> logger.debug("Created the health node selector task"), e -> {
                        Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                        if (t instanceof ResourceAlreadyExistsException == false) {
                            logger.error("Failed to create the health node selector task", e);
                        }
                    })
                );

            }
        }
    }

    // visible for testing
    void shuttingDown(ClusterChangedEvent event) {
        String nodeId = clusterService.localNode().getId();
        if (isNodeShuttingDown(event, nodeId)) {
            abortTaskIfApplicable("node [" + nodeId + "] shutting down");
        }
    }

    // visible for testing
    void abortTaskIfApplicable(String reason) {
        HealthNodeSelector task = currentTask.get();
        if (task != null && task.isCancelled() == false) {
            logger.info("Aborting health node selector task due to {}.", reason);
            task.markAsLocallyAborted(reason);
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
