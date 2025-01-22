/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.node.NodeClosedException;
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

import static org.elasticsearch.health.node.selection.HealthNode.TASK_NAME;

/**
 * Persistent task executor that is managing the {@link HealthNode}.
 */
public final class HealthNodeTaskExecutor extends PersistentTasksExecutor<HealthNodeTaskParams> {

    private static final Logger logger = LogManager.getLogger(HealthNodeTaskExecutor.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.node.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final FeatureService featureService;
    private final AtomicReference<HealthNode> currentTask = new AtomicReference<>();
    private final ClusterStateListener taskStarter;
    private final ClusterStateListener shutdownListener;
    private volatile boolean enabled;

    private HealthNodeTaskExecutor(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        Settings settings
    ) {
        super(TASK_NAME, clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.featureService = featureService;
        this.taskStarter = this::startTask;
        this.shutdownListener = this::shuttingDown;
        this.enabled = ENABLED_SETTING.get(settings);
    }

    public static HealthNodeTaskExecutor create(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        HealthNodeTaskExecutor healthNodeTaskExecutor = new HealthNodeTaskExecutor(
            clusterService,
            persistentTasksService,
            featureService,
            settings
        );
        healthNodeTaskExecutor.registerListeners(clusterSettings);
        return healthNodeTaskExecutor;
    }

    private void registerListeners(ClusterSettings clusterSettings) {
        if (this.enabled) {
            clusterService.addListener(taskStarter);
            clusterService.addListener(shutdownListener);
        }
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
    }

    private void enable(boolean enabled) {
        this.enabled = enabled;
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
    protected void nodeOperation(AllocatedPersistentTask task, HealthNodeTaskParams params, PersistentTaskState state) {
        HealthNode healthNode = (HealthNode) task;
        currentTask.set(healthNode);
        DiscoveryNode node = clusterService.localNode();
        logger.info("Node [{{}}{{}}] is selected as the current health node.", node.getName(), node.getId());
    }

    @Override
    protected HealthNode createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<HealthNodeTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new HealthNode(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    /**
     * Returns the node id from the eligible health nodes
     */
    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        HealthNodeTaskParams params,
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
        // Wait until master is stable before starting health task
        if (event.localNodeMaster() && event.state().clusterRecovered() && HealthNode.findTask(event.state()) == null) {
            persistentTasksService.sendStartRequest(
                TASK_NAME,
                TASK_NAME,
                new HealthNodeTaskParams(),
                null,
                ActionListener.wrap(r -> logger.debug("Created the health node task"), e -> {
                    if (e instanceof NodeClosedException) {
                        logger.debug("Failed to create health node task because node is shutting down", e);
                        return;
                    }
                    Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                    if (t instanceof ResourceAlreadyExistsException == false) {
                        logger.error("Failed to create the health node task", e);
                        if (enabled) {
                            clusterService.addListener(taskStarter);
                        }
                    }
                })
            );
        }
    }

    // visible for testing
    void shuttingDown(ClusterChangedEvent event) {
        if (isNodeShuttingDown(event)) {
            var node = event.state().getNodes().getLocalNode();
            abortTaskIfApplicable("node [{" + node.getName() + "}{" + node.getId() + "}] shutting down");
        }
    }

    // visible for testing
    void abortTaskIfApplicable(String reason) {
        HealthNode task = currentTask.get();
        if (task != null && task.isCancelled() == false) {
            logger.info("Aborting health node task due to {}.", reason);
            task.markAsLocallyAborted(reason);
            currentTask.set(null);
        }
    }

    private static boolean isNodeShuttingDown(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return false;
        }
        var shutdownsOld = event.previousState().metadata().nodeShutdowns();
        var shutdownsNew = event.state().metadata().nodeShutdowns();
        if (shutdownsNew == shutdownsOld) {
            return false;
        }
        String nodeId = event.state().nodes().getLocalNodeId();
        return shutdownsOld.contains(nodeId) == false && shutdownsNew.contains(nodeId);

    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TASK_NAME), HealthNodeTaskParams::fromXContent)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TASK_NAME, HealthNodeTaskParams::new));
    }
}
