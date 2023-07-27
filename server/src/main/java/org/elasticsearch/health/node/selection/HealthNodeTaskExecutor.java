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
    private final AtomicReference<HealthNode> currentTask = new AtomicReference<>();
    private final ClusterStateListener taskStarter;
    private final ClusterStateListener shutdownListener;
    private volatile boolean enabled;

    private HealthNodeTaskExecutor(ClusterService clusterService, PersistentTasksService persistentTasksService, Settings settings) {
        super(TASK_NAME, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.taskStarter = this::startTask;
        this.shutdownListener = this::shuttingDown;
        this.enabled = ENABLED_SETTING.get(settings);
    }

    public static HealthNodeTaskExecutor create(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        HealthNodeTaskExecutor healthNodeTaskExecutor = new HealthNodeTaskExecutor(clusterService, persistentTasksService, settings);
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
        // Wait until every node in the cluster is upgraded to 8.5.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_5_0)) {
            boolean healthNodeTaskExists = HealthNode.findTask(event.state()) != null;
            boolean isElectedMaster = event.localNodeMaster();
            if (isElectedMaster || healthNodeTaskExists) {
                clusterService.removeListener(taskStarter);
            }
            if (isElectedMaster && healthNodeTaskExists == false) {
                persistentTasksService.sendStartRequest(
                    TASK_NAME,
                    TASK_NAME,
                    new HealthNodeTaskParams(),
                    ActionListener.wrap(r -> logger.debug("Created the health node task"), e -> {
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
    }

    // visible for testing
    void shuttingDown(ClusterChangedEvent event) {
        DiscoveryNode node = clusterService.localNode();
        if (isNodeShuttingDown(event, node.getId())) {
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

    private static boolean isNodeShuttingDown(ClusterChangedEvent event, String nodeId) {
        return event.previousState().metadata().nodeShutdowns().contains(nodeId) == false
            && event.state().metadata().nodeShutdowns().contains(nodeId);
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
