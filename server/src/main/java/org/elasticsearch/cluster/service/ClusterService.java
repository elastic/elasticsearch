/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

public class ClusterService extends AbstractLifecycleComponent {
    private final MasterService masterService;

    private final ClusterApplierService clusterApplierService;

    public static final org.elasticsearch.common.settings.Setting.AffixSetting<String> USER_DEFINED_METADATA = Setting.prefixKeySetting(
        "cluster.metadata.",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    /**
     * The node's settings.
     */
    private final Settings settings;

    private final ClusterName clusterName;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private final String nodeName;

    private RerouteService rerouteService;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, TaskManager taskManager) {
        this(
            settings,
            clusterSettings,
            new MasterService(settings, clusterSettings, threadPool, taskManager),
            new ClusterApplierService(Node.NODE_NAME_SETTING.get(settings), settings, clusterSettings, threadPool)
        );
    }

    public ClusterService(
        Settings settings,
        ClusterSettings clusterSettings,
        MasterService masterService,
        ClusterApplierService clusterApplierService
    ) {
        this.settings = settings;
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.masterService = masterService;
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        // Add a no-op update consumer so changes are logged
        this.clusterSettings.addAffixUpdateConsumer(USER_DEFINED_METADATA, (first, second) -> {}, (first, second) -> {});
        this.clusterApplierService = clusterApplierService;
    }

    public ThreadPool threadPool() {
        return clusterApplierService.threadPool();
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        clusterApplierService.setNodeConnectionsService(nodeConnectionsService);
    }

    public void setRerouteService(RerouteService rerouteService) {
        assert this.rerouteService == null : "RerouteService is already set";
        this.rerouteService = rerouteService;
    }

    public RerouteService getRerouteService() {
        assert this.rerouteService != null : "RerouteService not set";
        return rerouteService;
    }

    @Override
    protected synchronized void doStart() {
        clusterApplierService.start();
        masterService.start();
    }

    @Override
    protected synchronized void doStop() {
        masterService.stop();
        clusterApplierService.stop();
    }

    @Override
    protected synchronized void doClose() {
        masterService.close();
        clusterApplierService.close();
    }

    /**
     * The local node.
     */
    public DiscoveryNode localNode() {
        DiscoveryNode localNode = state().getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    /**
     * The currently applied cluster state.
     * TODO: Should be renamed to appliedState / appliedClusterState
     */
    public ClusterState state() {
        return clusterApplierService.state();
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addHighPriorityApplier(applier);
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addLowPriorityApplier(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        clusterApplierService.addStateApplier(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        clusterApplierService.removeApplier(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterApplierService.addListener(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterApplierService.removeListener(listener);
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        clusterApplierService.addLocalNodeMasterListener(listener);
    }

    public MasterService getMasterService() {
        return masterService;
    }

    public ClusterApplierService getClusterApplierService() {
        return clusterApplierService;
    }

    public static boolean assertClusterOrMasterStateThread() {
        return ThreadPool.assertCurrentThreadPool(
            ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME,
            MasterService.MASTER_UPDATE_THREAD_NAME
        );
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    /**
     * The node's settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * The name of this node.
     */
    public final String getNodeName() {
        return nodeName;
    }

    /**
     * Submits an unbatched cluster state update task. This method exists for legacy reasons but is deprecated and forbidden in new
     * production code because unbatched tasks are a source of performance and stability bugs. You should instead implement your update
     * logic in a dedicated {@link ClusterStateTaskExecutor} which is reused across multiple task instances. The task itself is typically
     * just a collection of parameters consumed by the executor, together with any listeners to be notified when execution completes.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     */
    @Deprecated
    @SuppressForbidden(reason = "this method is itself forbidden")
    public void submitUnbatchedStateUpdateTask(String source, ClusterStateUpdateTask updateTask) {
        masterService.submitUnbatchedStateUpdateTask(source, updateTask);
    }

    /**
     * Create a new task queue which can be used to submit tasks for execution by the master service. Tasks submitted to the same queue
     * (while the master service is otherwise busy) will be batched together into a single cluster state update. You should therefore re-use
     * each queue as much as possible.
     *
     * @param name The name of the queue, which is mostly useful for debugging.
     *
     * @param priority The priority at which tasks submitted to the queue are executed. Avoid priorites other than {@link Priority#NORMAL}
     *                 where possible. A stream of higher-priority tasks can starve lower-priority ones from running. Higher-priority tasks
     *                 should definitely re-use the same {@link MasterServiceTaskQueue} so that they are executed in batches.
     *
     * @param executor The executor which processes each batch of tasks.
     *
     * @param <T> The type of the tasks
     *
     * @return A new batching task queue.
     */
    public <T extends ClusterStateTaskListener> MasterServiceTaskQueue<T> createTaskQueue(
        String name,
        Priority priority,
        ClusterStateTaskExecutor<T> executor
    ) {
        return masterService.createTaskQueue(name, priority, executor);
    }
}
