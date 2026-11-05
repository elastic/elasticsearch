/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;

/**
 * Periodically deletes old task results from the {@code .tasks} index.
 * <p>
 * Documents in {@code .tasks} carry no write timestamp, so the age of a result is taken from the task's start time,
 * {@code task.start_time_in_millis}: a result is deleted once its task started more than {@link #TASK_RESULTS_RETENTION_PERIOD_SETTING}
 * ago. The retention period is therefore measured from when the task <em>started</em>, not from when it finished, so the result of a task
 * that runs for longer than the retention period can be deleted soon after it completes.
 * <p>
 * Modelled on {@code AsyncTaskMaintenanceService}: the cleanup runs only on the node holding the primary of the {@code .tasks} write
 * index, so there is a single cleaner per project and no coordination between nodes is needed.
 */
public class TaskResultsCleanupService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TaskResultsCleanupService.class);

    /**
     * How long a task result is kept in {@code .tasks}, measured from the start time of the task.
     */
    public static final Setting<TimeValue> TASK_RESULTS_RETENTION_PERIOD_SETTING = Setting.positiveTimeSetting(
        "tasks.results.retention_period",
        TimeValue.timeValueDays(7),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * How long to wait between the end of one cleanup and the start of the next.
     */
    public static final Setting<TimeValue> TASK_RESULTS_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "tasks.results.cleanup_interval",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope,
        Property.Dynamic
    );

    private static final String START_TIME_FIELD = "task.start_time_in_millis";

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final String localNodeId;
    private final ThreadPool threadPool;
    private final Client clientWithOrigin;

    private volatile TimeValue retentionPeriod;
    private volatile TimeValue cleanupInterval;

    private boolean isCleanupRunning;
    private Collection<ProjectId> projectsToCleanup;
    private volatile Scheduler.Cancellable cancellable;

    public TaskResultsCleanupService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        String localNodeId,
        ThreadPool threadPool,
        Client clientWithOrigin
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.localNodeId = localNodeId;
        this.threadPool = threadPool;
        this.clientWithOrigin = clientWithOrigin;
        clusterService.getClusterSettings().initializeAndWatch(TASK_RESULTS_RETENTION_PERIOD_SETTING, v -> this.retentionPeriod = v);
        clusterService.getClusterSettings().initializeAndWatch(TASK_RESULTS_CLEANUP_INTERVAL_SETTING, v -> this.cleanupInterval = v);
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
        stopCleanup();
    }

    @Override
    protected void doClose() throws IOException {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }
        tryStartCleanup(state);
    }

    synchronized void tryStartCleanup(ClusterState state) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        // .tasks is created lazily, so a project without it (or without an assigned primary) is simply skipped
        final List<ProjectId> projectsOnLocalNode = state.metadata().projects().keySet().stream().filter(project -> {
            final IndexAbstraction index = state.projectState(project).metadata().getIndicesLookup().get(TASK_INDEX);
            if (index == null || index.getWriteIndex() == null) {
                return false;
            }
            final IndexRoutingTable indexRouting = state.routingTable(project).index(index.getWriteIndex());
            if (indexRouting == null) {
                return false;
            }
            return localNodeId.equals(indexRouting.shard(0).primaryShard().currentNodeId());
        }).toList();

        if (projectsOnLocalNode.isEmpty()) {
            this.projectsToCleanup = null;
            if (isCleanupRunning) {
                stopCleanup();
            }
        } else {
            this.projectsToCleanup = List.copyOf(projectsOnLocalNode);
            if (isCleanupRunning == false) {
                isCleanupRunning = true;
                executeNextCleanup();
            }
        }
    }

    synchronized void executeNextCleanup() {
        if (isCleanupRunning) {
            final ActionListener<Void> listener = new CountDownActionListener(
                projectsToCleanup.size(),
                ActionListener.running(this::scheduleNextCleanup)
            );
            for (ProjectId project : projectsToCleanup) {
                cleanupIndex(project, listener);
            }
        }
    }

    private void cleanupIndex(ProjectId projectId, ActionListener<Void> listener) {
        final DeleteByQueryRequest toDelete = buildDeleteRequest(System.currentTimeMillis(), retentionPeriod);
        projectResolver.executeOnProject(
            projectId,
            () -> clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, toDelete, ActionListener.wrap(response -> {
                logger.debug("deleted [{}] expired task results from [{}] in project [{}]", response.getDeleted(), TASK_INDEX, projectId);
                listener.onResponse(null);
            }, e -> {
                // A failure is logged and retried on the next run; it must not stop the schedule.
                logger.warn(() -> "failed to delete expired task results from [" + TASK_INDEX + "] in project [" + projectId + "]", e);
                listener.onResponse(null);
            }))
        );
    }

    static DeleteByQueryRequest buildDeleteRequest(long nowInMillis, TimeValue retentionPeriod) {
        final long cutoffInMillis = nowInMillis - retentionPeriod.millis();
        final DeleteByQueryRequest request = new DeleteByQueryRequest(TASK_INDEX).setQuery(
            QueryBuilders.rangeQuery(START_TIME_FIELD).lte(cutoffInMillis)
        );
        // A conflict means the result was rewritten after it was matched; leave it for the next run.
        request.setAbortOnVersionConflict(false);
        return request;
    }

    synchronized void scheduleNextCleanup() {
        if (isCleanupRunning) {
            try {
                cancellable = threadPool.schedule(this::executeNextCleanup, cleanupInterval, threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("failed to schedule next task results cleanup; shutting down", e);
                } else {
                    throw e;
                }
            }
        }
    }

    synchronized void stopCleanup() {
        if (isCleanupRunning) {
            if (cancellable != null && cancellable.isCancelled() == false) {
                cancellable.cancel();
            }
            isCleanupRunning = false;
        }
    }
}
