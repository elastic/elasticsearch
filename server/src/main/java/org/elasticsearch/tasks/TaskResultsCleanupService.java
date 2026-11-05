/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;

/**
 * Periodically deletes old task results from the {@code .tasks} index.
 * <p>
 * Documents in {@code .tasks} carry no write timestamp, so the age of a result is approximated by the time the task finished:
 * {@code task.start_time_in_millis + task.running_time_in_nanos}. Both are captured when the result is built, so this is within a
 * relocation handoff of the time the document was written.
 * <p>
 * {@code .tasks} is owned by {@code server}, and server has neither the delete-by-query implementation nor a scripting language, so the
 * sum cannot be pushed down to a query. Instead the cleanup pages through the documents that <em>might</em> be expired (those whose start
 * time is before the cutoff, which is a sound superset because running time is never negative), computes the exact completion time on
 * the node, and bulk deletes the expired ones.
 * <p>
 * Modelled on {@code AsyncTaskMaintenanceService}: the cleanup runs only on the node holding the primary of the {@code .tasks} write
 * index, so there is a single cleaner per project and no coordination between nodes is needed.
 */
public class TaskResultsCleanupService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TaskResultsCleanupService.class);

    /**
     * How long a task result is kept in {@code .tasks} after the task finished.
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

    /**
     * The number of candidate documents examined per search, and so the largest bulk delete issued at once.
     */
    static final int PAGE_SIZE = 1000;

    private static final String START_TIME_FIELD = "task.start_time_in_millis";
    private static final String RUNNING_TIME_FIELD = "task.running_time_in_nanos";
    private static final String NODE_FIELD = "task.node";
    private static final String ID_FIELD = "task.id";

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

        // .tasks is created lazily, so a project without it (or without an assigned primary) is simply skipped. Only data nodes can
        // hold a primary, so no node role check is needed.
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
                cleanupProject(project, listener);
            }
        }
    }

    private void cleanupProject(ProjectId projectId, ActionListener<Void> listener) {
        final long cutoffInMillis = System.currentTimeMillis() - retentionPeriod.millis();
        // A failure is logged and the project is retried on the next run; it must not stop the schedule.
        cleanupPage(projectId, cutoffInMillis, null, 0, ActionListener.wrap(deleted -> {
            logger.debug("deleted [{}] expired task results from [{}] in project [{}]", deleted, TASK_INDEX, projectId);
            listener.onResponse(null);
        }, e -> {
            logger.warn(() -> "failed to delete expired task results from [" + TASK_INDEX + "] in project [" + projectId + "]", e);
            listener.onResponse(null);
        }));
    }

    /**
     * Examines one page of candidates, deletes the expired ones and continues with the next page until the candidates are exhausted.
     * Pages are ordered by (start time, node, id), which is unique per document, so {@code search_after} needs no point in time. A
     * concurrent change can at worst make a page skip a document, which the next run picks up.
     */
    private void cleanupPage(
        ProjectId projectId,
        long cutoffInMillis,
        @Nullable Object[] searchAfter,
        long deletedSoFar,
        ActionListener<Long> listener
    ) {
        final SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery(START_TIME_FIELD).lte(cutoffInMillis))
            .size(PAGE_SIZE)
            .trackTotalHits(false)
            // the result body can be large and is not needed; only the two doc values the age is computed from are read
            .fetchSource(false)
            .docValueField(START_TIME_FIELD)
            .docValueField(RUNNING_TIME_FIELD)
            .seqNoAndPrimaryTerm(true)
            .sort(START_TIME_FIELD, SortOrder.ASC)
            .sort(NODE_FIELD, SortOrder.ASC)
            .sort(ID_FIELD, SortOrder.ASC);
        if (searchAfter != null) {
            source.searchAfter(searchAfter);
        }
        final SearchRequest searchRequest = new SearchRequest(TASK_INDEX).source(source);

        projectResolver.executeOnProject(
            projectId,
            () -> clientWithOrigin.search(searchRequest, listener.delegateFailureAndWrap((l, response) -> {
                // The response is released once this listener returns, so copy out everything that is needed first.
                final SearchHit[] hits = response.getHits().getHits();
                final List<DeleteRequest> expired = new ArrayList<>();
                for (SearchHit hit : hits) {
                    if (isExpired(hit, cutoffInMillis)) {
                        expired.add(
                            new DeleteRequest(TASK_INDEX).id(hit.getId()).setIfSeqNo(hit.getSeqNo()).setIfPrimaryTerm(hit.getPrimaryTerm())
                        );
                    }
                }
                final boolean lastPage = hits.length < PAGE_SIZE;
                final Object[] nextSearchAfter = hits.length == 0 ? null : hits[hits.length - 1].getSortValues();

                final ActionListener<Long> afterDelete = l.delegateFailureAndWrap((l2, deletedInPage) -> {
                    final long deleted = deletedSoFar + deletedInPage;
                    if (lastPage) {
                        l2.onResponse(deleted);
                    } else {
                        cleanupPage(projectId, cutoffInMillis, nextSearchAfter, deleted, l2);
                    }
                });
                if (expired.isEmpty()) {
                    afterDelete.onResponse(0L);
                } else {
                    deleteExpired(projectId, expired, afterDelete);
                }
            }))
        );
    }

    /**
     * A candidate is expired if it finished before the cutoff. A document missing either field is skipped rather than failing the page.
     */
    private static boolean isExpired(SearchHit hit, long cutoffInMillis) {
        final var startTime = hit.field(START_TIME_FIELD);
        final var runningTime = hit.field(RUNNING_TIME_FIELD);
        if (startTime == null || runningTime == null) {
            return false;
        }
        final long startTimeInMillis = ((Number) startTime.getValue()).longValue();
        final long runningTimeInNanos = ((Number) runningTime.getValue()).longValue();
        return startTimeInMillis + TimeUnit.NANOSECONDS.toMillis(runningTimeInNanos) <= cutoffInMillis;
    }

    private void deleteExpired(ProjectId projectId, List<DeleteRequest> expired, ActionListener<Long> listener) {
        final BulkRequest bulkRequest = new BulkRequest();
        expired.forEach(bulkRequest::add);
        projectResolver.executeOnProject(
            projectId,
            () -> clientWithOrigin.bulk(bulkRequest, listener.delegateFailureAndWrap((l, response) -> {
                long deleted = 0;
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed() == false) {
                        deleted++;
                    } else if (item.getFailure().getStatus() != RestStatus.CONFLICT) {
                        // A conflict means the result was rewritten after it was matched, so it is no longer expired. Anything else
                        // is worth reporting, but the remaining documents were still processed.
                        logger.warn(() -> "failed to delete task result [" + item.getId() + "]", item.getFailure().getCause());
                    }
                }
                l.onResponse(deleted);
            }))
        );
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
