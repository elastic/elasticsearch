/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.EsqlCursorIndexService.CURSOR_ID_FIELD;
import static org.elasticsearch.xpack.esql.plugin.EsqlCursorIndexService.CURSOR_INDEX;
import static org.elasticsearch.xpack.esql.plugin.EsqlCursorIndexService.DOC_TYPE_FIELD;
import static org.elasticsearch.xpack.esql.plugin.EsqlCursorIndexService.DOC_TYPE_METADATA;
import static org.elasticsearch.xpack.esql.plugin.EsqlCursorIndexService.EXPIRATION_TIME_FIELD;

/**
 * Periodically deletes expired cursor documents from the {@code .esql-cursors} system index.
 */
public class EsqlCursorMaintenanceService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(EsqlCursorMaintenanceService.class);
    private static final TimeValue CLEANUP_INTERVAL = TimeValue.timeValueHours(1);
    static final int CLEANUP_BATCH_SIZE = 100;

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final String localNodeId;
    private final ThreadPool threadPool;
    private final Client clientWithOrigin;

    private boolean isCleanupRunning;
    private Collection<ProjectId> projectsToCleanup;
    private volatile Scheduler.Cancellable cancellable;

    public EsqlCursorMaintenanceService(
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
            return;
        }
        tryStartCleanup(state);
    }

    synchronized void tryStartCleanup(ClusterState state) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final List<ProjectId> projectsOnLocalNode = state.metadata().projects().keySet().stream().filter(project -> {
            final IndexRoutingTable indexRouting = state.routingTable(project).index(CURSOR_INDEX);
            if (indexRouting == null) {
                return false;
            }
            final String primaryNodeId = indexRouting.shard(0).primaryShard().currentNodeId();
            return localNodeId.equals(primaryNodeId);
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
            ActionListener<Void> listener = new CountDownActionListener(
                this.projectsToCleanup.size(),
                ActionListener.running(this::scheduleNextCleanup)
            );
            for (ProjectId project : this.projectsToCleanup) {
                cleanupIndex(project, listener);
            }
        }
    }

    private void cleanupIndex(ProjectId projectId, ActionListener<Void> listener) {
        final long nowInMillis = System.currentTimeMillis();
        SearchRequest searchRequest = new SearchRequest(CURSOR_INDEX).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, DOC_TYPE_METADATA))
                    .filter(QueryBuilders.rangeQuery(EXPIRATION_TIME_FIELD).lte(nowInMillis))
            ).fetchSource(false).size(CLEANUP_BATCH_SIZE)
        );
        projectResolver.executeOnProject(projectId, () -> clientWithOrigin.search(searchRequest, ActionListener.wrap(searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                listener.onResponse(null);
                return;
            }
            boolean batchFull = hits.length >= CLEANUP_BATCH_SIZE;
            ActionListener<Void> batchListener = batchFull
                ? ActionListener.wrap(v -> cleanupIndex(projectId, listener), listener::onFailure)
                : listener;
            CountDownActionListener countDown = new CountDownActionListener(hits.length, batchListener);
            for (SearchHit hit : hits) {
                String cursorId = hit.getId();
                DeleteByQueryRequest dbq = new DeleteByQueryRequest(CURSOR_INDEX).setQuery(
                    QueryBuilders.termQuery(CURSOR_ID_FIELD, cursorId)
                );
                clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, dbq, countDown.map(r -> null));
            }
        }, listener::onFailure)));
    }

    synchronized void scheduleNextCleanup() {
        if (isCleanupRunning) {
            try {
                cancellable = threadPool.schedule(this::executeNextCleanup, CLEANUP_INTERVAL, threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("failed to schedule next cursor cleanup; shutting down", e);
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
