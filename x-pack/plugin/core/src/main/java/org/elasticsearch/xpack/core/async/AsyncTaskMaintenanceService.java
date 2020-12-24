/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;

import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.EXPIRATION_TIME_FIELD;

/**
 * A service that runs a periodic cleanup over the async execution index.
 * <p>
 * Since we will have several injected implementation of this class injected into different transports, and we bind components created
 * by {@linkplain org.elasticsearch.plugins.Plugin#createComponents} to their classes, we need to implement one class per binding.
 */
public class AsyncTaskMaintenanceService extends AbstractLifecycleComponent implements ClusterStateListener {

    /**
     * Controls the interval at which the cleanup is scheduled.
     * Defaults to 1h. It is an undocumented/expert setting that
     * is mainly used by integration tests to make the garbage
     * collection of search responses more reactive.
     */
    public static final Setting<TimeValue> ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING =
        Setting.timeSetting("async_search.index_cleanup_interval", TimeValue.timeValueHours(1), Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(AsyncTaskMaintenanceService.class);

    private final ClusterService clusterService;
    private final String index;
    private final String localNodeId;
    private final ThreadPool threadPool;
    private final AsyncTaskIndexService<?> indexService;
    private final TimeValue delay;

    private boolean isCleanupRunning;
    private volatile Scheduler.Cancellable cancellable;

    public AsyncTaskMaintenanceService(ClusterService clusterService,
                                       String localNodeId,
                                       Settings nodeSettings,
                                       ThreadPool threadPool,
                                       AsyncTaskIndexService<?> indexService) {
        this.clusterService = clusterService;
        this.index = XPackPlugin.ASYNC_RESULTS_INDEX;
        this.localNodeId = localNodeId;
        this.threadPool = threadPool;
        this.indexService = indexService;
        this.delay = ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING.get(nodeSettings);
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
    protected final void doClose() throws IOException {
    }

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
        IndexRoutingTable indexRouting = state.routingTable().index(index);
        if (indexRouting == null) {
            stopCleanup();
            return;
        }
        String primaryNodeId = indexRouting.shard(0).primaryShard().currentNodeId();
        if (localNodeId.equals(primaryNodeId)) {
            if (isCleanupRunning == false) {
                isCleanupRunning = true;
                executeNextCleanup();
            }
        } else {
            stopCleanup();
        }
    }

    synchronized void executeNextCleanup() {
        if (isCleanupRunning) {
            long nowInMillis = System.currentTimeMillis();
            DeleteByQueryRequest toDelete = new DeleteByQueryRequest(index)
                .setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME_FIELD).lte(nowInMillis));
            indexService.getClient()
                .execute(DeleteByQueryAction.INSTANCE, toDelete, ActionListener.wrap(this::scheduleNextCleanup));
        }
    }

    synchronized void scheduleNextCleanup() {
        if (isCleanupRunning) {
            try {
                cancellable = threadPool.schedule(this::executeNextCleanup, delay, ThreadPool.Names.GENERIC);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("failed to schedule next maintenance task; shutting down", e);
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
