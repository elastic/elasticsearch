/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.search.AsyncSearchIndexService.EXPIRATION_TIME_FIELD;

/**
 * A service that runs a periodic cleanup over the async-search index.
 */
class AsyncSearchMaintenanceService implements Releasable, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(AsyncSearchMaintenanceService.class);

    private final String localNodeId;
    private final ThreadPool threadPool;
    private final AsyncSearchIndexService indexService;
    private final TimeValue delay;

    private final AtomicBoolean isCleanupRunning = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private volatile Scheduler.Cancellable cancellable;

    AsyncSearchMaintenanceService(String localNodeId,
                                  ThreadPool threadPool,
                                  AsyncSearchIndexService indexService,
                                  TimeValue delay) {
        this.localNodeId = localNodeId;
        this.threadPool = threadPool;
        this.indexService = indexService;
        this.delay = delay;
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

    void tryStartCleanup(ClusterState state) {
        if (isClosed.get()) {
            return;
        }
        IndexRoutingTable indexRouting = state.routingTable().index(AsyncSearchIndexService.INDEX);
        if (indexRouting == null) {
            if (isCleanupRunning.compareAndSet(true, false)) {
                close();
            }
            return;
        }
        String primaryNodeId = indexRouting.shard(0).primaryShard().currentNodeId();
        if (localNodeId.equals(primaryNodeId)) {
            if (isCleanupRunning.compareAndSet(false, true)) {
                executeNextCleanup();
            }
        } else if (isCleanupRunning.compareAndSet(true, false)) {
            close();
        }
    }

    synchronized void executeNextCleanup() {
        if (isClosed.get() == false && isCleanupRunning.get()) {
            long nowInMillis = System.currentTimeMillis();
            DeleteByQueryRequest toDelete = new DeleteByQueryRequest()
                .setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME_FIELD).lte(nowInMillis));
            indexService.getClient()
                .execute(DeleteByQueryAction.INSTANCE, toDelete, ActionListener.wrap(() -> scheduleNextCleanup()));
        }
    }

    synchronized void scheduleNextCleanup() {
        if (isClosed.get() == false && isCleanupRunning.get()) {
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

    @Override
    public void close() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            cancellable.cancel();
        }
        isClosed.compareAndSet(false, true);
    }
}
