/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final WatcherService watcherService;
    private final ClusterService clusterService;

    // TODO: If Watcher was stopped via api and the master is changed then Watcher will start regardless of the previous
    // stop command, so at some point this needs to be a cluster setting
    private volatile boolean manuallyStopped;

    @Inject
    public WatcherLifeCycleService(Settings settings, ClusterService clusterService, ThreadPool threadPool, WatcherService watcherService) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.watcherService = watcherService;
        clusterService.add(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an watch is scheduled.
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop(false);
            }
        });
        manuallyStopped = !settings.getAsBoolean("watcher.start_immediately",  true);
    }

    public void start() {
        start(clusterService.state(), true);
    }

    public void stop() {
        stop(true);
    }

    private synchronized void stop(boolean manual) {
        manuallyStopped = manual;
        watcherService.stop();
    }

    private synchronized void start(ClusterState state, boolean manual) {
        WatcherState watcherState = watcherService.state();
        if (watcherState != WatcherState.STOPPED) {
            logger.debug("not starting watcher. watcher can only start if its current state is [{}]", WatcherState.STOPPED);
            return;
        }

        // If we start from a cluster state update we need to check if previously we stopped manually
        // otherwise Watcher would start upon the next cluster state update while the user instructed Watcher to not run
        if (!manual && manuallyStopped) {
            logger.debug("not starting watcher. watcher was stopped manually and therefore cannot be auto-start");
            return;
        }

        if (!watcherService.validate(state)) {
            logger.debug("not starting watcher. because the cluster isn't ready yet to run watcher");
            return;
        }

        int attempts = 0;
        for (; attempts < 3; attempts++) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("start attempt [{}]...", attempts);
                } else if (logger.isTraceEnabled()) {
                    logger.trace("starting... (attempt [{}] - based on cluster state version [{}])", attempts, state.getVersion());
                }
                watcherService.start(state);
                return;
            } catch (Exception e) {
                logger.warn("error occurred while starting, retrying...", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (!clusterService.localNode().masterNode()) {
                    logger.error("abort retry, we are no longer master");
                    return;
                }
            }
        }
        logger.error("failed to start watcher. attempted to start [{}] times. please try to start Watcher manually", attempts);
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            // We're no longer the master so we need to stop the watcher.
            // Stopping the watcher may take a while since it will wait on the scheduler to complete shutdown,
            // so we fork here so that we don't wait too long. Other events may need to be processed and
            // other cluster state listeners may need to be executed as well for this event.
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    stop(false);
                }
            });
        } else {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                // wait until the gateway has recovered from disk, otherwise we think may not have .watches and
                // a .triggered_watches index, but they may not have been restored from the cluster state on disk
                return;
            }

            final ClusterState state = event.state();
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    start(state, false);
                }
            });
        }
    }
}
