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
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final WatcherService watcherService;
    private final ClusterService clusterService;

    // Maybe this should be a setting in the cluster settings?
    private volatile boolean manuallyStopped;

    @Inject
    public WatcherLifeCycleService(Settings settings, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, WatcherService watcherService) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.watcherService = watcherService;
        clusterService.add(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an watch is scheduled.
        indicesService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop(false);
            }
        });
        manuallyStopped = !settings.getAsBoolean("watcher.start_immediately",  true);
    }

    public void start() {
        start(clusterService.state());
    }

    public void stop() {
        stop(true);
    }

    private synchronized void start(ClusterState state) {
        watcherService.start(state);
    }

    private synchronized void stop(boolean manual) {
        manuallyStopped = manual;
        watcherService.stop();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
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
                // wait until the gateway has recovered from disk, otherwise we think may not have .watchs and
                // a .watch_history index, but they may not have been restored from the cluster state on disk
                return;
            }

            final ClusterState state = event.state();
            if (!watcherService.validate(state)) {
                return;
            }

            if (watcherService.state() == WatcherService.State.STOPPED && !manuallyStopped) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        int attempts = 0;
                        while(true) {
                            try {
                                start(state);
                                return;
                            } catch (Exception e) {
                                if (++attempts < 3) {
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
                                } else {
                                    logger.error("attempted to start Watcher [{}] times, aborting now, please try to start Watcher manually", attempts);
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}
