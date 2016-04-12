/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

/**
 */
public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final WatcherService watcherService;
    private final ClusterService clusterService;

    private volatile WatcherMetaData watcherMetaData;

    @Inject
    public WatcherLifeCycleService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   WatcherService watcherService) {
        super(settings);
        this.threadPool = threadPool;
        this.watcherService = watcherService;
        this.clusterService = clusterService;
        clusterService.add(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an watch is scheduled.
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop(false);
            }
        });
        watcherMetaData = new WatcherMetaData(!settings.getAsBoolean("xpack.watcher.start_immediately", true));
    }

    public void start() {
        start(clusterService.state(), true);
    }

    public void stop() {
        stop(true);
    }

    private synchronized void stop(boolean manual) {
        WatcherState watcherState = watcherService.state();
        if (watcherState != WatcherState.STARTED) {
            logger.debug("not stopping watcher. watcher can only stop if its current state is [{}], but its current state now is [{}]",
                    WatcherState.STARTED, watcherState);
        } else {
            watcherService.stop();
        }
        if (manual) {
            updateManualStopped(true);
        }
    }

    private synchronized void start(ClusterState state, boolean manual) {
        WatcherState watcherState = watcherService.state();
        if (watcherState != WatcherState.STOPPED) {
            logger.debug("not starting watcher. watcher can only start if its current state is [{}], but its current state now is [{}]",
                    WatcherState.STOPPED, watcherState);
            return;
        }

        // If we start from a cluster state update we need to check if previously we stopped manually
        // otherwise Watcher would start upon the next cluster state update while the user instructed Watcher to not run
        if (!manual && watcherMetaData.manuallyStopped()) {
            logger.debug("not starting watcher. watcher was stopped manually and therefore cannot be auto-started");
            return;
        }

        if (watcherService.validate(state)) {
            logger.trace("starting... (based on cluster state version [{}]) (manual [{}])", state.getVersion(), manual);
            try {
                watcherService.start(state);
            } catch (Exception e) {
                logger.warn("failed to start watcher. please wait for the cluster to become ready or try to start Watcher manually", e);
            }
        } else {
            logger.debug("not starting watcher. because the cluster isn't ready yet to run watcher");
        }
        if (manual) {
            updateManualStopped(false);
        }
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .watches and
            // a .triggered_watches index, but they may not have been restored from the cluster state on disk
            return;
        }

        WatcherMetaData watcherMetaData = event.state().getMetaData().custom(WatcherMetaData.TYPE);
        if (watcherMetaData != null) {
            this.watcherMetaData = watcherMetaData;
        }

        if (!event.localNodeMaster()) {
            if (watcherService.state() != WatcherState.STARTED) {
                // to avoid unnecessary forking of threads...
                return;
            }

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
            if (watcherService.state() != WatcherState.STOPPED) {
                // to avoid unnecessary forking of threads...
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

    public WatcherMetaData watcherMetaData() {
        return watcherMetaData;
    }

    private void updateManualStopped(final boolean stopped) {
        watcherMetaData = new WatcherMetaData(stopped);

        // We need to make sure that the new WatcherMetaData has arrived on all nodes,
        // so in order to do this we need to use AckedClusterStateUpdateTask which
        // requires a AckedRequest and ActionListener...
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.warn("updating manually stopped isn't acked", throwable);
                latch.countDown();
            }
        };
        AckedRequest request = new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(30);
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return TimeValue.timeValueSeconds(30);
            }
        };
        clusterService.submitStateUpdateTask("update_watcher_manually_stopped",
                new AckedClusterStateUpdateTask<Boolean>(request, listener) {

            @Override
            protected Boolean newResponse(boolean result) {
                return result;
            }

            @Override
            public ClusterState execute(ClusterState clusterState) throws Exception {
                ClusterState.Builder builder = new ClusterState.Builder(clusterState);
                builder.metaData(MetaData.builder(clusterState.getMetaData())
                        .putCustom(WatcherMetaData.TYPE, new WatcherMetaData(stopped)));
                return builder.build();
            }

            @Override
            public void onFailure(String source, Throwable throwable) {
                latch.countDown();
                logger.warn("couldn't update watcher metadata [{}]", throwable, source);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

}
