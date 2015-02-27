/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

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
public class AlertsLifeCycleService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final AlertsService alertsService;
    private final ClusterService clusterService;

    // Maybe this should be a setting in the cluster settings?
    private volatile boolean manuallyStopped;

    @Inject
    public AlertsLifeCycleService(Settings settings, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, AlertsService alertsService) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.alertsService = alertsService;
        clusterService.add(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an alert is scheduled.
        indicesService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop(false);
            }
        });
        manuallyStopped = !settings.getAsBoolean("alerts.start_immediately",  true);
    }

    public void start() {
        start(clusterService.state());
    }

    public void stop() {
        stop(true);
    }

    private synchronized void start(ClusterState state) {
        alertsService.start(state);
    }

    private synchronized void stop(boolean manual) {
        manuallyStopped = manual;
        alertsService.stop();
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            // We're no longer the master so we need to stop alerting.
            // Stopping alerting may take a while since it will wait on the scheduler to complete shutdown,
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
                // wait until the gateway has recovered from disk, otherwise we think may not have .alerts and
                // a .alerts_history index, but they may not have been restored from the cluster state on disk
                return;
            }
            if (alertsService.state() == AlertsService.State.STOPPED && !manuallyStopped) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        start(event.state());
                    }
                });
            }
        }
    }
}
