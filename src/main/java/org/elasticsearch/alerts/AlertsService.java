/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.history.HistoryService;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.support.Callback;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class AlertsService extends AbstractComponent {

    private final Scheduler scheduler;
    private final AlertsStore alertsStore;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AlertLockService alertLockService;
    private final HistoryService historyService;
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    private volatile boolean manuallyStopped;

    @Inject
    public AlertsService(Settings settings, ClusterService clusterService, Scheduler scheduler, AlertsStore alertsStore,
                         IndicesService indicesService, HistoryService historyService, ThreadPool threadPool,
                         AlertLockService alertLockService) {
        super(settings);
        this.scheduler = scheduler;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
        this.clusterService = clusterService;
        this.alertLockService = alertLockService;
        this.historyService = historyService;

        clusterService.add(new AlertsClusterStateListener());
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an alert is scheduled.
        indicesService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });
        manuallyStopped = !settings.getAsBoolean("alerts.start_immediately",  true);
    }

    /**
     * Manually starts alerting if not already started
     */
    public void start() {
        manuallyStopped = false;
        ClusterState state = clusterService.state();
        internalStart(state);
    }

    /**
     * Manually stops alerting if not already stopped.
     */
    public void stop() {
        manuallyStopped = true;
        internalStop();
    }

    private void internalStop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            logger.info("stopping alert service...");
            alertLockService.stop();
            historyService.stop();
            scheduler.stop();
            alertsStore.stop();
            state.set(State.STOPPED);
            logger.info("alert service has stopped");
        }
    }

    private void internalStart(ClusterState clusterState) {
        if (state.compareAndSet(State.STOPPED, State.STARTING)) {
            logger.info("starting alert service...");
            alertLockService.start();

            // Try to load alert store before the action service, b/c action depends on alert store
            alertsStore.start(clusterState, new Callback<ClusterState>(){

                @Override
                public void onSuccess(ClusterState clusterState) {
                    historyService.start(clusterState, new Callback<ClusterState>() {

                        @Override
                        public void onSuccess(ClusterState clusterState) {
                            scheduler.start(alertsStore.getAlerts().values());
                            state.set(State.STARTED);
                            logger.info("alert service has started");
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("failed to start alert service", e);
                        }
                    });
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("failed to start alert service", e);
                }
            });
        }
    }

    public AlertsStore.AlertDelete deleteAlert(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        AlertLockService.Lock lock = alertLockService.acquire(name);
        try {
            AlertsStore.AlertDelete delete = alertsStore.deleteAlert(name);
            if (delete.deleteResponse().isFound()) {
                scheduler.remove(name);
            }
            return delete;
        } finally {
            lock.release();
        }
    }

    public IndexResponse putAlert(String name, BytesReference alertSource) {
        ensureStarted();
        AlertLockService.Lock lock = alertLockService.acquire(name);
        try {
            AlertsStore.AlertPut result = alertsStore.putAlert(name, alertSource);
            if (result.previous() == null || !result.previous().schedule().equals(result.current().schedule())) {
                scheduler.schedule(result.current());
            }
            return result.indexResponse();
        } finally {
            lock.release();
        }
    }

    /**
     * TODO: add version, fields, etc support that the core get api has as well.
     */
    public Alert getAlert(String name) {
        return alertsStore.getAlert(name);
    }

    public State state() {
        return state.get();
    }

    /**
     * Acks the alert if needed
     */
    public Alert.Status ackAlert(String name) {
        ensureStarted();
        AlertLockService.Lock lock = alertLockService.acquire(name);
        try {
            Alert alert = alertsStore.getAlert(name);
            if (alert == null) {
                throw new AlertsException("alert [" + name + "] does not exist");
            }
            if (alert.ack()) {
                try {
                    alertsStore.updateAlertStatus(alert);
                } catch (IOException ioe) {
                    throw new AlertsException("failed to update the alert on ack", ioe);
                }
            }
            // we need to create a safe copy of the status
            return new Alert.Status(alert.status());
        } finally {
            lock.release();
        }
    }

    public long getNumberOfAlerts() {
        return alertsStore.getAlerts().size();
    }

    private void ensureStarted() {
        if (state.get() != State.STARTED) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    /**
     * Return once a cluster state version appears that is never than the version
     */
    private ClusterState newClusterState(ClusterState previous) {
        ClusterState current;
        while (true) {
            current = clusterService.state();
            if (current.getVersion() > previous.getVersion()) {
                return current;
            }
        }
    }

    private final class AlertsClusterStateListener implements ClusterStateListener {

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
                        internalStop();
                    }
                });
            } else {
                if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                    // wait until the gateway has recovered from disk, otherwise we think may not have .alerts and
                    // a .alerts_history index, but they may not have been restored from the cluster state on disk
                    return;
                }
                if (state.get() == State.STOPPED && !manuallyStopped) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                        @Override
                        public void run() {
                            internalStart(event.state());
                        }
                    });
                }
            }
        }

    }

    /**
     * Encapsulates the state of the alerts plugin.
     */
    public static enum State {

        /**
         * The alerts plugin is not running and not functional.
         */
        STOPPED(0),

        /**
         * The alerts plugin is performing the necessary operations to get into a started state.
         */
        STARTING(1),

        /**
         * The alerts plugin is running and completely functional.
         */
        STARTED(2),

        /**
         * The alerts plugin is shutting down and not functional.
         */
        STOPPING(3);

        private final byte id;

        State(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public static State fromId(byte id) {
            switch (id) {
                case 0:
                    return STOPPED;
                case 1:
                    return STARTING;
                case 2:
                    return STARTED;
                case 3:
                    return STOPPING;
                default:
                    throw new AlertsException("unknown id alerts service state id [" + id + "]");
            }
        }
    }
}
