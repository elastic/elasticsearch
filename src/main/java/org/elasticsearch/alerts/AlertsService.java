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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class AlertsService extends AbstractComponent {

    private final Scheduler scheduler;
    private final AlertsStore alertsStore;
    private final AlertLockService alertLockService;
    private final HistoryService historyService;
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    @Inject
    public AlertsService(Settings settings, Scheduler scheduler, AlertsStore alertsStore, HistoryService historyService,
                         AlertLockService alertLockService) {
        super(settings);
        this.scheduler = scheduler;
        this.alertsStore = alertsStore;
        this.alertLockService = alertLockService;
        this.historyService = historyService;
    }

    public void start(ClusterState clusterState) {
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

    public void stop() {
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
                scheduler.add(result.current());
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
