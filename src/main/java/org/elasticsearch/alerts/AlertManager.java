/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.actions.AlertActionEntry;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlertManager extends AbstractComponent {

    private final AlertScheduler scheduler;
    private final AlertsStore alertsStore;
    private final TriggerManager triggerManager;
    private final AlertActionManager actionManager;
    private final AlertActionRegistry actionRegistry;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private final KeyedLock<String> alertLock = new KeyedLock<>();

    @Inject
    public AlertManager(Settings settings, ClusterService clusterService, AlertScheduler scheduler, AlertsStore alertsStore,
                        IndicesService indicesService, TriggerManager triggerManager, AlertActionManager actionManager,
                        AlertActionRegistry actionRegistry, ThreadPool threadPool) {
        super(settings);
        this.scheduler = scheduler;
        this.threadPool = threadPool;
        this.scheduler.setAlertManager(this);
        this.alertsStore = alertsStore;
        this.triggerManager = triggerManager;
        this.actionManager = actionManager;
        this.actionManager.setAlertManager(this);
        this.actionRegistry = actionRegistry;
        clusterService.add(new AlertsClusterStateListener());
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an alert is scheduled.
        indicesService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });

    }

    public DeleteResponse deleteAlert(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        alertLock.acquire(name);
        try {
            DeleteResponse deleteResponse = alertsStore.deleteAlert(name);
            if (deleteResponse.isFound()) {
                scheduler.remove(name);
            }
            return deleteResponse;
        } finally {
            alertLock.release(name);
        }
    }

    public IndexResponse addAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        alertLock.acquire(alertName);
        try {
            Tuple<Alert, IndexResponse> result = alertsStore.addAlert(alertName, alertSource);
            scheduler.add(alertName, result.v1());
            return result.v2();
        } finally {
            alertLock.release(alertName);
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public void scheduleAlert(String alertName, DateTime scheduledFireTime, DateTime fireTime){
        ensureStarted();
        alertLock.acquire(alertName);
        try {
            Alert alert = alertsStore.getAlert(alertName);
            if (alert == null) {
                logger.warn("Unable to find [{}] in the alert store, perhaps it has been deleted", alertName);
                return;
            }
            if (!alert.enabled()) {
                logger.debug("Alert [{}] is not enabled", alert.alertName());
                return;
            }

            try {
                actionManager.addAlertAction(alert, scheduledFireTime, fireTime);
            } catch (IOException ioe) {
                logger.error("Failed to add alert action for [{}]", ioe, alert);
            }
        } finally {
            alertLock.release(alertName);
        }
    }

    public TriggerResult executeAlert(AlertActionEntry entry) throws IOException {
        ensureStarted();
        alertLock.acquire(entry.getAlertName());
        try {
            Alert alert = alertsStore.getAlert(entry.getAlertName());
            if (alert == null) {
                throw new ElasticsearchException("Alert is not available");
            }
            TriggerResult triggerResult = triggerManager.isTriggered(alert, entry.getScheduledTime(), entry.getFireTime());
            entry.setSearchRequest(triggerResult.getRequest());
            entry.setSearchResponse(triggerResult.getResponse());
            if (triggerResult.isTriggered()) {
                entry.setTriggered(true);
                actionRegistry.doAction(alert, triggerResult);
                alert.lastActionFire(entry.getFireTime());
                alertsStore.updateAlert(alert);
            }
            return triggerResult;
        } finally {
            alertLock.release(entry.getAlertName());
        }
    }


    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.info("Stopping alert manager...");
            scheduler.stop();
            actionManager.stop();
            alertsStore.stop();
            logger.info("Alert manager has stopped");
        }
    }

    /**
     * For testing only to clear the alerts between tests.
     */
    public void clear() {
        scheduler.clearAlerts();
        alertsStore.clear();
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    public long getNumberOfAlerts() {
        return alertsStore.getAlerts().size();
    }

    private final class AlertsClusterStateListener implements ClusterStateListener {

        @Override
        public void clusterChanged(final ClusterChangedEvent event) {
            if (!event.localNodeMaster()) {
                // We're not the master
                stop();
            } else {
                if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                    return; // wait until the gateway has recovered from disk
                }

                if (started.get()) {
                    return; // We're already started
                }

                alertsStore.start(event.state(), new LoadingListener() {
                    @Override
                    public void onSuccess() {
                        startIfReady();
                    }

                    @Override
                    public void onFailure() {
                        retry(event);
                    }
                });
                actionManager.start(event.state(), new LoadingListener() {
                    @Override
                    public void onSuccess() {
                        startIfReady();
                    }

                    @Override
                    public void onFailure() {
                        retry(event);
                    }
                });
            }
        }

        private void startIfReady() {
            if (alertsStore.started() && actionManager.started()) {
                if (started.compareAndSet(false, true)) {
                    scheduler.start(alertsStore.getAlerts());
                }
            }
        }

        private void retry(final ClusterChangedEvent event) {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    // Retry with the same event:
                    clusterChanged(event);
                }
            });
        }

    }

}
