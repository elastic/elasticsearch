/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.cluster.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: Add lock synchronization via KeyedLock so that we can lock concurrent operations for the same alert.
// For example 2 concurrent deletes for the same alert, so that at least one fails, but not that 2 deletes are half done.
// The KeyedLock make sure that we only lock on the same alert, but not on different alerts.
public class AlertManager extends AbstractComponent {

    private AlertScheduler scheduler;
    private final AlertsStore alertsStore;
    private final TriggerManager triggerManager;
    private final ClusterService clusterService;
    private final AlertActionManager actionManager;
    private final AtomicBoolean started = new AtomicBoolean(false);


    @Inject
    public AlertManager(Settings settings, ClusterService clusterService, AlertsStore alertsStore,
                        IndicesService indicesService, TriggerManager triggerManager, AlertActionManager actionManager) {
        super(settings);
        this.alertsStore = alertsStore;
        this.clusterService = clusterService;
        this.triggerManager = triggerManager;
        this.actionManager = actionManager;
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

    public void setAlertScheduler(AlertScheduler scheduler){
        this.scheduler = scheduler;
    }

    public void clearAndReload() {
        ensureStarted();
        try {
            scheduler.clearAlerts();
            alertsStore.reload();
            sendAlertsToScheduler();
        } catch (Exception e){
            throw new ElasticsearchException("Failed to refresh alerts",e);
        }
    }


    public DeleteResponse deleteAlert(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        if (alertsStore.hasAlert(name)) {
            assert scheduler.remove(name);
            return alertsStore.deleteAlert(name);
        } else {
            return null;
        }
    }

    public Alert addAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        Alert alert = alertsStore.createAlert(alertName, alertSource);
        scheduler.add(alertName, alert);
        return alert;
    }

    public Alert addAlert(Alert alert) {
        ensureStarted();
        try {
            alertsStore.createAlert(alert);
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to create alert [" + alert +  "]", ioe);
        }
        scheduler.add(alert.alertName(), alert);
        return alert;
    }


    public List<Alert> getAllAlerts() {
        ensureStarted();
        return ImmutableList.copyOf(alertsStore.getAlerts().values());
    }

    public boolean isStarted() {
        return started.get();
    }

    public void executeAlert(String alertName, DateTime scheduledFireTime, DateTime fireTime){
        ensureStarted();
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
            TriggerResult result = triggerManager.isTriggered(alert, scheduledFireTime, fireTime);
            actionManager.addAlertAction(alert, result, scheduledFireTime, fireTime);
        } catch (Exception e) {
            logger.error("Failed execute alert [{}]", e, alertName);
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            scheduler.stop();
            alertsStore.stop();
            actionManager.stop();
        }
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    private void sendAlertsToScheduler() {
        for (Map.Entry<String, Alert> entry : alertsStore.getAlerts().entrySet()) {
            scheduler.add(entry.getKey(), entry.getValue());
        }
    }

    public Alert getAlert(String alertName) {
        return alertsStore.getAlert(alertName);
    }

    public boolean updateAlert(Alert alert) {
        if (!alertsStore.hasAlert(alert.alertName())) {
            return false;
        }
        return alertsStore.updateAlert(alert);

    }

    private final class AlertsClusterStateListener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.localNodeMaster()) {
                // We're not the master
                stop();
            } else {
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
                        retry();
                    }
                });
                actionManager.start(event.state(), new LoadingListener() {
                    @Override
                    public void onSuccess() {
                        startIfReady();
                    }

                    @Override
                    public void onFailure() {
                        retry();
                    }
                });
            }
        }

        private void startIfReady() {
            if (alertsStore.started() && actionManager.started()) {
                if (started.compareAndSet(false, true)) {
                    scheduler.start();
                    sendAlertsToScheduler();
                }
            }
        }

        private void retry() {
            clusterService.submitStateUpdateTask("alerts-retry", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    // Force a new cluster state to trigger that alerts cluster state listener gets invoked again.
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(String source, @Nullable Throwable t) {
                    logger.error("Error during {} ", t, source);
                }
            });
        }

    }

}
