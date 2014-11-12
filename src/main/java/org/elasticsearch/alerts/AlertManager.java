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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: Add lock synchronization via KeyedLock so that we can lock concurrent operations for the same alert.
// For example 2 concurrent deletes for the same alert, so that at least one fails, but not that 2 deletes are half done.
// The KeyedLock make sure that we only lock on the same alert, but not on different alerts.
public class AlertManager extends AbstractComponent {

    private final AlertScheduler scheduler;
    private final AlertsStore alertsStore;
    private final TriggerManager triggerManager;
    private final AlertActionManager actionManager;
    private final AlertActionRegistry actionRegistry;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ThreadPool threadPool;

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
        DeleteResponse deleteResponse = alertsStore.deleteAlert(name);
        if (deleteResponse.isFound()) {
            scheduler.remove(name);
        }
        return deleteResponse;
    }

    public IndexResponse addAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        Tuple<Alert, IndexResponse> result = alertsStore.addAlert(alertName, alertSource);
        scheduler.add(alertName, result.v1());
        return result.v2();
    }

    public boolean isStarted() {
        return started.get();
    }

    public void scheduleAlert(String alertName, DateTime scheduledFireTime, DateTime fireTime){
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
            actionManager.addAlertAction(alert, scheduledFireTime, fireTime);
        } catch (IOException ioe) {
            logger.error("Failed to add alert action for [{}]", ioe, alert);
        }
    }

    public TriggerResult executeAlert(String alertName, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        ensureStarted();
        Alert alert = alertsStore.getAlert(alertName);
        if (alert == null) {
            throw new ElasticsearchException("Alert is not available");
        }
        TriggerResult triggerResult = triggerManager.isTriggered(alert, scheduledFireTime, fireTime);
        if (triggerResult.isTriggered()) {
            actionRegistry.doAction(alert, triggerResult);
            alert.lastActionFire(fireTime);
            alertsStore.updateAlert(alert);
        }
        return triggerResult;
    }


    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.info("Stopping alert manager...");
            synchronized (scheduler) {
                scheduler.stop();
            }
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
