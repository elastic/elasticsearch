/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionEntry;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: Add lock synchronization via KeyedLock so that we can lock concurrent operations for the same alert.
// For example 2 concurrent deletes for the same alert, so that at least one fails, but not that 2 deletes are half done.
// The KeyedLock make sure that we only lock on the same alert, but not on different alerts.
public class AlertManager extends AbstractComponent {

    public static final String ALERT_INDEX = ".alerts";
    public static final String ALERT_TYPE = "alert";

    private final ThreadPool threadPool;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean startActions = new AtomicBoolean(false);

    private AlertScheduler scheduler;
    private final AlertsStore alertsStore;
    private final AlertActionRegistry actionRegistry;
    private final AlertActionManager actionManager;

    @Inject
    public AlertManager(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                        AlertActionRegistry actionRegistry, AlertsStore alertsStore) {
        super(settings);
        this.threadPool = threadPool;
        this.actionRegistry = actionRegistry;
        this.actionManager = new AlertActionManager(client, this, actionRegistry, threadPool);
        this.alertsStore = alertsStore;
        clusterService.add(new AlertsClusterStateListener());
    }

    public void setAlertScheduler(AlertScheduler scheduler){
        this.scheduler = scheduler;
    }

    public void doAction(Alert alert, AlertActionEntry result, DateTime scheduledTime) {
        ensureStarted();
        logger.warn("We have triggered");
        DateTime lastActionFire = timeActionLastTriggered(alert.alertName());
        long msSinceLastAction = scheduledTime.getMillis() - lastActionFire.getMillis();
        logger.error("last action fire [{}]", lastActionFire);
        logger.error("msSinceLastAction [{}]", msSinceLastAction);

        if (alert.timePeriod().getMillis() > msSinceLastAction) {
            logger.warn("Not firing action because it was fired in the timePeriod");
        } else {
            actionRegistry.doAction(alert, result);
            logger.warn("Did action !");

            alert.lastActionFire(scheduledTime);
            alertsStore.updateAlert(alert);
        }
    }

    public boolean claimAlertRun(String alertName, DateTime scheduleRunTime) {
        ensureStarted();
        Alert alert;
        try {
            alert = alertsStore.getAlert(alertName);
            if (!alert.enabled()) {
                return false;
            }
        } catch (Throwable t) {
            throw new ElasticsearchException("Unable to load alert from index",t);
        }

        alert.running(scheduleRunTime);
        alertsStore.updateAlert(alert);
        return true;
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

    public boolean updateLastRan(String alertName, DateTime fireTime, DateTime scheduledTime) throws Exception {
        ensureStarted();
        Alert alert = alertsStore.getAlert(alertName);
        alert.lastRan(fireTime);
        alertsStore.updateAlert(alert);
        return true;
    }

    public boolean deleteAlert(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        if (alertsStore.hasAlert(name)) {
            assert scheduler.deleteAlertFromSchedule(name);
            alertsStore.deleteAlert(name);
            return true;
        } else {
            return false;
        }
    }

    public Alert addAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        Alert alert = alertsStore.createAlert(alertName, alertSource);
        scheduler.addAlert(alertName, alert);
        return alert;
    }

    public Alert getAlertForName(String alertName) {
        ensureStarted();
        return alertsStore.getAlert(alertName);
    }

    public List<Alert> getAllAlerts() {
        ensureStarted();
        return ImmutableList.copyOf(alertsStore.getAlerts().values());
    }

    public boolean isStarted() {
        return started.get();
    }

    public boolean addHistory(String alertName, boolean isTriggered, DateTime dateTime, DateTime scheduledTime,
                              SearchRequestBuilder srb, AlertTrigger trigger, long totalHits, List<AlertAction> actions,
                              List<String> indices) throws IOException{
        return actionManager.addHistory(alertName, isTriggered, dateTime, scheduledTime, srb, trigger, totalHits, actions, indices);
    }

    private void ensureStarted() {
        if (!started.get() || !startActions.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    private void sendAlertsToScheduler() {
        for (Map.Entry<String, Alert> entry : alertsStore.getAlerts().entrySet()) {
            scheduler.addAlert(entry.getKey(), entry.getValue());
        }
    }

    private DateTime timeActionLastTriggered(String alertName) {
        Alert alert = alertsStore.getAlert(alertName);
        if (alert != null) {
            return alert.lastActionFire();
        } else {
            return null;
        }
    }

    private final class AlertsClusterStateListener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.localNodeMaster()) { //We are not the master
                if (started.compareAndSet(false, true)) {
                    scheduler.clearAlerts();
                    alertsStore.clear();
                }

                if (startActions.compareAndSet(false, true)) {
                    //If actionManager was running and we aren't the master stop
                    actionManager.doStop(); //Safe to call this multiple times, it's a noop if we are already stopped
                }
                return;
            }

            if (!started.get()) {
                IndexMetaData alertIndexMetaData = event.state().getMetaData().index(ALERT_INDEX);
                if (alertIndexMetaData != null) {
                    if (event.state().routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
                        started.set(true);
                        // TODO: the starter flag should only be set to true once the alert loader has completed.
                        // Right now there is a window of time between when started=true and loading has completed where
                        // alerts can get lost
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AlertLoader());
                    }
                }
            }

            if (!startActions.get()) {
                IndexMetaData indexMetaData = event.state().getMetaData().index(AlertActionManager.ALERT_HISTORY_INDEX);
                if (indexMetaData != null) {
                    if (event.state().routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
                        startActions.set(true);
                        actionManager.doStart();
                    }
                }
            }
        }

    }

    private class AlertLoader implements Runnable {
        @Override
        public void run() {
            // TODO: have some kind of retry mechanism?
            try {
                alertsStore.reload();
                sendAlertsToScheduler();
            } catch (Exception e) {
                logger.warn("Error during loading of alerts from an existing .alerts index... refresh the alerts manually");
            }
            started.set(true);

        }
    }

}
