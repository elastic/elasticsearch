/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsPlugin;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class HistoryService extends AbstractComponent {

    private final HistoryStore historyStore;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private AlertsService alertsService;

    // Holds fired alerts that were fired before on a different elected master node, but never had the chance to run.
    private volatile ImmutableList<FiredAlert> previousFiredAlerts;

    @Inject
    public HistoryService(Settings settings, HistoryStore historyStore, ThreadPool threadPool, AlertsStore alertsStore) {
        super(settings);
        this.historyStore = historyStore;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
    }

    public void setAlertsService(AlertsService alertsService){
        this.alertsService = alertsService;
    }

    public boolean start(ClusterState state) {
        if (started.get()) {
            return true;
        }

        assert alertsThreadPool().getQueue().isEmpty() : "queue should be empty, but contains " + alertsThreadPool().getQueue().size() + " elements.";
        HistoryStore.LoadResult loadResult = historyStore.loadFiredAlerts(state);
        if (loadResult.succeeded()) {
            if (!loadResult.notRanFiredAlerts().isEmpty()) {
                this.previousFiredAlerts = ImmutableList.copyOf(loadResult.notRanFiredAlerts());
                logger.debug("loaded [{}] actions from the alert history index into actions queue", previousFiredAlerts.size());
            }
            logger.debug("starting history service");
            if (started.compareAndSet(false, true)) {
                if (alertsThreadPool().isShutdown()) {
                    // this update threadpool settings work around is for restarting the alerts thread pool,
                    // that creates a new alerts thread pool and cleans up the existing one that has previously been shutdown.
                    int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
                    threadPool.updateSettings(AlertsPlugin.alertThreadPoolSettings(availableProcessors));
                }
                logger.debug("started history service");
            }
            return true;
        } else {
            return false;
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.debug("stopping history service");
            // We could also rely on the shutdown in #updateSettings call, but
            // this is a forceful shutdown that also interrupts the worker threads in the threadpool
            List<Runnable> cancelledTasks = alertsThreadPool().shutdownNow();
            logger.debug("cancelled [{}] queued tasks", cancelledTasks.size());
            logger.debug("stopped history service");
        }
    }

    public boolean started() {
        return started.get();
    }

    // We can only process previosly fired alerts if the alert service has gone into a started state,
    // so we let the alert service execute this method when it gets into that state.

    // TODO: We maybe have a AlertServiceStateListener interface for component that are interrested in when the state
    // of alerts changes then these components can register themselves.
    public void executePreviouslyFiredAlerts() {
        ImmutableList<FiredAlert> firedAlerts = this.previousFiredAlerts;
        if (firedAlerts != null) {
            this.previousFiredAlerts = null;
            for (FiredAlert firedAlert : firedAlerts) {
                innerExecute(firedAlert);
            }
        }
    }

    public void alertFired(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws HistoryException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
        FiredAlert firedAlert = new FiredAlert(alert, scheduledFireTime, fireTime, FiredAlert.State.AWAITS_RUN);
        logger.debug("adding fired alert [{}]", alert.name());
        historyStore.put(firedAlert);
        innerExecute(firedAlert);
    }

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long getQueueSize() {
        return alertsThreadPool().getQueue().size();
    }

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long getLargestQueueSize() {
        return alertsThreadPool().getLargestPoolSize();
    }

    private void innerExecute(FiredAlert firedAlert) {
        try {
            alertsThreadPool().execute(new AlertHistoryRunnable(firedAlert));
        } catch (RejectedExecutionException e) {
            logger.debug("[{}] failed to execute fired alert", firedAlert.name());
            firedAlert.state(FiredAlert.State.FAILED);
            firedAlert.errorMessage("failed to run fired alert due to thread pool capacity");
            historyStore.update(firedAlert);
        }
    }

    private EsThreadPoolExecutor alertsThreadPool() {
        return (EsThreadPoolExecutor) threadPool.executor(AlertsPlugin.NAME);
    }

    private final class AlertHistoryRunnable implements Runnable {

        private final FiredAlert alert;

        private AlertHistoryRunnable(FiredAlert alert) {
            this.alert = alert;
        }

        @Override
        public void run() {
            try {
                Alert alert = alertsStore.getAlert(this.alert.name());
                if (alert == null) {
                    this.alert.errorMessage("alert was not found in the alerts store");
                    this.alert.state(FiredAlert.State.FAILED);
                    historyStore.update(this.alert);
                    return;
                }
                this.alert.state(FiredAlert.State.RUNNING);
                historyStore.update(this.alert);
                logger.debug("running an alert [{}]", this.alert.name());
                AlertsService.AlertRun alertRun = alertsService.runAlert(this.alert);
                this.alert.finalize(alert, alertRun);
                historyStore.update(this.alert);
            } catch (Exception e) {
                if (started()) {
                    logger.warn("failed to run alert [{}]", e, alert.name());
                    try {
                        alert.errorMessage(e.getMessage());
                        alert.state(FiredAlert.State.FAILED);
                        historyStore.update(alert);
                    } catch (Exception e2) {
                        logger.error("failed to update fired alert [{}] with the error message", e2, alert);
                    }
                } else {
                    logger.debug("failed to execute fired alert [{}] after shutdown", e, alert);
                }
            }
        }
    }

}
