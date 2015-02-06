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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class HistoryService extends AbstractComponent {

    private final HistoryStore historyStore;
    private AlertsService alertsService;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;

    private final AtomicLong largestQueueSize = new AtomicLong(0);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final BlockingQueue<FiredAlert> actionsToBeProcessed = new LinkedBlockingQueue<>();
    private volatile Thread queueReaderThread;

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

        assert actionsToBeProcessed.isEmpty() : "Queue should be empty, but contains " + actionsToBeProcessed.size() + " elements.";
        HistoryStore.LoadResult loadResult = historyStore.loadFiredAlerts(state);
        if (loadResult.succeeded()) {
            if (!loadResult.notRanFiredAlerts().isEmpty()) {
                actionsToBeProcessed.addAll(loadResult.notRanFiredAlerts());
                logger.debug("Loaded [{}] actions from the alert history index into actions queue", actionsToBeProcessed.size());
                largestQueueSize.set(actionsToBeProcessed.size());
            }
            doStart();
            return true;
        } else {
            return false;
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.info("Stopping job queue...");
            try {
                if (queueReaderThread.isAlive()) {
                    queueReaderThread.interrupt();
                    queueReaderThread.join();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            actionsToBeProcessed.clear();
            logger.info("Job queue has been stopped");
        }
    }

    public boolean started() {
        return started.get();
    }

    public void alertFired(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws HistoryException {
        ensureStarted();
        FiredAlert firedAlert = new FiredAlert(alert, scheduledFireTime, fireTime, FiredAlert.State.AWAITS_RUN);
        logger.debug("adding fired alert [{}]", alert.name());
        historyStore.put(firedAlert);

        long currentSize = actionsToBeProcessed.size() + 1;
        actionsToBeProcessed.add(firedAlert);
        long currentLargestQueueSize = largestQueueSize.get();
        boolean done = false;
        while (!done) {
            if (currentSize > currentLargestQueueSize) {
                done = largestQueueSize.compareAndSet(currentLargestQueueSize, currentSize);
            } else {
                break;
            }
            currentLargestQueueSize = largestQueueSize.get();
        }
    }

    public long getQueueSize() {
        return actionsToBeProcessed.size();
    }

    public long getLargestQueueSize() {
        return largestQueueSize.get();
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    private void doStart() {
        logger.info("Starting job queue");
        if (started.compareAndSet(false, true)) {
            startQueueReaderThread();
        }
    }

    private void startQueueReaderThread() {
        queueReaderThread = new Thread(new QueueReaderThread(), EsExecutors.threadName(settings, "queue_reader"));
        queueReaderThread.start();
    }

    private class AlertHistoryRunnable implements Runnable {

        private final FiredAlert alert;

        private AlertHistoryRunnable(FiredAlert alert) {
            this.alert = alert;
        }

        @Override
        public void run() {
            try {
                Alert alert = alertsStore.getAlert(this.alert.name());
                if (alert == null) {
                    this.alert.errorMsg("alert was not found in the alerts store");
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
                        alert.errorMsg(e.getMessage());
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

    private class QueueReaderThread implements Runnable {

        @Override
        public void run() {
            try {
                logger.debug("starting thread to read from the job queue");
                while (started()) {
                    FiredAlert alert = actionsToBeProcessed.take();
                    if (alert != null) {
                        threadPool.executor(AlertsPlugin.NAME).execute(new AlertHistoryRunnable(alert));
                    }
                }
            } catch (Exception e) {
                if (started()) {
                    logger.error("error during reader thread, restarting queue reader thread...", e);
                    startQueueReaderThread();
                } else {
                    logger.error("error during reader thread", e);
                }
            }
        }
    }

}
