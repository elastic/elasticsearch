/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class HistoryService extends AbstractComponent {

    private final HistoryStore historyStore;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;
    private final AlertLockService alertLockService;

    private final AtomicBoolean started = new AtomicBoolean(false);

    // Holds fired alerts that were fired before on a different elected master node, but never had the chance to run.
    private volatile ImmutableList<FiredAlert> previousFiredAlerts = ImmutableList.of();

    @Inject
    public HistoryService(Settings settings, HistoryStore historyStore, ThreadPool threadPool,
                          AlertsStore alertsStore, AlertLockService alertLockService, Scheduler scheduler) {
        super(settings);
        this.historyStore = historyStore;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
        this.alertLockService = alertLockService;
        scheduler.addListener(new SchedulerListener());
    }

    public boolean start(ClusterState state) {
        if (started.get()) {
            return true;
        }

        assert alertsThreadPool().getQueue().isEmpty() : "queue should be empty, but contains " + alertsThreadPool().getQueue().size() + " elements.";
        HistoryStore.LoadResult loadResult = historyStore.loadFiredAlerts(state, FiredAlert.State.AWAITS_EXECUTION);
        if (!loadResult.succeeded()) {
            return false;
        }
        this.previousFiredAlerts = ImmutableList.copyOf(loadResult);
        if (!previousFiredAlerts.isEmpty()) {
            logger.debug("loaded [{}] actions from the alert history index into actions queue", previousFiredAlerts.size());
        }
        logger.debug("starting history service");
        if (started.compareAndSet(false, true)) {
            if (alertsThreadPool().isShutdown()) {
                logger.info("Restarting thread pool that had been shutdown");
                // this update thread pool settings work around is for restarting the alerts thread pool,
                // that creates a new alerts thread pool and cleans up the existing one that has previously been shutdown.
                int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
                /***
                 *TODO Horrible horrible hack to make sure that settings are always different from the previous settings
                 *
                 * THIS NEEDS TO CHANGE ASAP
                 */
                int queueSize = alertsThreadPool().getQueue().remainingCapacity();
                if (queueSize % 2 == 0){
                    queueSize = queueSize + 1;
                } else {
                    queueSize = queueSize - 1;
                }
                //TODO END HORRIBLE HACK

                threadPool.updateSettings(AlertsPlugin.alertThreadPoolSettings(availableProcessors, queueSize));
                assert !alertsThreadPool().isShutdown();
            }
            logger.debug("started history service");
        }
        executePreviouslyFiredAlerts();
        return true;
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

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long getQueueSize() {
        return alertsThreadPool().getQueue().size();
    }

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long getLargestQueueSize() {
        return alertsThreadPool().getLargestPoolSize();
    }

    void fire(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws HistoryException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
        FiredAlert firedAlert = new FiredAlert(alert, scheduledFireTime, fireTime);
        logger.debug("adding fired alert [{}]", alert.name());
        historyStore.put(firedAlert);
        execute(firedAlert, alert);
    }

    void execute(FiredAlert firedAlert, Alert alert) {
        try {
            if (alertsThreadPool().isShutdown()) {
                throw new AlertsException("attempting to add to a shutdown thread pool");
            }
            alertsThreadPool().execute(new AlertExecutionTask(firedAlert, alert));
        } catch (EsRejectedExecutionException e) {
            logger.debug("[{}] failed to execute fired alert", firedAlert.name());
            firedAlert.update(FiredAlert.State.FAILED, "failed to run fired alert due to thread pool capacity");
            historyStore.update(firedAlert);
        }
    }

    void executePreviouslyFiredAlerts() {
        ImmutableList<FiredAlert> firedAlerts = this.previousFiredAlerts;
        if (firedAlerts != null) {
            this.previousFiredAlerts = ImmutableList.of();
            for (FiredAlert firedAlert : firedAlerts) {
                Alert alert = alertsStore.getAlert(firedAlert.name());
                if (alert == null) {
                    logger.warn("unable to find alert [{}] in alert store, perhaps it has been deleted. skipping...", firedAlert.name());
                    continue;
                }
                execute(firedAlert, alert);
            }
        }
    }

    private EsThreadPoolExecutor alertsThreadPool() {
        return (EsThreadPoolExecutor) threadPool.executor(AlertsPlugin.NAME);
    }

    private final class AlertExecutionTask implements Runnable {

        private final FiredAlert firedAlert;
        private final Alert alert;

        private AlertExecutionTask(FiredAlert firedAlert, Alert alert) {
            this.firedAlert = firedAlert;
            this.alert = alert;
        }

        @Override
        public void run() {
            if (!started.get()) {
                throw new ElasticsearchIllegalStateException("not started");
            }
            try {
                firedAlert.update(FiredAlert.State.CHECKING, null);
                logger.debug("checking alert [{}]", firedAlert.name());
                ExecutionContext ctx = new ExecutionContext(firedAlert.id(), alert, firedAlert.fireTime(), firedAlert.scheduledTime());
                AlertExecution alertExecution = execute(ctx);
                firedAlert.update(alertExecution);
                historyStore.update(firedAlert);
            } catch (Exception e) {
                if (started()) {
                    logger.warn("failed to run alert [{}]", e, firedAlert.name());
                    try {
                        firedAlert.update(FiredAlert.State.FAILED, e.getMessage());
                        historyStore.update(firedAlert);
                        
                    } catch (Exception e2) {
                        logger.error("failed to update fired alert [{}] with the error message", e2, firedAlert);
                    }
                } else {
                    logger.debug("failed to execute fired alert [{}] after shutdown", e, firedAlert);
                }
            }
        }

        /*
           The execution of an alert is split into operations, a schedule part which just makes sure that store the fact an alert
           has fired and an execute part which actually executed the alert.

           The reason this is split into two operations is that we don't want to lose the fact an alert has fired. If we
           would not split the execution of an alert and many alerts fire in a small window of time then it can happen that
           thread pool that receives fired jobs from the quartz scheduler is going to reject jobs and then we would never
           know about jobs that have fired. By splitting the execution of fired jobs into two operations we lower the chance
           we lose fired jobs signficantly.
         */

        AlertExecution execute(ExecutionContext ctx) throws IOException {
            AlertLockService.Lock lock = alertLockService.acquire(alert.name());
            try {

                Condition.Result conditionResult = alert.condition().execute(ctx);
                ctx.onConditionResult(conditionResult);

                if (conditionResult.met()) {
                    Throttler.Result throttleResult = alert.throttler().throttle(ctx, conditionResult);
                    ctx.onThrottleResult(throttleResult);

                    if (!throttleResult.throttle()) {
                        Transform.Result result = alert.transform().apply(ctx, conditionResult.payload());
                        ctx.onTransformResult(result);

                        for (Action action : alert.actions()) {
                            Action.Result actionResult = action.execute(ctx, result.payload());
                            ctx.onActionResult(actionResult);
                        }
                    }
                }
                return ctx.finish();

            } finally {
                lock.release();
            }
        }
    }

    private class SchedulerListener implements Scheduler.Listener {
        @Override
        public void fire(String name, DateTime scheduledFireTime, DateTime fireTime) {
            if (!started.get()) {
                throw new ElasticsearchIllegalStateException("not started");
            }
            Alert alert = alertsStore.getAlert(name);
            if (alert == null) {
                logger.warn("unable to find [{}] in the alert store, perhaps it has been deleted", name);
                return;
            }
            try {
                HistoryService.this.fire(alert, scheduledFireTime, fireTime);
            } catch (Exception e) {
                logger.error("failed to fire alert [{}]", e, name);
            }
        }
    }
}
