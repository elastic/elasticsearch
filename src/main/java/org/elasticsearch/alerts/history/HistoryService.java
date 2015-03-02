/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.support.Callback;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class HistoryService extends AbstractComponent {

    private final HistoryStore historyStore;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;
    private final ClusterService clusterService;
    private final AlertLockService alertLockService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger initializationRetries = new AtomicInteger();

    @Inject
    public HistoryService(Settings settings, HistoryStore historyStore, ThreadPool threadPool,
                          AlertsStore alertsStore, AlertLockService alertLockService, Scheduler scheduler,
                          ClusterService clusterService) {
        super(settings);
        this.historyStore = historyStore;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
        this.alertLockService = alertLockService;
        this.clusterService = clusterService;
        scheduler.addListener(new SchedulerListener());
    }

    public void start(ClusterState state, Callback<ClusterState> callback) {
        if (started.get()) {
            callback.onSuccess(state);
            return;
        }

        assert alertsThreadPool().getQueue().isEmpty() : "queue should be empty, but contains " + alertsThreadPool().getQueue().size() + " elements.";
        HistoryStore.LoadResult loadResult = historyStore.loadFiredAlerts(state, FiredAlert.State.AWAITS_EXECUTION);
        if (!loadResult.succeeded()) {
            retry(callback);
            return;
        }
        if (started.compareAndSet(false, true)) {
            logger.debug("starting history service");
            executePreviouslyFiredAlerts(loadResult);
            logger.debug("started history service");
        }
        callback.onSuccess(state);
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.debug("stopping history service");
            // We could also rely on the shutdown in #updateSettings call, but
            // this is a forceful shutdown that also interrupts the worker threads in the threadpool
            List<Runnable> cancelledTasks = new ArrayList<>();
            alertsThreadPool().getQueue().drainTo(cancelledTasks);
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
        executeAsync(firedAlert, alert);
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

    void executeAsync(FiredAlert firedAlert, Alert alert) {
        try {
            alertsThreadPool().execute(new AlertExecutionTask(firedAlert, alert));
        } catch (EsRejectedExecutionException e) {
            logger.debug("[{}] failed to execute fired alert", firedAlert.name());
            firedAlert.update(FiredAlert.State.FAILED, "failed to run fired alert due to thread pool capacity");
            historyStore.update(firedAlert);
        }
    }

    AlertExecution execute(ExecutionContext ctx) throws IOException {
        Alert alert = ctx.alert();
        Input.Result inputResult = alert.input().execute(ctx);
        ctx.onInputResult(inputResult);
        Condition.Result conditionResult = alert.condition().execute(ctx);
        ctx.onConditionResult(conditionResult);

        if (conditionResult.met()) {
            Throttler.Result throttleResult = alert.throttler().throttle(ctx);
            ctx.onThrottleResult(throttleResult);

            if (!throttleResult.throttle()) {
                Transform.Result result = alert.transform().apply(ctx, inputResult.payload());
                ctx.onTransformResult(result);
                for (Action action : alert.actions()) {
                    Action.Result actionResult = action.execute(ctx, result.payload());
                    ctx.onActionResult(actionResult);
                }
            }
        }
        return ctx.finish();
    }

    void executePreviouslyFiredAlerts(HistoryStore.LoadResult loadResult) {
        if (loadResult != null) {
            int counter = 0;
            for (FiredAlert firedAlert : loadResult) {
                Alert alert = alertsStore.getAlert(firedAlert.name());
                if (alert == null) {
                    logger.warn("unable to find alert [{}] in alert store, perhaps it has been deleted. skipping...", firedAlert.name());
                    continue;
                }
                executeAsync(firedAlert, alert);
                counter++;
            }
            logger.debug("executed [{}] not executed previous fired alerts from the alert history index ", counter);
        }
    }

    private EsThreadPoolExecutor alertsThreadPool() {
        return (EsThreadPoolExecutor) threadPool.executor(AlertsPlugin.NAME);
    }

    private void retry(final Callback<ClusterState> callback) {
        ClusterStateListener clusterStateListener = new ClusterStateListener() {

            @Override
            public void clusterChanged(final ClusterChangedEvent event) {
                // Remove listener, so that it doesn't get called on the next cluster state update:
                assert initializationRetries.decrementAndGet() == 0 : "Only one retry can run at the time";
                clusterService.remove(this);
                // We fork into another thread, because start(...) is expensive and we can't call this from the cluster update thread.
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            start(event.state(), callback);
                        } catch (Exception e) {
                            callback.onFailure(e);
                        }
                    }
                });
            }
        };
        assert initializationRetries.incrementAndGet() == 1 : "Only one retry can run at the time";
        clusterService.add(clusterStateListener);
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
                logger.debug("can't run alert execution, because history service is not started, ignoring it...");
                return;
            }
            AlertLockService.Lock lock = alertLockService.acquire(alert.name());
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
