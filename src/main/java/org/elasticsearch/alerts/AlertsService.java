/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryService;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class AlertsService extends AbstractComponent {

    private final Scheduler scheduler;
    private final AlertsStore alertsStore;
    private final HistoryService historyService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final KeyedLock<String> alertLock = new KeyedLock<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    private volatile boolean manuallyStopped;

    @Inject
    public AlertsService(Settings settings, ClusterService clusterService, Scheduler scheduler, AlertsStore alertsStore,
                         IndicesService indicesService, HistoryService historyService,
                         ThreadPool threadPool) {
        super(settings);
        this.scheduler = scheduler;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
        this.historyService = historyService;
        this.historyService.setAlertsService(this);
        this.clusterService = clusterService;

        scheduler.addListener(new SchedulerListener());

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

    public AlertsStore.AlertDelete deleteAlert(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        alertLock.acquire(name);
        try {
            AlertsStore.AlertDelete delete = alertsStore.deleteAlert(name);
            if (delete.deleteResponse().isFound()) {
                scheduler.remove(name);
            }
            return delete;
        } finally {
            alertLock.release(name);
        }
    }

    public IndexResponse putAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        alertLock.acquire(alertName);
        try {
            AlertsStore.AlertPut result = alertsStore.putAlert(alertName, alertSource);
            if (result.previous() == null || !result.previous().schedule().equals(result.current().schedule())) {
                scheduler.schedule(result.current());
            }
            return result.indexResponse();
        } finally {
            alertLock.release(alertName);
        }
    }

    /**
     * TODO: add version, fields, etc support that the core get api has as well.
     */
    public Alert getAlert(String alertName) {
        return alertsStore.getAlert(alertName);
    }

    public State getState() {
        return state.get();
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

    /**
     * This does the necessary actions, so we don't lose the fact that an alert got execute from the {@link org.elasticsearch.alerts.scheduler.Scheduler}
     * It writes the an entry in the alert history index with the proper status for this alert.
     *
     * The rest of the actions happen in {@link #runAlert(org.elasticsearch.alerts.history.FiredAlert)}.
     *
     * The reason the executing of the alert is split into two, is that we don't want to lose the fact that an alert has
     * fired. If we were
     */
    void triggerAlert(String alertName, DateTime scheduledFireTime, DateTime fireTime){
        ensureStarted();
        alertLock.acquire(alertName);
        try {
            Alert alert = alertsStore.getAlert(alertName);
            if (alert == null) {
                logger.warn("unable to find [{}] in the alert store, perhaps it has been deleted", alertName);
                return;
            }

            try {
                historyService.alertFired(alert, scheduledFireTime, fireTime);
            } catch (Exception e) {
                logger.error("failed to schedule alert action for [{}]", e, alert);
            }
        } finally {
            alertLock.release(alertName);
        }
    }

    /**
     * This actually runs the alert:
     * 1) Runs the configured search request
     * 2) Checks if the search request triggered (matches with the defined conditions)
     * 3) If the alert has been triggered, checks if the alert should be throttled
     * 4) If the alert hasn't been throttled runs the configured actions
     */
    public AlertRun runAlert(FiredAlert entry) throws IOException {
        ensureStarted();
        alertLock.acquire(entry.name());
        try {
            Alert alert = alertsStore.getAlert(entry.name());
            if (alert == null) {
                throw new ElasticsearchException("Alert is not available");
            }

            AlertContext ctx = new AlertContext(alert, entry.fireTime(), entry.scheduledTime());

            Trigger.Result triggerResult = alert.trigger().execute(ctx);
            ctx.triggerResult(triggerResult);

            if (triggerResult.triggered()) {
                alert.status().onTrigger(true, entry.fireTime());

                Throttler.Result throttleResult = alert.throttler().throttle(ctx, triggerResult);
                ctx.throttleResult(throttleResult);

                if (!throttleResult.throttle()) {
                    Transform.Result result = alert.transform().apply(ctx, triggerResult.payload());
                    ctx.transformResult(result);

                    for (Action action : alert.actions()){
                        Action.Result actionResult = action.execute(ctx, result.payload());
                        ctx.addActionResult(actionResult);
                    }
                    alert.status().onExecution(entry.scheduledTime());
                } else {
                    alert.status().onThrottle(entry.fireTime(), throttleResult.reason());
                }
            } else {
                alert.status().onTrigger(false, entry.fireTime());
            }
            alert.status().onRun(entry.fireTime());
            alertsStore.updateAlert(alert);
            return new AlertRun(ctx);
        } finally {
            alertLock.release(entry.name());
        }
    }

    /**
     * Acks the alert if needed
     */
    public Alert.Status ackAlert(String alertName) {
        ensureStarted();
        alertLock.acquire(alertName);
        try {
            Alert alert = alertsStore.getAlert(alertName);
            if (alert == null) {
                throw new AlertsException("alert [" + alertName + "] does not exist");
            }
            if (alert.status().onAck(new DateTime())) {
                try {
                    alertsStore.updateAlertStatus(alert);
                } catch (IOException ioe) {
                    throw new AlertsException("failed to update the alert on ack", ioe);
                }
            }
            // we need to create a safe copy of the status
            return new Alert.Status(alert.status());
        } finally {
            alertLock.release(alertName);
        }
    }

    /**
     * Manually starts alerting if not already started
     */
    public void start() {
        manuallyStopped = false;
        logger.info("starting alert service...");
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
            while (true) {
                // It can happen we have still ongoing operations and we wait those operations to finish to avoid
                // that AlertManager or any of its components end up in a illegal state after the state as been set to stopped.
                //
                // For example: An alert action entry may be added while we stopping alerting if we don't wait for
                // ongoing operations to complete. Resulting in once the alert service starts again that more than
                // expected alert action entries are processed.
                //
                // Note: new operations will fail now because the state has been set to: stopping
                if (!alertLock.hasLockedKeys()) {
                    break;
                }
            }

            historyService.stop();
            scheduler.stop();
            alertsStore.stop();
            state.set(State.STOPPED);
            logger.info("alert service has stopped");
        }
    }

    private void internalStart(ClusterState initialState) {
        if (state.compareAndSet(State.STOPPED, State.STARTING)) {
            ClusterState clusterState = initialState;

            // Try to load alert store before the action service, b/c action depends on alert store
            while (true) {
                if (alertsStore.start(clusterState)) {
                    break;
                }
                clusterState = newClusterState(clusterState);
            }
            while (true) {
                if (historyService.start(clusterState)) {
                    break;
                }
                clusterState = newClusterState(clusterState);
            }

            scheduler.start(alertsStore.getAlerts().values());
            state.set(State.STARTED);
            historyService.executePreviouslyFiredAlerts();
            logger.info("alert service has started");
        }
    }

    private void ensureStarted() {
        if (state.get() != State.STARTED) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    public long getNumberOfAlerts() {
        return alertsStore.getAlerts().size();
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

    private class SchedulerListener implements Scheduler.Listener {
        @Override
        public void fire(String alertName, DateTime scheduledFireTime, DateTime fireTime) {
            triggerAlert(alertName, scheduledFireTime, fireTime);
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
                    // a .alertshistory index, but they may not have been restored from the cluster state on disk
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

    public static class AlertRun {

        private final Trigger.Result triggerResult;
        private final Throttler.Result throttleResult;
        private final Map<String, Action.Result> actionsResults;
        private final Payload payload;

        public AlertRun(AlertContext context) {
            triggerResult = context.triggerResult();
            throttleResult = context.throttleResult();
            actionsResults = context.actionsResults();
            payload = context.payload();
        }

        public Trigger.Result triggerResult() {
            return triggerResult;
        }

        public Throttler.Result throttleResult() {
            return throttleResult;
        }

        public Map<String, Action.Result> actionsResults() {
            return actionsResults;
        }

        public Payload payload() {
            return payload;
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
