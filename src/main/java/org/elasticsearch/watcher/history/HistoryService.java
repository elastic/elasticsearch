/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.scheduler.Scheduler;
import org.elasticsearch.watcher.support.Callback;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.watcher.watch.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class HistoryService extends AbstractComponent {

    private final HistoryStore historyStore;
    private final WatchExecutor executor;
    private final WatchStore watchStore;
    private final ClusterService clusterService;
    private final WatchLockService watchLockService;
    private final Clock clock;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger initializationRetries = new AtomicInteger();

    @Inject
    public HistoryService(Settings settings, HistoryStore historyStore, WatchExecutor executor,
                          WatchStore watchStore, WatchLockService watchLockService, Scheduler scheduler,
                          ClusterService clusterService, Clock clock) {
        super(settings);
        this.historyStore = historyStore;
        this.executor = executor;
        this.watchStore = watchStore;
        this.watchLockService = watchLockService;
        this.clusterService = clusterService;
        this.clock = clock;
        scheduler.addListener(new SchedulerListener());
    }

    public void start(ClusterState state, Callback<ClusterState> callback) {
        if (started.get()) {
            callback.onSuccess(state);
            return;
        }

        assert executor.queue().isEmpty() : "queue should be empty, but contains " + executor.queue().size() + " elements.";
        Collection<WatchRecord> records = historyStore.loadRecords(state, WatchRecord.State.AWAITS_EXECUTION);
        if (records == null) {
            retry(callback);
            return;
        }
        if (started.compareAndSet(false, true)) {
            logger.debug("starting history service");
            executeRecords(records);
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
            executor.queue().drainTo(cancelledTasks);
            logger.debug("cancelled [{}] queued tasks", cancelledTasks.size());
            logger.debug("stopped history service");
        }
    }

    public boolean started() {
        return started.get();
    }

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long queueSize() {
        return executor.queue().size();
    }

    // TODO: should be removed from the stats api? This is already visible in the thread pool cat api.
    public long largestQueueSize() {
        return executor.largestPoolSize();
    }

    void execute(Watch watch, DateTime scheduledFireTime, DateTime fireTime) throws HistoryException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
        WatchRecord watchRecord = new WatchRecord(watch, scheduledFireTime, fireTime);
        logger.debug("saving watch record [{}]", watch.name());
        historyStore.put(watchRecord);
        executeAsync(watchRecord, watch);
    }

    /*
       The execution of an watch is split into two phases:
       1. the trigger part which just makes sure to store the associated watch record in the history
       2. the actual processing of the watch

       The reason this split is that we don't want to lose the fact watch was triggered. This way, even if the
       thread pool that executes the watches is completely busy, we don't lose the fact that the watch was
       triggered (it'll have its history record)
    */

    void executeAsync(WatchRecord watchRecord, Watch watch) {
        try {
            executor.execute(new WatchExecutionTask(watchRecord, watch));
        } catch (EsRejectedExecutionException e) {
            logger.debug("failed to execute triggered watch [{}]", watchRecord.name());
            watchRecord.update(WatchRecord.State.FAILED, "failed to run triggered watch [" + watchRecord.name() + "] due to thread pool capacity");
            historyStore.update(watchRecord);
        }
    }

    WatchExecution execute(WatchExecutionContext ctx) throws IOException {
        Watch watch = ctx.watch();
        Input.Result inputResult = watch.input().execute(ctx);
        ctx.onInputResult(inputResult);
        Condition.Result conditionResult = watch.condition().execute(ctx);
        ctx.onConditionResult(conditionResult);

        if (conditionResult.met()) {
            Throttler.Result throttleResult = watch.throttler().throttle(ctx);
            ctx.onThrottleResult(throttleResult);

            if (!throttleResult.throttle()) {
                Transform transform = watch.transform();
                if (transform != null) {
                    Transform.Result result = watch.transform().apply(ctx, inputResult.payload());
                    ctx.onTransformResult(result);
                }
                for (Action action : watch.actions()) {
                    Action.Result actionResult = action.execute(ctx);
                    ctx.onActionResult(actionResult);
                }
            }
        }
        return ctx.finish();
    }

    void executeRecords(Collection<WatchRecord> records) {
        assert records != null;
        int counter = 0;
        for (WatchRecord record : records) {
            Watch watch = watchStore.get(record.name());
            if (watch == null) {
                logger.warn("unable to find watch [{}] in watch store. perhaps it has been deleted. skipping...", record.name());
                continue;
            }
            executeAsync(record, watch);
            counter++;
        }
        logger.debug("executed [{}] watches from the watch history", counter);
    }

    private void retry(final Callback<ClusterState> callback) {
        ClusterStateListener clusterStateListener = new ClusterStateListener() {

            @Override
            public void clusterChanged(final ClusterChangedEvent event) {
                // Remove listener, so that it doesn't get called on the next cluster state update:
                assert initializationRetries.decrementAndGet() == 0 : "Only one retry can run at the time";
                clusterService.remove(this);
                // We fork into another thread, because start(...) is expensive and we can't call this from the cluster update thread.
                executor.execute(new Runnable() {

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

    private final class WatchExecutionTask implements Runnable {

        private final WatchRecord watchRecord;
        private final Watch watch;

        private WatchExecutionTask(WatchRecord watchRecord, Watch watch) {
            this.watchRecord = watchRecord;
            this.watch = watch;
        }

        @Override
        public void run() {
            if (!started.get()) {
                logger.debug("can't initiate watch execution as history service is not started, ignoring it...");
                return;
            }
            WatchLockService.Lock lock = watchLockService.acquire(watch.name());
            try {
                watchRecord.update(WatchRecord.State.CHECKING, null);
                logger.debug("checking watch [{}]", watchRecord.name());
                WatchExecutionContext ctx = new WatchExecutionContext(watchRecord.id(), watch, clock.now(), watchRecord.fireTime(), watchRecord.scheduledTime());
                WatchExecution execution = execute(ctx);
                watchRecord.seal(execution);
                historyStore.update(watchRecord);
            } catch (Exception e) {
                if (started()) {
                    logger.warn("failed to execute watch [{}]", e, watchRecord.name());
                    try {
                        watchRecord.update(WatchRecord.State.FAILED, e.getMessage());
                        historyStore.update(watchRecord);
                        
                    } catch (Exception e2) {
                        logger.error("failed to update watch record [{}] failure [{}]", e2, watchRecord, e.getMessage());
                    }
                } else {
                    logger.debug("failed to execute watch [{}] after shutdown", e, watchRecord);
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
            Watch watch = watchStore.get(name);
            if (watch == null) {
                logger.warn("unable to find watch [{}] in the watch store, perhaps it has been deleted", name);
                return;
            }
            try {
                HistoryService.this.execute(watch, scheduledFireTime, fireTime);
            } catch (Exception e) {
                logger.error("failed to execute watch [{}]", e, name);
            }
        }
    }
}
