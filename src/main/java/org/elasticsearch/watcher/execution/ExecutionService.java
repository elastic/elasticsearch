/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchLockService;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;

/**
 */
public class ExecutionService extends AbstractComponent {

    private static final TimeValue DEFAULT_MAX_STOP_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String DEFAULT_MAX_STOP_TIMEOUT_SETTING = "watcher.stop.timeout";

    private final HistoryStore historyStore;
    private final TriggeredWatchStore triggeredWatchStore;
    private final WatchExecutor executor;
    private final WatchStore watchStore;
    private final WatchLockService watchLockService;
    private final Clock clock;
    private final TimeValue defaultThrottlePeriod;
    private final TimeValue maxStopTimeout;

    private volatile CurrentExecutions currentExecutions = null;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public ExecutionService(Settings settings, HistoryStore historyStore, TriggeredWatchStore triggeredWatchStore, WatchExecutor executor, WatchStore watchStore,
                            WatchLockService watchLockService, Clock clock, WatcherSettingsValidation settingsValidation) {
        super(settings);
        this.historyStore = historyStore;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executor = executor;
        this.watchStore = watchStore;
        this.watchLockService = watchLockService;
        this.clock = clock;
        this.defaultThrottlePeriod = componentSettings.getAsTime("default_throttle_period", TimeValue.timeValueSeconds(5));
        maxStopTimeout = settings.getAsTime(DEFAULT_MAX_STOP_TIMEOUT_SETTING, DEFAULT_MAX_STOP_TIMEOUT);
        if (ExecutionService.this.defaultThrottlePeriod.millis() < 0) {
            settingsValidation.addError("watcher.execution.default_throttle_period", "time value cannot be negative");
        }
    }

    public void start(ClusterState state) {
        if (started.get()) {
            return;
        }

        assert executor.queue().isEmpty() : "queue should be empty, but contains " + executor.queue().size() + " elements.";
        if (started.compareAndSet(false, true)) {
            logger.debug("starting execution service");
            historyStore.start();
            triggeredWatchStore.start();
            currentExecutions = new CurrentExecutions();
            Collection<TriggeredWatch> records = triggeredWatchStore.loadTriggeredWatches(state);
            executeRecords(records);
            logger.debug("started execution service");
        }
    }

    public boolean validate(ClusterState state) {
        return historyStore.validate(state) && triggeredWatchStore.validate(state);
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.debug("stopping execution service");
            // We could also rely on the shutdown in #updateSettings call, but
            // this is a forceful shutdown that also interrupts the worker threads in the threadpool
            List<Runnable> cancelledTasks = new ArrayList<>();
            executor.queue().drainTo(cancelledTasks);

            currentExecutions.sealAndAwaitEmpty(maxStopTimeout);
            triggeredWatchStore.stop();
            historyStore.stop();
            logger.debug("cancelled [{}] queued tasks", cancelledTasks.size());
            logger.debug("stopped execution service");
        }
    }

    public boolean started() {
        return started.get();
    }

    public TimeValue defaultThrottlePeriod() {
        return defaultThrottlePeriod;
    }

    public long queueSize() {
        return executor.queue().size();
    }

    public long largestQueueSize() {
        return executor.largestPoolSize();
    }

    public List<WatchExecutionSnapshot> currentExecutions() {
        List<WatchExecutionSnapshot> currentExecutions = new ArrayList<>();
        for (WatchExecution watchExecution : this.currentExecutions) {
            currentExecutions.add(watchExecution.createSnapshot());
        }
        // Lets show the longest running watch first:
        Collections.sort(currentExecutions, new Comparator<WatchExecutionSnapshot>() {
            @Override
            public int compare(WatchExecutionSnapshot e1, WatchExecutionSnapshot e2) {
                return e1.executionTime().compareTo(e2.executionTime());
            }
        });
        return currentExecutions;
    }

    public List<QueuedWatch> queuedWatches() {
        List<Runnable> snapshot = new ArrayList<>(executor.queue());
        if (snapshot.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueuedWatch> queuedWatches = new ArrayList<>(snapshot.size());
        for (Runnable task : snapshot) {
            WatchExecutionTask executionTask = (WatchExecutionTask) task;
            queuedWatches.add(new QueuedWatch(executionTask.ctx));
        }
        // Lets show the execution that pending the longest first:
        Collections.sort(queuedWatches, new Comparator<QueuedWatch>() {
            @Override
            public int compare(QueuedWatch e1, QueuedWatch e2) {
                return e1.executionTime().compareTo(e2.executionTime());
            }
        });
        return queuedWatches;
    }

    void processEventsAsync(Iterable<TriggerEvent> events) throws WatcherException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
        final LinkedList<TriggeredWatch> triggeredWatches = new LinkedList<>();
        final LinkedList<TriggeredExecutionContext> contexts = new LinkedList<>();

        DateTime now = clock.now(UTC);
        for (TriggerEvent event : events) {
            Watch watch = watchStore.get(event.jobName());
            if (watch == null) {
                logger.warn("unable to find watch [{}] in the watch store, perhaps it has been deleted", event.jobName());
                continue;
            }
            TriggeredExecutionContext ctx = new TriggeredExecutionContext(watch, now, event, defaultThrottlePeriod);
            contexts.add(ctx);
            triggeredWatches.add(new TriggeredWatch(ctx.id(), event));
        }

        logger.debug("saving watch records [{}]", triggeredWatches.size());
        if (triggeredWatches.size() == 0) {
            return;
        }

        if (triggeredWatches.size() == 1) {
            final TriggeredWatch triggeredWatch = triggeredWatches.getFirst();
            final TriggeredExecutionContext ctx = contexts.getFirst();
            triggeredWatchStore.put(triggeredWatch, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean aBoolean) {
                    executeAsync(ctx, triggeredWatch);
                }

                @Override
                public void onFailure(Throwable e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof EsRejectedExecutionException) {
                        logger.debug("failed to store watch record [{}]/[{}] due to overloaded threadpool [{}]", triggeredWatch, ctx.id(), ExceptionsHelper.detailedMessage(e));
                    } else {
                        logger.warn("failed to store watch record [{}]/[{}]", e, triggeredWatch, ctx.id());
                    }
                }
            });
        } else {
            triggeredWatchStore.putAll(triggeredWatches, new ActionListener<List<Integer>>() {
                @Override
                public void onResponse(List<Integer> successFullSlots) {
                    for (Integer slot : successFullSlots) {
                        executeAsync(contexts.get(slot), triggeredWatches.get(slot));
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof EsRejectedExecutionException) {
                        logger.debug("failed to store watch records due to overloaded threadpool [{}]", ExceptionsHelper.detailedMessage(e));
                    } else {
                        logger.warn("failed to store watch records", e);
                    }
                }
            });
        }
    }

    void processEventsSync(Iterable<TriggerEvent> events) throws WatcherException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
        final LinkedList<TriggeredWatch> triggeredWatches = new LinkedList<>();
        final LinkedList<TriggeredExecutionContext> contexts = new LinkedList<>();

        DateTime now = clock.now(UTC);
        for (TriggerEvent event : events) {
            Watch watch = watchStore.get(event.jobName());
            if (watch == null) {
                logger.warn("unable to find watch [{}] in the watch store, perhaps it has been deleted", event.jobName());
                continue;
            }
            TriggeredExecutionContext ctx = new TriggeredExecutionContext(watch, now, event, defaultThrottlePeriod);
            contexts.add(ctx);
            triggeredWatches.add(new TriggeredWatch(ctx.id(), event));
        }

        logger.debug("saving watch records [{}]", triggeredWatches.size());
        if (triggeredWatches.size() == 0) {
            return;
        }

        if (triggeredWatches.size() == 1) {
            final TriggeredWatch triggeredWatch = triggeredWatches.getFirst();
            final TriggeredExecutionContext ctx = contexts.getFirst();
            triggeredWatchStore.put(triggeredWatch);
            executeAsync(ctx, triggeredWatch);
        } else {
            List<Integer> slots = triggeredWatchStore.putAll(triggeredWatches);
            for (Integer slot : slots) {
                executeAsync(contexts.get(slot), triggeredWatches.get(slot));
            }
        }
    }

    public WatchRecord execute(WatchExecutionContext ctx) throws IOException {
        WatchRecord record;
        WatchLockService.Lock lock = watchLockService.acquire(ctx.watch().id());
        try {
            currentExecutions.put(ctx.watch().id(), new WatchExecution(ctx, Thread.currentThread()));
            record = executeInner(ctx);
            if (ctx.recordExecution()) {
                watchStore.updateStatus(ctx.watch());
            }
        } catch (DocumentMissingException vcee) {
            throw new WatchMissingException("failed to update the watch [{}] on execute perhaps it was force deleted", vcee, ctx.watch().id());
        } finally {
            currentExecutions.remove(ctx.watch().id());
            lock.release();
        }
        if (ctx.recordExecution()) {
            historyStore.put(record);
        }
        return record;
    }

    /*
       The execution of an watch is split into two phases:
       1. the trigger part which just makes sure to store the associated watch record in the history
       2. the actual processing of the watch

       The reason this split is that we don't want to lose the fact watch was triggered. This way, even if the
       thread pool that executes the watches is completely busy, we don't lose the fact that the watch was
       triggered (it'll have its history record)
    */

    private void executeAsync(WatchExecutionContext ctx, TriggeredWatch triggeredWatch) {
        try {
            executor.execute(new WatchExecutionTask(ctx));
        } catch (EsRejectedExecutionException e) {
            String message = "failed to run triggered watch [" + triggeredWatch.id() + "] due to thread pool capacity";
            logger.debug(message);
            WatchRecord record = ctx.abort(message, ExecutionState.FAILED);
            historyStore.put(record);
            triggeredWatchStore.delete(triggeredWatch.id());
        }
    }

    WatchRecord executeInner(WatchExecutionContext ctx) throws IOException {
        ctx.start();
        Watch watch = ctx.watch();
        ctx.beforeInput();
        Input.Result inputResult = ctx.inputResult();
        if (inputResult == null) {
            inputResult = watch.input().execute(ctx);
            ctx.onInputResult(inputResult);
        }
        ctx.beforeCondition();
        Condition.Result conditionResult = ctx.conditionResult();
        if (conditionResult == null) {
            conditionResult = watch.condition().execute(ctx);
            ctx.onConditionResult(conditionResult);
        }

        if (conditionResult.met()) {
            ctx.beforeAction();
            for (ActionWrapper action : watch.actions()) {
                ActionWrapper.Result actionResult = action.execute(ctx);
                ctx.onActionResult(actionResult);
            }
        }
        return ctx.finish();
    }

    void executeRecords(Collection<TriggeredWatch> triggeredWatches) {
        assert triggeredWatches != null;
        int counter = 0;
        for (TriggeredWatch triggeredWatch : triggeredWatches) {
            Watch watch = watchStore.get(triggeredWatch.id().watchId());
            if (watch == null) {
                String message = "unable to find watch for record [" + triggeredWatch.id().watchId() + "]/[" + triggeredWatch.id() + "], perhaps it has been deleted, ignoring...";
                WatchRecord record = new WatchRecord(triggeredWatch.id(), triggeredWatch.triggerEvent(), message, ExecutionState.DELETED_WHILE_QUEUED);
                historyStore.put(record);
                triggeredWatchStore.delete(triggeredWatch.id());
            } else {
                TriggeredExecutionContext ctx = new TriggeredExecutionContext(watch, clock.now(UTC), triggeredWatch.triggerEvent(), defaultThrottlePeriod);
                executeAsync(ctx, triggeredWatch);
                counter++;
            }
        }
        logger.debug("executed [{}] watches from the watch history", counter);
    }

    private final class WatchExecutionTask implements Runnable {

        private final WatchExecutionContext ctx;

        private WatchExecutionTask(WatchExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!started.get()) {
                logger.debug("can't initiate watch execution as execution service is not started, ignoring it...");
                return;
            }
            logger.trace("executing [{}] [{}]", ctx.watch().id(), ctx.id());

            WatchRecord record = null;
            WatchLockService.Lock lock = watchLockService.acquire(ctx.watch().id());
            try {
                currentExecutions.put(ctx.watch().id(), new WatchExecution(ctx, Thread.currentThread()));
                if (watchStore.get(ctx.watch().id()) == null) {
                    //Fail fast if we are trying to execute a deleted watch
                    String message = "unable to find watch for record [" + ctx.id() + "], perhaps it has been deleted, ignoring...";
                    record = ctx.abort(message, ExecutionState.DELETED_WHILE_QUEUED);
                } else {
                    logger.debug("checking watch [{}]", ctx.id().watchId());
                    record = executeInner(ctx);
                    if (ctx.recordExecution()) {
                        watchStore.updateStatus(ctx.watch());
                    }
                }
            } catch (Exception e) {
                String detailedMessage = ExceptionsHelper.detailedMessage(e);
                logger.warn("failed to execute watch [{}], failure [{}]", ctx.id().value(), detailedMessage);
                record = ctx.abort(detailedMessage, ExecutionState.FAILED);
            } finally {
                // The recordExecution doesn't need to check if it is in a started state here, because the
                // ExecutionService doesn't stop before the WatchLockService stops
                if (record != null && ctx.recordExecution()) {
                    try {
                        historyStore.put(record);
                        triggeredWatchStore.delete(ctx.id());
                    } catch (Exception e) {
                        logger.error("failed to update watch record [{}], failure [{}], record failure if any [{}]", ctx.id(), ExceptionsHelper.detailedMessage(e), record.message());
                    }
                }
                currentExecutions.remove(ctx.watch().id());
                lock.release();
                logger.trace("finished [{}]/[{}]", ctx.watch().id(), ctx.id());
            }

        }
    }

    public static class WatchExecution {

        private final WatchExecutionContext context;
        private final Thread executionThread;

        public WatchExecution(WatchExecutionContext context, Thread executionThread) {
            this.context = context;
            this.executionThread = executionThread;
        }

        public WatchExecutionSnapshot createSnapshot() {
            return context.createSnapshot(executionThread);
        }

    }
}
