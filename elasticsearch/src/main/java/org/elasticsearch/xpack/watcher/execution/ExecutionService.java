/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.common.stats.Counters;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.condition.Condition;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.joda.time.DateTimeZone.UTC;

public final class ExecutionService extends AbstractComponent {

    public static final Setting<TimeValue> DEFAULT_THROTTLE_PERIOD_SETTING =
        Setting.positiveTimeSetting("xpack.watcher.execution.default_throttle_period",
                                    TimeValue.timeValueSeconds(5), Setting.Property.NodeScope);

    private final MeanMetric totalExecutionsTime = new MeanMetric();
    private final Map<String, MeanMetric> actionByTypeExecutionTime = new HashMap<>();

    private final HistoryStore historyStore;
    private final TriggeredWatchStore triggeredWatchStore;
    private final WatchExecutor executor;
    private final Clock clock;
    private final TimeValue defaultThrottlePeriod;
    private final TimeValue maxStopTimeout;
    private final ThreadPool threadPool;
    private final Watch.Parser parser;
    private final WatcherClientProxy client;

    private volatile CurrentExecutions currentExecutions;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ExecutionService(Settings settings, HistoryStore historyStore, TriggeredWatchStore triggeredWatchStore, WatchExecutor executor,
                            Clock clock, ThreadPool threadPool, Watch.Parser parser,
                            WatcherClientProxy client) {
        super(settings);
        this.historyStore = historyStore;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executor = executor;
        this.clock = clock;
        this.defaultThrottlePeriod = DEFAULT_THROTTLE_PERIOD_SETTING.get(settings);
        this.maxStopTimeout = Watcher.MAX_STOP_TIMEOUT_SETTING.get(settings);
        this.threadPool = threadPool;
        this.parser = parser;
        this.client = client;
    }

    public void start(ClusterState state) throws Exception {
        if (started.get()) {
            return;
        }

        assert executor.queue().isEmpty() : "queue should be empty, but contains " + executor.queue().size() + " elements.";
        if (started.compareAndSet(false, true)) {
            try {
                logger.debug("starting execution service");
                historyStore.start();
                triggeredWatchStore.start();
                currentExecutions = new CurrentExecutions();
                Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.loadTriggeredWatches(state);
                executeTriggeredWatches(triggeredWatches);
                logger.debug("started execution service");
            } catch (Exception e) {
                started.set(false);
                throw e;
            }
        }
    }

    public boolean validate(ClusterState state) {
        boolean triggeredWatchStoreReady = triggeredWatchStore.validate(state);
        try {
            IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metaData());
            if (indexMetaData != null) {
                return triggeredWatchStoreReady && state.routingTable().index(indexMetaData.getIndex()).allPrimaryShardsActive();
            }
        } catch (Exception e) {
            return false;
        }
        return triggeredWatchStoreReady;
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            logger.debug("stopping execution service");
            // We could also rely on the shutdown in #updateSettings call, but
            // this is a forceful shutdown that also interrupts the worker threads in the thread pool
            int cancelledTaskCount = executor.queue().drainTo(new ArrayList<>());

            currentExecutions.sealAndAwaitEmpty(maxStopTimeout);
            triggeredWatchStore.stop();
            historyStore.stop();
            logger.debug("cancelled [{}] queued tasks", cancelledTaskCount);
            logger.debug("stopped execution service");
        }
    }

    public boolean started() {
        return started.get();
    }

    public TimeValue defaultThrottlePeriod() {
        return defaultThrottlePeriod;
    }

    public long executionThreadPoolQueueSize() {
        return executor.queue().size();
    }

    public long executionThreadPoolMaxSize() {
        return executor.largestPoolSize();
    }

    // for testing only
    CurrentExecutions getCurrentExecutions() {
        return currentExecutions;
    }

    public List<WatchExecutionSnapshot> currentExecutions() {
        List<WatchExecutionSnapshot> currentExecutions = new ArrayList<>();
        for (WatchExecution watchExecution : this.currentExecutions) {
            currentExecutions.add(watchExecution.createSnapshot());
        }
        // Lets show the longest running watch first:
        Collections.sort(currentExecutions, Comparator.comparing(WatchExecutionSnapshot::executionTime));
        return currentExecutions;
    }

    public List<QueuedWatch> queuedWatches() {
        List<Runnable> snapshot = new ArrayList<>();
        executor.tasks().forEach(t -> snapshot.add(t));
        if (snapshot.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueuedWatch> queuedWatches = new ArrayList<>(snapshot.size());
        for (Runnable task : snapshot) {
            WatchExecutionTask executionTask = (WatchExecutionTask) task;
            queuedWatches.add(new QueuedWatch(executionTask.ctx));
        }

        // Lets show the execution that pending the longest first:
        // Note that the type parameters on comparing are required to make the comparing method work
        Collections.sort(queuedWatches, Comparator.<QueuedWatch, DateTime>comparing(QueuedWatch::executionTime));
        return queuedWatches;
    }

    void processEventsAsync(Iterable<TriggerEvent> events) throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("not started");
        }
        final List<TriggeredWatch> triggeredWatches = new ArrayList<>();
        final List<TriggeredExecutionContext> contexts = new ArrayList<>();
        DateTime now = new DateTime(clock.millis(), UTC);

        threadPool.generic().execute(() -> {
            for (TriggerEvent event : events) {
                GetResponse response = client.getWatch(event.jobName());
                if (response.isExists() == false) {
                    logger.warn("unable to find watch [{}] in watch index, perhaps it has been deleted", event.jobName());
                } else {
                    try {
                        Watch watch =
                                parser.parseWithSecrets(response.getId(), true, response.getSourceAsBytesRef(), now, XContentType.JSON);
                        TriggeredExecutionContext ctx = new TriggeredExecutionContext(watch, now, event, defaultThrottlePeriod);
                        contexts.add(ctx);
                        triggeredWatches.add(new TriggeredWatch(ctx.id(), event));
                    } catch (IOException e) {
                        logger.warn("unable to parse watch [{}]", event.jobName());
                    }
                }
            }

            if (triggeredWatches.isEmpty() == false) {
                logger.debug("saving triggered [{}] watches", triggeredWatches.size());

                triggeredWatchStore.putAll(triggeredWatches, ActionListener.wrap(
                        (slots) -> {
                            int slot = 0;
                            while ((slot = slots.nextSetBit(slot)) != -1) {
                                executeAsync(contexts.get(slot), triggeredWatches.get(slot));
                                slot++;
                            }
                        },
                        (e) -> logger.warn("failed to store watch [] records", e)));
            }
        });
    }

    void processEventsSync(Iterable<TriggerEvent> events) throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("not started");
        }
        final List<TriggeredWatch> triggeredWatches = new ArrayList<>();
        final List<TriggeredExecutionContext> contexts = new ArrayList<>();

        DateTime now = new DateTime(clock.millis(), UTC);
        for (TriggerEvent event : events) {
            GetResponse response = client.getWatch(event.jobName());
            if (response.isExists() == false) {
                logger.warn("unable to find watch [{}] in watch index, perhaps it has been deleted", event.jobName());
                continue;
            }
            Watch watch = parser.parseWithSecrets(response.getId(), true, response.getSourceAsBytesRef(), now, XContentType.JSON);
            TriggeredExecutionContext ctx = new TriggeredExecutionContext(watch, now, event, defaultThrottlePeriod);
            contexts.add(ctx);
            triggeredWatches.add(new TriggeredWatch(ctx.id(), event));
        }

        if (triggeredWatches.isEmpty() == false) {
            logger.debug("saving triggered [{}] watches", triggeredWatches.size());
            BitSet slots = triggeredWatchStore.putAll(triggeredWatches);
            int slot = 0;
            while ((slot = slots.nextSetBit(slot)) != -1) {
                executeAsync(contexts.get(slot), triggeredWatches.get(slot));
                slot++;
            }
        }
    }

    public WatchRecord execute(WatchExecutionContext ctx) {
        WatchRecord record = null;
        try {
            boolean executionAlreadyExists = currentExecutions.put(ctx.watch().id(), new WatchExecution(ctx, Thread.currentThread()));
            if (executionAlreadyExists) {
                logger.trace("not executing watch [{}] because it is already queued", ctx.watch().id());
                record = ctx.abortBeforeExecution(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED, "Watch is already queued in thread pool");
            } else {
                boolean watchExists = false;
                try {
                    GetResponse response = client.getWatch(ctx.watch().id());
                    watchExists = response.isExists();
                } catch (IndexNotFoundException e) {}

                if (ctx.knownWatch() && watchExists == false) {
                    // fail fast if we are trying to execute a deleted watch
                    String message = "unable to find watch for record [" + ctx.id() + "], perhaps it has been deleted, ignoring...";
                    record = ctx.abortBeforeExecution(ExecutionState.NOT_EXECUTED_WATCH_MISSING, message);

                } else {
                    logger.debug("executing watch [{}]", ctx.id().watchId());

                    record = executeInner(ctx);
                    if (ctx.recordExecution()) {
                        client.updateWatchStatus(ctx.watch());
                    }
                }
            }
        } catch (Exception e) {
            record = createWatchRecord(record, ctx, e);
            logWatchRecord(ctx, e);
        } finally {
            if (ctx.knownWatch() && record != null && ctx.recordExecution()) {
                try {
                    if (ctx.overrideRecordOnConflict()) {
                        historyStore.forcePut(record);
                    } else {
                        historyStore.put(record);
                    }
                } catch (Exception e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to update watch record [{}]", ctx.id()), e);
                    // TODO log watch record in logger, when saving in history store failed, otherwise the info is gone!
                }
            }
            try {
                triggeredWatchStore.delete(ctx.id());
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to delete triggered watch [{}]", ctx.id()), e);
            }
            currentExecutions.remove(ctx.watch().id());
            logger.trace("finished [{}]/[{}]", ctx.watch().id(), ctx.id());
        }
        return record;
    }

    private WatchRecord createWatchRecord(WatchRecord existingRecord, WatchExecutionContext ctx, Exception e) {
        // it is possible that the watch store update failed, the execution phase is finished
        if (ctx.executionPhase().sealed()) {
            if (existingRecord == null) {
                return new WatchRecord.ExceptionWatchRecord(ctx, e);
            } else {
                return new WatchRecord.ExceptionWatchRecord(existingRecord, e);
            }
        } else {
            return ctx.abortFailedExecution(e);
        }
    }

    private void logWatchRecord(WatchExecutionContext ctx, Exception e) {
        // failed watches stack traces are only logged in debug, otherwise they should be checked out in the history
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to execute watch [{}]", ctx.id()), e);
        } else {
            logger.warn("Failed to execute watch [{}]", ctx.id());
        }
    }

    /*
       The execution of an watch is split into two phases:
       1. the trigger part which just makes sure to store the associated watch record in the history
       2. the actual processing of the watch

       The reason this split is that we don't want to lose the fact watch was triggered. This way, even if the
       thread pool that executes the watches is completely busy, we don't lose the fact that the watch was
       triggered (it'll have its history record)
    */
    private void executeAsync(WatchExecutionContext ctx, final TriggeredWatch triggeredWatch) {
        try {
            executor.execute(new WatchExecutionTask(ctx));
        } catch (EsRejectedExecutionException e) {
            String message = "failed to run triggered watch [" + triggeredWatch.id() + "] due to thread pool capacity";
            WatchRecord record = ctx.abortBeforeExecution(ExecutionState.FAILED, message);
            try {
                if (ctx.overrideRecordOnConflict()) {
                    historyStore.forcePut(record);
                } else {
                    historyStore.put(record);
                }
            } catch (Exception exc) {
                logger.error((Supplier<?>) () ->
                        new ParameterizedMessage("Error storing watch history record for watch [{}] after thread pool rejection",
                                triggeredWatch.id()), exc);
            }

            try {
                triggeredWatchStore.delete(triggeredWatch.id());
            } catch (Exception exc) {
                logger.error((Supplier<?>) () ->
                        new ParameterizedMessage("Error deleting triggered watch store record for watch [{}] after thread pool " +
                                "rejection", triggeredWatch.id()), exc);
            }
        };
    }

    WatchRecord executeInner(WatchExecutionContext ctx) {
        ctx.start();
        Watch watch = ctx.watch();

        // input
        ctx.beforeInput();
        Input.Result inputResult = ctx.inputResult();
        if (inputResult == null) {
            inputResult = watch.input().execute(ctx, ctx.payload());
            ctx.onInputResult(inputResult);
        }
        if (inputResult.status() == Input.Result.Status.FAILURE) {
            return ctx.abortFailedExecution("failed to execute watch input");
        }

        // condition
        ctx.beforeCondition();
        Condition.Result conditionResult = ctx.conditionResult();
        if (conditionResult == null) {
            conditionResult = watch.condition().execute(ctx);
            ctx.onConditionResult(conditionResult);
        }
        if (conditionResult.status() == Condition.Result.Status.FAILURE) {
            return ctx.abortFailedExecution("failed to execute watch condition");
        }

        if (conditionResult.met()) {
            if (watch.actions().size() > 0 && watch.transform() != null) {
                ctx.beforeWatchTransform();
                Transform.Result transformResult = watch.transform().execute(ctx, ctx.payload());
                ctx.onWatchTransformResult(transformResult);
                if (transformResult.status() == Transform.Result.Status.FAILURE) {
                    return ctx.abortFailedExecution("failed to execute watch transform");
                }
            }

            // actions
            ctx.beforeActions();
            for (ActionWrapper action : watch.actions()) {
                long now = System.currentTimeMillis();
                ActionWrapper.Result actionResult = action.execute(ctx);
                long executionTime = System.currentTimeMillis() - now;
                String type = action.action().type();
                actionByTypeExecutionTime.putIfAbsent(type, new MeanMetric());
                actionByTypeExecutionTime.get(type).inc(executionTime);
                ctx.onActionResult(actionResult);
            }
        }

        WatchRecord record = ctx.finish();
        totalExecutionsTime.inc(record.result().executionDurationMs());
        return record;
    }

    void executeTriggeredWatches(Collection<TriggeredWatch> triggeredWatches) throws Exception {
        assert triggeredWatches != null;
        int counter = 0;
        for (TriggeredWatch triggeredWatch : triggeredWatches) {
            GetResponse response = client.getWatch(triggeredWatch.id().watchId());
            if (response.isExists() == false) {
                String message = "unable to find watch for record [" + triggeredWatch.id().watchId() + "]/[" + triggeredWatch.id() +
                        "], perhaps it has been deleted, ignoring...";
                WatchRecord record = new WatchRecord.MessageWatchRecord(triggeredWatch.id(), triggeredWatch.triggerEvent(),
                        ExecutionState.NOT_EXECUTED_WATCH_MISSING, message);
                historyStore.forcePut(record);
                triggeredWatchStore.delete(triggeredWatch.id());
            } else {
                DateTime now = new DateTime(clock.millis(), UTC);
                Watch watch = parser.parseWithSecrets(response.getId(), true, response.getSourceAsBytesRef(), now, XContentType.JSON);
                TriggeredExecutionContext ctx =
                        new StartupExecutionContext(watch, now, triggeredWatch.triggerEvent(), defaultThrottlePeriod);
                executeAsync(ctx, triggeredWatch);
                counter++;
            }
        }
        logger.debug("triggered execution of [{}] watches", counter);
    }

    public Map<String, Object> usageStats() {
        Counters counters = new Counters();
        counters.inc("execution.actions._all.total", totalExecutionsTime.count());
        counters.inc("execution.actions._all.total_time_in_ms", totalExecutionsTime.sum());

        for (Map.Entry<String, MeanMetric> entry : actionByTypeExecutionTime.entrySet()) {
            counters.inc("execution.actions." + entry.getKey() + ".total", entry.getValue().count());
            counters.inc("execution.actions." + entry.getKey() + ".total_time_in_ms", entry.getValue().sum());
        }

        return counters.toMap();
    }

    /**
     * This clears out the current executions and sets new empty current executions
     * This is needed, because when this method is called, watcher keeps running, so sealing executions would be a bad idea
     */
    public void clearExecutions() {
        currentExecutions.sealAndAwaitEmpty(maxStopTimeout);
        currentExecutions = new CurrentExecutions();
    }

    private static final class StartupExecutionContext extends TriggeredExecutionContext {

        public StartupExecutionContext(Watch watch, DateTime executionTime, TriggerEvent triggerEvent, TimeValue defaultThrottlePeriod) {
            super(watch, executionTime, triggerEvent, defaultThrottlePeriod);
        }

        @Override
        public boolean overrideRecordOnConflict() {
            return true;
        }
    }

    private final class WatchExecutionTask implements Runnable {

        private final WatchExecutionContext ctx;

        private WatchExecutionTask(WatchExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            execute(ctx);
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
