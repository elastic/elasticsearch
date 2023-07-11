/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.QueuedWatch;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionSnapshot;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.watch.WatchParser;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class ExecutionService {

    public static final Setting<TimeValue> DEFAULT_THROTTLE_PERIOD_SETTING = Setting.positiveTimeSetting(
        "xpack.watcher.execution.default_throttle_period",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ExecutionService.class);

    private final MeanMetric totalExecutionsTime = new MeanMetric();
    private final Map<String, MeanMetric> actionByTypeExecutionTime = new HashMap<>();

    private final TimeValue defaultThrottlePeriod;
    private final TimeValue maxStopTimeout;
    private final TimeValue indexDefaultTimeout;

    private final HistoryStore historyStore;
    private final TriggeredWatchStore triggeredWatchStore;
    private final Clock clock;
    private final WatchParser parser;
    private final ClusterService clusterService;
    private final Client client;
    private final WatchExecutor executor;
    private final ExecutorService genericExecutor;

    private AtomicReference<CurrentExecutions> currentExecutions = new AtomicReference<>();
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public ExecutionService(
        Settings settings,
        HistoryStore historyStore,
        TriggeredWatchStore triggeredWatchStore,
        WatchExecutor executor,
        Clock clock,
        WatchParser parser,
        ClusterService clusterService,
        Client client,
        ExecutorService genericExecutor
    ) {
        this.historyStore = historyStore;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executor = executor;
        this.clock = clock;
        this.defaultThrottlePeriod = DEFAULT_THROTTLE_PERIOD_SETTING.get(settings);
        this.maxStopTimeout = Watcher.MAX_STOP_TIMEOUT_SETTING.get(settings);
        this.parser = parser;
        this.clusterService = clusterService;
        this.client = client;
        this.genericExecutor = genericExecutor;
        this.indexDefaultTimeout = settings.getAsTime("xpack.watcher.internal.ops.index.default_timeout", TimeValue.timeValueSeconds(30));
        this.currentExecutions.set(new CurrentExecutions());
    }

    public void unPause() {
        paused.set(false);
    }

    /**
     * Pause the execution of the watcher executor, and empty the state.
     * Pausing means, that no new watch executions will be done unless this pausing is explicitly unset.
     * This is important when watcher is stopped, so that scheduled watches do not accidentally get executed.
     * This should not be used when we need to reload watcher based on some cluster state changes, then just calling
     * {@link #clearExecutionsAndQueue(Runnable)} is the way to go
     *
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may be a no-op assuming the
     *                     {@link WatcherState#STOPPED} is set elsewhere or not needed to be set.
     *
     * @return the number of tasks that have been removed
     */
    public int pause(Runnable stoppedListener) {
        assert stoppedListener != null;
        paused.set(true);
        return clearExecutionsAndQueue(stoppedListener);
    }

    /**
     * Empty the currently queued tasks and wait for current executions to finish.
     *
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may be a no-op assuming the
     *                     {@link WatcherState#STOPPED} is set elsewhere or not needed to be set.
     *
     * @return the number of tasks that have been removed
     */
    public int clearExecutionsAndQueue(Runnable stoppedListener) {
        assert stoppedListener != null;
        int cancelledTaskCount = executor.queue().drainTo(new ArrayList<>());
        this.clearExecutions(stoppedListener);
        return cancelledTaskCount;
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
        return currentExecutions.get();
    }

    public List<WatchExecutionSnapshot> currentExecutions() {
        List<WatchExecutionSnapshot> currentExecutions = new ArrayList<>();
        for (WatchExecution watchExecution : this.currentExecutions.get()) {
            currentExecutions.add(watchExecution.createSnapshot());
        }
        // Lets show the longest running watch first:
        currentExecutions.sort(Comparator.comparing(WatchExecutionSnapshot::executionTime));
        return currentExecutions;
    }

    public List<QueuedWatch> queuedWatches() {
        List<Runnable> snapshot = new ArrayList<>();
        executor.tasks().forEach(snapshot::add);
        if (snapshot.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueuedWatch> queuedWatches = new ArrayList<>(snapshot.size());
        for (Runnable task : snapshot) {
            WatchExecutionTask executionTask = (WatchExecutionTask) task;
            queuedWatches.add(new QueuedWatch(executionTask.ctx));
        }

        // Lets show the execution that pending the longest first:
        queuedWatches.sort(Comparator.comparing(QueuedWatch::executionTime));
        return queuedWatches;
    }

    void processEventsAsync(Iterable<TriggerEvent> events) throws Exception {
        if (paused.get()) {
            logger.debug("watcher execution service paused, not processing [{}] events", Iterables.size(events));
            return;
        }
        Tuple<List<TriggeredWatch>, List<TriggeredExecutionContext>> watchesAndContext = createTriggeredWatchesAndContext(events);
        List<TriggeredWatch> triggeredWatches = watchesAndContext.v1();
        triggeredWatchStore.putAll(
            triggeredWatches,
            ActionListener.wrap(response -> executeTriggeredWatches(response, watchesAndContext), e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof EsRejectedExecutionException) {
                    logger.debug("failed to store watch records due to filled up watcher threadpool");
                } else {
                    logger.warn("failed to store watch records", e);
                }
            })
        );
    }

    void processEventsSync(Iterable<TriggerEvent> events) throws IOException {
        if (paused.get()) {
            logger.debug("watcher execution service paused, not processing [{}] events", Iterables.size(events));
            return;
        }
        Tuple<List<TriggeredWatch>, List<TriggeredExecutionContext>> watchesAndContext = createTriggeredWatchesAndContext(events);
        List<TriggeredWatch> triggeredWatches = watchesAndContext.v1();
        logger.debug("saving watch records [{}]", triggeredWatches.size());
        BulkResponse bulkResponse = triggeredWatchStore.putAll(triggeredWatches);
        executeTriggeredWatches(bulkResponse, watchesAndContext);
    }

    /**
     * Create a tuple of triggered watches and their corresponding contexts, usable for sync and async processing
     *
     * @param events The iterable list of trigger events to create the two lists from
     * @return       Two linked lists that contain the triggered watches and contexts
     */
    private Tuple<List<TriggeredWatch>, List<TriggeredExecutionContext>> createTriggeredWatchesAndContext(Iterable<TriggerEvent> events) {
        final LinkedList<TriggeredWatch> triggeredWatches = new LinkedList<>();
        final LinkedList<TriggeredExecutionContext> contexts = new LinkedList<>();

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        for (TriggerEvent event : events) {
            GetResponse response = getWatch(event.jobName());
            if (response.isExists() == false) {
                logger.warn("unable to find watch [{}] in watch index, perhaps it has been deleted", event.jobName());
                continue;
            }
            TriggeredExecutionContext ctx = new TriggeredExecutionContext(event.jobName(), now, event, defaultThrottlePeriod);
            contexts.add(ctx);
            triggeredWatches.add(new TriggeredWatch(ctx.id(), event));
        }

        return Tuple.tuple(triggeredWatches, contexts);
    }

    /**
     * Execute triggered watches, which have been successfully indexed into the triggered watches index
     *
     * @param response            The bulk response containing the response of indexing triggered watches
     * @param watchesAndContext   The triggered watches and context objects needed for execution
     */
    private void executeTriggeredWatches(
        final BulkResponse response,
        final Tuple<List<TriggeredWatch>, List<TriggeredExecutionContext>> watchesAndContext
    ) {
        for (int i = 0; i < response.getItems().length; i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (itemResponse.isFailed()) {
                logger.error(
                    () -> "could not store triggered watch with id [" + itemResponse.getId() + "]",
                    itemResponse.getFailure().getCause()
                );
            } else {
                executeAsync(watchesAndContext.v2().get(i), watchesAndContext.v1().get(i));
            }
        }
    }

    public WatchRecord execute(WatchExecutionContext ctx) {
        ctx.setNodeId(clusterService.localNode().getId());
        WatchRecord record = null;
        final String watchId = ctx.id().watchId();
        // pull this to a local reference since the class reference can be swapped, and need to ensure same object is used for put/remove
        final CurrentExecutions currentExecutions = this.currentExecutions.get();
        try {
            boolean executionAlreadyExists = currentExecutions.put(watchId, new WatchExecution(ctx, Thread.currentThread()));
            if (executionAlreadyExists) {
                logger.trace("not executing watch [{}] because it is already queued", watchId);
                record = ctx.abortBeforeExecution(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED, "Watch is already queued in thread pool");
            } else {
                try {
                    ctx.ensureWatchExists(() -> {
                        GetResponse resp = getWatch(watchId);
                        if (resp.isExists() == false) {
                            throw new ResourceNotFoundException("watch [{}] does not exist", watchId);
                        }
                        return parser.parseWithSecrets(
                            watchId,
                            true,
                            resp.getSourceAsBytesRef(),
                            ctx.executionTime(),
                            XContentType.JSON,
                            resp.getSeqNo(),
                            resp.getPrimaryTerm()
                        );
                    });
                } catch (ResourceNotFoundException e) {
                    String message = "unable to find watch for record [" + ctx.id() + "]";
                    record = ctx.abortBeforeExecution(ExecutionState.NOT_EXECUTED_WATCH_MISSING, message);
                } catch (Exception e) {
                    record = ctx.abortFailedExecution(e);
                }

                if (ctx.watch() != null) {
                    if (ctx.shouldBeExecuted()) {
                        logger.debug("executing watch [{}]", watchId);

                        record = executeInner(ctx);
                        if (ctx.recordExecution()) {
                            updateWatchStatus(ctx.watch());
                        }
                    } else {
                        logger.debug("not executing watch [{}]", watchId);
                        record = ctx.abortBeforeExecution(ExecutionState.EXECUTION_NOT_NEEDED, "Watch is not active");
                    }
                }
            }
        } catch (Exception e) {
            record = createWatchRecord(record, ctx, e);
            logWatchRecord(ctx, e);
        } finally {
            if (ctx.knownWatch()) {
                if (record != null && ctx.recordExecution()) {
                    try {
                        if (ctx.overrideRecordOnConflict()) {
                            historyStore.forcePut(record);
                        } else {
                            historyStore.put(record);
                        }
                    } catch (Exception e) {
                        logger.error(() -> "failed to update watch record [" + ctx.id() + "]", e);
                        // TODO log watch record in logger, when saving in history store failed, otherwise the info is gone!
                    }
                }

                triggeredWatchStore.delete(ctx.id());
            }
            currentExecutions.remove(watchId);
            logger.debug("finished [{}]/[{}]", watchId, ctx.id());
        }
        return record;
    }

    /**
     * Updates and persists the status of the given watch
     *
     * If the watch is missing (because it might have been deleted by the user during an execution), then this method
     * does nothing and just returns without throwing an exception
     */
    public void updateWatchStatus(Watch watch) throws IOException {
        // at the moment we store the status together with the watch,
        // so we just need to update the watch itself
        // we do not want to update the status.state field, as it might have been deactivated in-between
        Map<String, String> parameters = Map.of(Watch.INCLUDE_STATUS_KEY, "true", WatchStatus.INCLUDE_STATE, "false");
        ToXContent.MapParams params = new ToXContent.MapParams(parameters);
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .field(WatchField.STATUS.getPreferredName(), watch.status(), params)
            .endObject();

        UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, watch.id());
        updateRequest.doc(source);
        updateRequest.setIfSeqNo(watch.getSourceSeqNo());
        updateRequest.setIfPrimaryTerm(watch.getSourcePrimaryTerm());
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)) {
            client.update(updateRequest).actionGet(indexDefaultTimeout);
        } catch (DocumentMissingException e) {
            // do not rethrow this exception, otherwise the watch history will contain an exception
            // even though the execution might have been fine
            // TODO should we really just drop this exception on the floor?
        }
    }

    private static WatchRecord createWatchRecord(WatchRecord existingRecord, WatchExecutionContext ctx, Exception e) {
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

    private static void logWatchRecord(WatchExecutionContext ctx, Exception e) {
        // failed watches stack traces are only logged in debug, otherwise they should be checked out in the history
        if (logger.isDebugEnabled()) {
            logger.debug(() -> "failed to execute watch [" + ctx.id().watchId() + "]", e);
        } else {
            logger.warn("failed to execute watch [{}]", ctx.id().watchId());
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
            executor.execute(new WatchExecutionTask(ctx, () -> execute(ctx)));
        } catch (EsRejectedExecutionException e) {
            // Using the generic pool here since this can happen from a write thread and we don't want to block a write
            // thread to kick off these additional write/delete requests.
            // Intentionally not using the HistoryStore or TriggerWatchStore to avoid re-using the same synchronous
            // BulkProcessor which can cause a deadlock see #41390
            genericExecutor.execute(new WatchExecutionTask(ctx, () -> {
                String message = "failed to run triggered watch [" + triggeredWatch.id() + "] due to thread pool capacity";
                logger.warn(message);
                WatchRecord record = ctx.abortBeforeExecution(ExecutionState.THREADPOOL_REJECTION, message);
                try {
                    forcePutHistory(record);
                } catch (Exception exc) {
                    logger.error(
                        () -> format("Error storing watch history record for watch [%s] after thread pool rejection", triggeredWatch.id()),
                        exc
                    );
                }
                try {
                    deleteTrigger(triggeredWatch.id());
                } catch (Exception exc) {
                    logger.error(
                        () -> format(
                            "Error deleting entry from .triggered_watches for watch [%s] after thread pool rejection",
                            triggeredWatch.id()
                        ),
                        exc
                    );
                }
            }));
        }
    }

    /**
     * Stores the specified watchRecord.
     * Any existing watchRecord will be overwritten.
     */
    private void forcePutHistory(WatchRecord watchRecord) {
        try {
            try (
                XContentBuilder builder = XContentFactory.jsonBuilder();
                ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)
            ) {
                watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);
                IndexRequest request = new IndexRequest(HistoryStoreField.DATA_STREAM).id(watchRecord.id().value())
                    .source(builder)
                    .opType(IndexRequest.OpType.CREATE);
                client.index(request).get(30, TimeUnit.SECONDS);
                logger.debug("indexed watch history record [{}]", watchRecord.id().value());
            } catch (VersionConflictEngineException vcee) {
                watchRecord = new WatchRecord.MessageWatchRecord(
                    watchRecord,
                    ExecutionState.EXECUTED_MULTIPLE_TIMES,
                    "watch record [{ " + watchRecord.id() + " }] has been stored before, previous state [" + watchRecord.state() + "]"
                );
                try (
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
                    ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)
                ) {
                    IndexRequest request = new IndexRequest(HistoryStoreField.DATA_STREAM).id(watchRecord.id().value())
                        .source(xContentBuilder.value(watchRecord));
                    client.index(request).get(30, TimeUnit.SECONDS);
                }
                logger.debug("overwrote watch history record [{}]", watchRecord.id().value());
            }
        } catch (InterruptedException | ExecutionException | TimeoutException | IOException ioe) {
            final WatchRecord wr = watchRecord;
            logger.error(() -> "failed to persist watch record [" + wr + "]", ioe);
        }
    }

    private void deleteTrigger(Wid watcherId) {
        DeleteRequest request = new DeleteRequest(TriggeredWatchStoreField.INDEX_NAME);
        request.id(watcherId.value());
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)) {
            client.delete(request).actionGet(30, TimeUnit.SECONDS);
        }
        logger.trace("successfully deleted triggered watch with id [{}]", watcherId);
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
                long start = System.nanoTime();
                ActionWrapperResult actionResult = action.execute(ctx);
                long executionTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
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

    public void executeTriggeredWatches(Collection<TriggeredWatch> triggeredWatches) {
        assert triggeredWatches != null;
        int counter = 0;
        for (TriggeredWatch triggeredWatch : triggeredWatches) {
            GetResponse response = getWatch(triggeredWatch.id().watchId());
            if (response.isExists() == false) {
                String message = "unable to find watch for record ["
                    + triggeredWatch.id().watchId()
                    + "]/["
                    + triggeredWatch.id()
                    + "], perhaps it has been deleted, ignoring...";
                WatchRecord record = new WatchRecord.MessageWatchRecord(
                    triggeredWatch.id(),
                    triggeredWatch.triggerEvent(),
                    ExecutionState.NOT_EXECUTED_WATCH_MISSING,
                    message,
                    clusterService.localNode().getId()
                );
                historyStore.forcePut(record);
                triggeredWatchStore.delete(triggeredWatch.id());
            } else {
                ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
                TriggeredExecutionContext ctx = new TriggeredExecutionContext(
                    triggeredWatch.id().watchId(),
                    now,
                    triggeredWatch.triggerEvent(),
                    defaultThrottlePeriod,
                    true
                );
                executeAsync(ctx, triggeredWatch);
                counter++;
            }
        }
        logger.debug("triggered execution of [{}] watches", counter);
    }

    /**
     * Gets a watch but in a synchronous way, so that no async calls need to be built
     * @param id The id of watch
     * @return The GetResponse of calling the get API of this watch
     */
    private GetResponse getWatch(String id) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)) {
            GetRequest getRequest = new GetRequest(Watch.INDEX, id).preference(Preference.LOCAL.type()).realtime(true);
            PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
            client.get(getRequest, future);
            return future.actionGet();
        }
    }

    public Counters executionTimes() {
        Counters counters = new Counters();
        counters.inc("execution.actions._all.total", totalExecutionsTime.count());
        counters.inc("execution.actions._all.total_time_in_ms", totalExecutionsTime.sum());

        for (Map.Entry<String, MeanMetric> entry : actionByTypeExecutionTime.entrySet()) {
            counters.inc("execution.actions." + entry.getKey() + ".total", entry.getValue().count());
            counters.inc("execution.actions." + entry.getKey() + ".total_time_in_ms", entry.getValue().sum());
        }

        return counters;
    }

    /**
     * This clears out the current executions and sets new empty current executions
     * This is needed, because when this method is called, watcher keeps running, so sealing executions would be a bad idea
     *
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may be a no-op assuming the
     *                     {@link WatcherState#STOPPED} is set elsewhere or not needed to be set.
     */
    private void clearExecutions(Runnable stoppedListener) {
        assert stoppedListener != null;
        final CurrentExecutions currentExecutionsBeforeSetting = currentExecutions.getAndSet(new CurrentExecutions());
        // clear old executions in background, no need to wait
        genericExecutor.execute(() -> currentExecutionsBeforeSetting.sealAndAwaitEmpty(maxStopTimeout, stoppedListener));
    }

    // the watch execution task takes another runnable as parameter
    // the best solution would be to move the whole execute() method, which is handed over as ctor parameter
    // over into this class, this is the quicker way though
    public static final class WatchExecutionTask implements Runnable {

        private final WatchExecutionContext ctx;
        private final Runnable runnable;

        public WatchExecutionTask(WatchExecutionContext ctx, Runnable runnable) {
            this.ctx = ctx;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    static class WatchExecution {

        private final WatchExecutionContext context;
        private final Thread executionThread;

        WatchExecution(WatchExecutionContext context, Thread executionThread) {
            this.context = context;
            this.executionThread = executionThread;
        }

        WatchExecutionSnapshot createSnapshot() {
            return context.createSnapshot(executionThread);
        }
    }
}
