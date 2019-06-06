/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.AggregationResultUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class DataFrameTransformTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    // interval the scheduler sends an event
    private static final int SCHEDULER_NEXT_MILLISECONDS = 10000;
    private static final Logger logger = LogManager.getLogger(DataFrameTransformTask.class);
    // TODO consider moving to dynamic cluster setting
    private static final int MAX_CONTINUOUS_FAILURES = 10;
    private static final IndexerState[] RUNNING_STATES = new IndexerState[]{IndexerState.STARTED, IndexerState.INDEXING};
    public static final String SCHEDULE_NAME = DataFrameField.TASK_NAME + "/schedule";

    private final DataFrameTransform transform;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameAuditor auditor;
    private final Map<String, Object> initialPosition;
    private final IndexerState initialIndexerState;

    private final SetOnce<ClientDataFrameIndexer> indexer = new SetOnce<>();

    private final AtomicReference<DataFrameTransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    // the checkpoint of this data frame, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;

    public DataFrameTransformTask(long id, String type, String action, TaskId parentTask, DataFrameTransform transform,
                                  DataFrameTransformState state, SchedulerEngine schedulerEngine, DataFrameAuditor auditor,
                                  ThreadPool threadPool, Map<String, String> headers) {
        super(id, type, action, DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + transform.getId(), parentTask, headers);
        this.transform = transform;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
        this.auditor = auditor;
        IndexerState initialState = IndexerState.STOPPED;
        DataFrameTransformTaskState initialTaskState = DataFrameTransformTaskState.STOPPED;
        String initialReason = null;
        long initialGeneration = 0;
        Map<String, Object> initialPosition = null;
        logger.trace("[{}] init, got state: [{}]", transform.getId(), state != null);
        if (state != null) {
            initialTaskState = state.getTaskState();
            initialReason = state.getReason();
            final IndexerState existingState = state.getIndexerState();
            logger.info("[{}] Loading existing state: [{}], position [{}]", transform.getId(), existingState, state.getPosition());
            if (existingState.equals(IndexerState.INDEXING)) {
                // reset to started as no indexer is running
                initialState = IndexerState.STARTED;
            } else if (existingState.equals(IndexerState.ABORTING) || existingState.equals(IndexerState.STOPPING)) {
                // reset to stopped as something bad happened
                initialState = IndexerState.STOPPED;
            } else {
                initialState = existingState;
            }
            initialPosition = state.getPosition();
            initialGeneration = state.getCheckpoint();
        }

        this.initialIndexerState = initialState;
        this.initialPosition = initialPosition;
        this.currentCheckpoint = new AtomicLong(initialGeneration);
        this.taskState = new AtomicReference<>(initialTaskState);
        this.stateReason = new AtomicReference<>(initialReason);
    }

    public String getTransformId() {
        return transform.getId();
    }

    /**
     * Enable Task API to return detailed status information
     */
    @Override
    public Status getStatus() {
        return getState();
    }

    private ClientDataFrameIndexer getIndexer() {
        return indexer.get();
    }

    public DataFrameTransformState getState() {
        if (getIndexer() == null) {
            return new DataFrameTransformState(
                taskState.get(),
                initialIndexerState,
                initialPosition,
                currentCheckpoint.get(),
                stateReason.get(),
                null);
        } else {
           return new DataFrameTransformState(
               taskState.get(),
               indexer.get().getState(),
               indexer.get().getPosition(),
               currentCheckpoint.get(),
               stateReason.get(),
               getIndexer().getProgress());
        }
    }

    public DataFrameIndexerTransformStats getStats() {
        if (getIndexer() == null) {
            return new DataFrameIndexerTransformStats(getTransformId());
        } else {
            return getIndexer().getStats();
        }
    }

    public long getCheckpoint() {
        return currentCheckpoint.get();
    }

    /**
     * Get the in-progress checkpoint
     *
     * @return checkpoint in progress or 0 if task/indexer is not active
     */
    public long getInProgressCheckpoint() {
        if (getIndexer() == null) {
            return 0;
        } else {
            return indexer.get().getState().equals(IndexerState.INDEXING) ? currentCheckpoint.get() + 1L : 0;
        }
    }

    public void setTaskStateStopped() {
        taskState.set(DataFrameTransformTaskState.STOPPED);
    }

    /**
     * Start the background indexer and set the task's state to started
     * @param startingCheckpoint Set the current checkpoint to this value. If null the
     *                           current checkpoint is not set
     * @param listener Started listener
     */
    public synchronized void start(Long startingCheckpoint, ActionListener<Response> listener) {
        if (getIndexer() == null) {
            listener.onFailure(new ElasticsearchException("Task for transform [{}] not fully initialized. Try again later",
                getTransformId()));
            return;
        }
        final IndexerState newState = getIndexer().start();
        if (Arrays.stream(RUNNING_STATES).noneMatch(newState::equals)) {
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame transform [{}], because state was [{}]",
                    transform.getId(), newState));
            return;
        }
        stateReason.set(null);
        taskState.set(DataFrameTransformTaskState.STARTED);
        if (startingCheckpoint != null) {
            currentCheckpoint.set(startingCheckpoint);
        }

        final DataFrameTransformState state = new DataFrameTransformState(
            DataFrameTransformTaskState.STARTED,
            IndexerState.STOPPED,
            getIndexer().getPosition(),
            currentCheckpoint.get(),
            null,
            getIndexer().getProgress());

        logger.info("Updating state for data frame transform [{}] to [{}]", transform.getId(), state.toString());
        persistStateToClusterState(state, ActionListener.wrap(
            task -> {
                auditor.info(transform.getId(),
                    "Updated data frame transform state to [" + state.getTaskState() + "].");
                long now = System.currentTimeMillis();
                // kick off the indexer
                triggered(new Event(schedulerJobName(), now, now));
                registerWithSchedulerJob();
                listener.onResponse(new StartDataFrameTransformTaskAction.Response(true));
            },
            exc -> {
                getIndexer().stop();
                listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform ["
                                    + transform.getId() + "] to [" + state.getIndexerState() + "].", exc));
            }
        ));
    }

    public synchronized void stop() {
        if (getIndexer() == null) {
            // If there is no indexer the task has not been triggered
            // but it still needs to be stopped and removed
            shutdown();
            return;
        }

        if (getIndexer().getState() == IndexerState.STOPPED) {
            return;
        }

        IndexerState state = getIndexer().stop();
        if (state == IndexerState.STOPPED) {
            getIndexer().doSaveState(state, getIndexer().getPosition(), () -> getIndexer().onStop());
        }
    }

    @Override
    public synchronized void triggered(Event event) {
        if (getIndexer() == null) {
            logger.warn("Data frame task [{}] triggered with an unintialized indexer", getTransformId());
            return;
        }
        //  for now no rerun, so only trigger if checkpoint == 0
        if (currentCheckpoint.get() == 0 && event.getJobName().equals(schedulerJobName())) {
            logger.debug("Data frame indexer [{}] schedule has triggered, state: [{}]", event.getJobName(), getIndexer().getState());
            getIndexer().maybeTriggerAsyncJob(System.currentTimeMillis());
        }
    }

    /**
     * Attempt to gracefully cleanup the data frame transform so it can be terminated.
     * This tries to remove the job from the scheduler and completes the persistent task
     */
    synchronized void shutdown() {
        deregisterSchedulerJob();
        markAsCompleted();
    }

    public DataFrameTransformProgress getProgress() {
        if (indexer.get() == null) {
            return null;
        }
        DataFrameTransformProgress indexerProgress = indexer.get().getProgress();
        if (indexerProgress == null) {
            return null;
        }
        return new DataFrameTransformProgress(indexerProgress);
    }

    void persistStateToClusterState(DataFrameTransformState state,
                                    ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>> listener) {
        updatePersistentTaskState(state, ActionListener.wrap(
            success -> {
                logger.debug("Successfully updated state for data frame transform [{}] to [{}]", transform.getId(), state.toString());
                listener.onResponse(success);
            },
            failure -> {
                auditor.warning(transform.getId(), "Failed to persist to state to cluster state: " + failure.getMessage());
                logger.error("Failed to update state for data frame transform [" + transform.getId() + "]", failure);
                listener.onFailure(failure);
            }
        ));
    }

    synchronized void markAsFailed(String reason, ActionListener<Void> listener) {
        taskState.set(DataFrameTransformTaskState.FAILED);
        stateReason.set(reason);
        auditor.error(transform.getId(), reason);
        persistStateToClusterState(getState(), ActionListener.wrap(
            r -> listener.onResponse(null),
            listener::onFailure
        ));
    }

    /**
     * This is called when the persistent task signals that the allocated task should be terminated.
     * Termination in the task framework is essentially voluntary, as the allocated task can only be
     * shut down from the inside.
     */
    @Override
    public synchronized void onCancelled() {
        logger.info(
                "Received cancellation request for data frame transform [" + transform.getId() + "], state: [" + taskState.get() + "]");
        if (getIndexer() != null && getIndexer().abort()) {
            // there is no background transform running, we can shutdown safely
            shutdown();
        }
    }

    private void registerWithSchedulerJob() {
        schedulerEngine.register(this);
        final SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(schedulerJobName(), next());
        schedulerEngine.add(schedulerJob);
    }

    private void deregisterSchedulerJob() {
        schedulerEngine.remove(schedulerJobName());
        schedulerEngine.unregister(this);
    }

    private String schedulerJobName() {
        return DataFrameTransformTask.SCHEDULE_NAME + "_" + getTransformId();
    }

    private SchedulerEngine.Schedule next() {
        return (startTime, now) -> {
            return now + SCHEDULER_NEXT_MILLISECONDS;
        };
    }

    synchronized void initializeIndexer(ClientDataFrameIndexerBuilder indexerBuilder) {
        indexer.set(indexerBuilder.build(this));
    }

    static class ClientDataFrameIndexerBuilder {
        private Client client;
        private DataFrameTransformsConfigManager transformsConfigManager;
        private DataFrameTransformsCheckpointService transformsCheckpointService;
        private String transformId;
        private DataFrameAuditor auditor;
        private Map<String, String> fieldMappings;
        private DataFrameTransformConfig transformConfig;
        private DataFrameIndexerTransformStats initialStats;
        private IndexerState indexerState = IndexerState.STOPPED;
        private Map<String, Object> initialPosition;
        private DataFrameTransformProgress progress;

        ClientDataFrameIndexerBuilder(String transformId) {
            this.transformId = transformId;
            this.initialStats = new DataFrameIndexerTransformStats(transformId);
        }

        ClientDataFrameIndexer build(DataFrameTransformTask parentTask) {
            return new ClientDataFrameIndexer(this.transformId,
                this.transformsConfigManager,
                this.transformsCheckpointService,
                new AtomicReference<>(this.indexerState),
                this.initialPosition,
                this.client,
                this.auditor,
                this.initialStats,
                this.transformConfig,
                this.fieldMappings,
                this.progress,
                parentTask);
        }

        ClientDataFrameIndexerBuilder setClient(Client client) {
            this.client = client;
            return this;
        }

        ClientDataFrameIndexerBuilder setTransformsConfigManager(DataFrameTransformsConfigManager transformsConfigManager) {
            this.transformsConfigManager = transformsConfigManager;
            return this;
        }

        ClientDataFrameIndexerBuilder setTransformsCheckpointService(DataFrameTransformsCheckpointService transformsCheckpointService) {
            this.transformsCheckpointService = transformsCheckpointService;
            return this;
        }

        ClientDataFrameIndexerBuilder setTransformId(String transformId) {
            this.transformId = transformId;
            return this;
        }

        ClientDataFrameIndexerBuilder setAuditor(DataFrameAuditor auditor) {
            this.auditor = auditor;
            return this;
        }

        ClientDataFrameIndexerBuilder setFieldMappings(Map<String, String> fieldMappings) {
            this.fieldMappings = fieldMappings;
            return this;
        }

        ClientDataFrameIndexerBuilder setTransformConfig(DataFrameTransformConfig transformConfig) {
            this.transformConfig = transformConfig;
            return this;
        }

        DataFrameTransformConfig getTransformConfig() {
            return this.transformConfig;
        }

        ClientDataFrameIndexerBuilder setInitialStats(DataFrameIndexerTransformStats initialStats) {
            this.initialStats = initialStats;
            return this;
        }

        ClientDataFrameIndexerBuilder setIndexerState(IndexerState indexerState) {
            this.indexerState = indexerState;
            return this;
        }

        ClientDataFrameIndexerBuilder setInitialPosition(Map<String, Object> initialPosition) {
            this.initialPosition = initialPosition;
            return this;
        }

        ClientDataFrameIndexerBuilder setProgress(DataFrameTransformProgress progress) {
            this.progress = progress;
            return this;
        }
    }

    static class ClientDataFrameIndexer extends DataFrameIndexer {
        private final Client client;
        private final DataFrameTransformsConfigManager transformsConfigManager;
        private final DataFrameTransformsCheckpointService transformsCheckpointService;
        private final String transformId;
        private final DataFrameTransformTask transformTask;
        private final AtomicInteger failureCount;
        // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
        private volatile String lastAuditedExceptionMessage = null;

        ClientDataFrameIndexer(String transformId,
                               DataFrameTransformsConfigManager transformsConfigManager,
                               DataFrameTransformsCheckpointService transformsCheckpointService,
                               AtomicReference<IndexerState> initialState,
                               Map<String, Object> initialPosition,
                               Client client,
                               DataFrameAuditor auditor,
                               DataFrameIndexerTransformStats initialStats,
                               DataFrameTransformConfig transformConfig,
                               Map<String, String> fieldMappings,
                               DataFrameTransformProgress transformProgress,
                               DataFrameTransformTask parentTask) {
            super(ExceptionsHelper.requireNonNull(parentTask, "parentTask")
                    .threadPool
                    .executor(ThreadPool.Names.GENERIC),
                ExceptionsHelper.requireNonNull(auditor, "auditor"),
                transformConfig,
                fieldMappings,
                ExceptionsHelper.requireNonNull(initialState, "initialState"),
                initialPosition,
                initialStats == null ? new DataFrameIndexerTransformStats(transformId) : initialStats,
                transformProgress);
            this.transformId = ExceptionsHelper.requireNonNull(transformId, "transformId");
            this.transformsConfigManager = ExceptionsHelper.requireNonNull(transformsConfigManager, "transformsConfigManager");
            this.transformsCheckpointService = ExceptionsHelper.requireNonNull(transformsCheckpointService,
                "transformsCheckpointService");
            this.client = ExceptionsHelper.requireNonNull(client, "client");
            this.transformTask = parentTask;
            this.failureCount = new AtomicInteger(0);
        }

        @Override
        protected void onStart(long now, ActionListener<Void> listener) {
            // Reset our failure count as we are starting again
            failureCount.set(0);
            // On each run, we need to get the total number of docs and reset the count of processed docs
            // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
            // the progress here, and not in the executor.
            if (initialRun()) {
                TransformProgressGatherer.getInitialProgress(this.client, getConfig(), ActionListener.wrap(
                    newProgress -> {
                        progress = newProgress;
                        super.onStart(now, listener);
                    },
                    failure -> {
                        progress = null;
                        logger.warn("Unable to load progress information for task [" + transformId + "]", failure);
                        super.onStart(now, listener);
                    }
                ));
            } else {
                super.onStart(now, listener);
            }
        }

        @Override
        protected String getJobId() {
            return transformId;
        }

        @Override
        public synchronized boolean maybeTriggerAsyncJob(long now) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("Schedule was triggered for transform [{}] but task is failed. Ignoring trigger.", getJobId());
                return false;
            }

            // ignore trigger if indexer is running, prevents log spam in A2P indexer
            IndexerState indexerState = getState();
            if (IndexerState.INDEXING.equals(indexerState) || IndexerState.STOPPING.equals(indexerState)) {
                logger.debug("Indexer for transform [{}] has state [{}], ignoring trigger", getJobId(), indexerState);
                return false;
            }

            return super.maybeTriggerAsyncJob(now);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client,
                    SearchAction.INSTANCE, request, nextPhase);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, BulkAction.INSTANCE,
                    request, nextPhase);
        }

        @Override
        protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
            if (indexerState.equals(IndexerState.ABORTING)) {
                // If we're aborting, just invoke `next` (which is likely an onFailure handler)
                next.run();
                return;
            }
            // If we are `STOPPED` on a `doSaveState` call, that indicates we transitioned to `STOPPED` from `STOPPING`
            // OR we called `doSaveState` manually as the indexer was not actively running.
            // Since we save the state to an index, we should make sure that our task state is in parity with the indexer state
            if (indexerState.equals(IndexerState.STOPPED)) {
                transformTask.setTaskStateStopped();
            }

            final DataFrameTransformState state = new DataFrameTransformState(
                transformTask.taskState.get(),
                indexerState,
                position,
                transformTask.currentCheckpoint.get(),
                transformTask.stateReason.get(),
                getProgress());
            logger.debug("Updating persistent state of transform [{}] to [{}]", transformConfig.getId(), state.toString());

            // Persisting stats when we call `doSaveState` should be ok as we only call it on a state transition and
            // only every-so-often when doing the bulk indexing calls.  See AsyncTwoPhaseIndexer#onBulkResponse for current periodicity
            transformsConfigManager.putOrUpdateTransformStats(
                    new DataFrameTransformStateAndStats(transformId, state, getStats(),
                            DataFrameTransformCheckpointingInfo.EMPTY), // TODO should this be null
                    ActionListener.wrap(
                            r -> {
                                next.run();
                            },
                            statsExc -> {
                                logger.error("Updating stats of transform [" + transformConfig.getId() + "] failed", statsExc);
                                auditor.warning(getJobId(),
                                    "Failure updating stats of transform: " + statsExc.getMessage());
                                next.run();
                            }
                    ));
        }

        @Override
        protected void onFailure(Exception exc) {
            // the failure handler must not throw an exception due to internal problems
            try {
                logger.warn("Data frame transform [" + transformTask.getTransformId() + "] encountered an exception: ", exc);

                // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
                // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
                if (exc.getMessage().equals(lastAuditedExceptionMessage) == false) {
                    auditor.warning(transformTask.getTransformId(), "Data frame transform encountered an exception: " + exc.getMessage());
                    lastAuditedExceptionMessage = exc.getMessage();
                }
                handleFailure(exc);
            } catch (Exception e) {
                logger.error("Data frame transform encountered an unexpected internal exception: " ,e);
            }
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            try {
                super.onFinish(listener);
                long checkpoint = transformTask.currentCheckpoint.incrementAndGet();
                auditor.info(transformTask.getTransformId(), "Finished indexing for data frame transform checkpoint [" + checkpoint + "].");
                logger.info(
                    "Finished indexing for data frame transform [" + transformTask.getTransformId() + "] checkpoint [" + checkpoint + "]");
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected void onStop() {
            auditor.info(transformConfig.getId(), "Data frame transform has stopped.");
            logger.info("Data frame transform [{}] indexer has stopped", transformConfig.getId());
            transformTask.shutdown();
        }

        @Override
        protected void onAbort() {
            auditor.info(transformConfig.getId(), "Received abort request, stopping data frame transform.");
            logger.info("Data frame transform [" + transformConfig.getId() + "] received abort request, stopping indexer");
            transformTask.shutdown();
        }

        @Override
        protected void createCheckpoint(ActionListener<Void> listener) {
            transformsCheckpointService.getCheckpoint(transformConfig,
                transformTask.currentCheckpoint.get() + 1,
                ActionListener.wrap(
                    checkpoint -> transformsConfigManager.putTransformCheckpoint(checkpoint,
                        ActionListener.wrap(
                            putCheckPointResponse -> listener.onResponse(null),
                            createCheckpointException ->
                                listener.onFailure(new RuntimeException("Failed to create checkpoint", createCheckpointException))
                    )),
                    getCheckPointException ->
                        listener.onFailure(new RuntimeException("Failed to retrieve checkpoint", getCheckPointException))
            ));
        }

        private boolean isIrrecoverableFailure(Exception e) {
            return e instanceof IndexNotFoundException || e instanceof AggregationResultUtils.AggregationExtractionException;
        }

        synchronized void handleFailure(Exception e) {
            if (handleCircuitBreakingException(e)) {
                return;
            }

            if (isIrrecoverableFailure(e) || failureCount.incrementAndGet() > MAX_CONTINUOUS_FAILURES) {
                String failureMessage = isIrrecoverableFailure(e) ?
                    "task encountered irrecoverable failure: " + e.getMessage() :
                    "task encountered more than " + MAX_CONTINUOUS_FAILURES + " failures; latest failure: " + e.getMessage();
                failIndexer(failureMessage);
            }
        }

        @Override
        protected void failIndexer(String failureMessage) {
            logger.error("Data frame transform [" + getJobId() + "]:" + failureMessage);
            auditor.error(transformTask.getTransformId(), failureMessage);
            transformTask.markAsFailed(failureMessage, ActionListener.wrap(
                r -> {
                    // Successfully marked as failed, reset counter so that task can be restarted
                    failureCount.set(0);
                }, e -> {}));
        }
    }
}
