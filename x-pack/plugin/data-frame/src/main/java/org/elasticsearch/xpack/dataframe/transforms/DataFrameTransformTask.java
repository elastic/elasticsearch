/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDoc;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.AggregationResultUtils;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.dataframe.DataFrameMessages.DATA_FRAME_CANNOT_START_FAILED_TRANSFORM;
import static org.elasticsearch.xpack.core.dataframe.DataFrameMessages.DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM;


public class DataFrameTransformTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    // Default interval the scheduler sends an event if the config does not specify a frequency
    private static final long SCHEDULER_NEXT_MILLISECONDS = 60000;
    private static final Logger logger = LogManager.getLogger(DataFrameTransformTask.class);
    private static final int DEFAULT_FAILURE_RETRIES = 10;
    private volatile int numFailureRetries = DEFAULT_FAILURE_RETRIES;
    // How many times the transform task can retry on an non-critical failure
    public static final Setting<Integer> NUM_FAILURE_RETRIES_SETTING = Setting.intSetting(
        "xpack.data_frame.num_transform_failure_retries",
        DEFAULT_FAILURE_RETRIES,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic);
    private static final IndexerState[] RUNNING_STATES = new IndexerState[]{IndexerState.STARTED, IndexerState.INDEXING};
    public static final String SCHEDULE_NAME = DataFrameField.TASK_NAME + "/schedule";

    private final DataFrameTransform transform;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameAuditor auditor;
    private final DataFrameIndexerPosition initialPosition;
    private final IndexerState initialIndexerState;
    private volatile Instant changesLastDetectedAt;

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
        DataFrameIndexerPosition initialPosition = null;
        if (state != null) {
            initialTaskState = state.getTaskState();
            initialReason = state.getReason();
            final IndexerState existingState = state.getIndexerState();
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
            return new DataFrameIndexerTransformStats();
        } else {
            return getIndexer().getStats();
        }
    }

    public long getCheckpoint() {
        return currentCheckpoint.get();
    }

    public void getCheckpointingInfo(DataFrameTransformsCheckpointService transformsCheckpointService,
            ActionListener<DataFrameTransformCheckpointingInfo> listener) {
        ClientDataFrameIndexer indexer = getIndexer();
        if (indexer == null) {
            transformsCheckpointService.getCheckpointingInfo(
                    transform.getId(),
                    currentCheckpoint.get(),
                    initialPosition,
                    null,
                    listener);
            return;
        }
        indexer.getCheckpointProvider().getCheckpointingInfo(
                indexer.getLastCheckpoint(),
                indexer.getNextCheckpoint(),
                indexer.getPosition(),
                indexer.getProgress(),
                ActionListener.wrap(
                    info -> {
                        if (changesLastDetectedAt == null) {
                            listener.onResponse(info);
                        } else {
                            listener.onResponse(info.setChangesLastDetectedAt(changesLastDetectedAt));
                        }
                    },
                    listener::onFailure
                ));
    }

    public DataFrameTransformCheckpoint getLastCheckpoint() {
        return getIndexer().getLastCheckpoint();
    }

    public DataFrameTransformCheckpoint getNextCheckpoint() {
        return getIndexer().getNextCheckpoint();
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

    public synchronized void setTaskStateStopped() {
        taskState.set(DataFrameTransformTaskState.STOPPED);
    }

    /**
     * Start the background indexer and set the task's state to started
     * @param startingCheckpoint Set the current checkpoint to this value. If null the
     *                           current checkpoint is not set
     * @param listener Started listener
     */
    public synchronized void start(Long startingCheckpoint, boolean force, ActionListener<Response> listener) {
        logger.debug("[{}] start called with force [{}] and state [{}].", getTransformId(), force, getState());
        if (taskState.get() == DataFrameTransformTaskState.FAILED && force == false) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DATA_FRAME_CANNOT_START_FAILED_TRANSFORM,
                    getTransformId(),
                    stateReason.get()),
                RestStatus.CONFLICT));
            return;
        }
        if (getIndexer() == null) {
            // If our state is failed AND the indexer is null, the user needs to _stop?force=true so that the indexer gets
            // fully initialized.
            // If we are NOT failed, then we can assume that `start` was just called early in the process.
            String msg = taskState.get() == DataFrameTransformTaskState.FAILED ?
                "It failed during the initialization process; force stop to allow reinitialization." :
                "Try again later.";
            listener.onFailure(new ElasticsearchStatusException("Task for transform [{}] not fully initialized. {}",
                RestStatus.CONFLICT,
                getTransformId(),
                msg));
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

        logger.info("[{}] updating state for data frame transform to [{}].", transform.getId(), state.toString());
        // Even though the indexer information is persisted to an index, we still need DataFrameTransformTaskState in the clusterstate
        // This keeps track of STARTED, FAILED, STOPPED
        // This is because a FAILED state can occur because we cannot read the config from the internal index, which would imply that
        //   we could not read the previous state information from said index.
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
                auditor.warning(transform.getId(),
                    "Failed to persist to cluster state while marking task as started. Failure: " + exc.getMessage());
                logger.error(new ParameterizedMessage("[{}] failed updating state to [{}].", getTransformId(), state), exc);
                getIndexer().stop();
                listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform ["
                                    + transform.getId() + "] to [" + state.getIndexerState() + "].", exc));
            }
        ));
    }

    public synchronized void stop(boolean force) {
        logger.debug("[{}] stop called with force [{}] and state [{}]", getTransformId(), force, getState());
        if (getIndexer() == null) {
            // If there is no indexer the task has not been triggered
            // but it still needs to be stopped and removed
            shutdown();
            return;
        }

        if (getIndexer().getState() == IndexerState.STOPPED) {
            return;
        }

        if (taskState.get() == DataFrameTransformTaskState.FAILED && force == false) {
            throw new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM,
                    getTransformId(),
                    stateReason.get()),
                RestStatus.CONFLICT);
        }

        IndexerState state = getIndexer().stop();
        stateReason.set(null);
        // We just don't want it to be failed if it is failed
        // Either we are running, and the STATE is already started or failed
        // doSaveState should transfer the state to STOPPED when it needs to.
        taskState.set(DataFrameTransformTaskState.STARTED);
        if (state == IndexerState.STOPPED) {
            getIndexer().onStop();
            getIndexer().doSaveState(state, getIndexer().getPosition(), () -> {});
        }
    }

    @Override
    public synchronized void triggered(Event event) {
        // Ignore if event is not for this job
        if (event.getJobName().equals(schedulerJobName()) == false)  {
            return;
        }

        if (getIndexer() == null) {
            logger.warn("[{}] data frame task triggered with an unintialized indexer.", getTransformId());
            return;
        }

        if (taskState.get() == DataFrameTransformTaskState.FAILED) {
            logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getTransformId());
            return;
        }

        // ignore trigger if indexer is running or completely stopped
        IndexerState indexerState = getIndexer().getState();
        if (IndexerState.INDEXING.equals(indexerState) ||
            IndexerState.STOPPING.equals(indexerState) ||
            IndexerState.STOPPED.equals(indexerState)) {
            logger.debug("[{}] indexer for transform has state [{}]. Ignoring trigger.", getTransformId(), indexerState);
            return;
        }

        logger.debug("[{}] data frame indexer schedule has triggered, state: [{}].", event.getJobName(), indexerState);

        // if it runs for the 1st time we just do it, if not we check for changes
        if (currentCheckpoint.get() == 0) {
            logger.debug("Trigger initial run.");
            getIndexer().maybeTriggerAsyncJob(System.currentTimeMillis());
        } else if (getIndexer().isContinuous()) {
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
                logger.debug("[{}] successfully updated state for data frame transform to [{}].", transform.getId(), state.toString());
                listener.onResponse(success);
            },
            failure -> {
                logger.error(new ParameterizedMessage("[{}] failed to update cluster state for data frame transform.",
                    transform.getId()),
                    failure);
                listener.onFailure(failure);
            }
        ));
    }

    synchronized void markAsFailed(String reason, ActionListener<Void> listener) {
        // If we are already flagged as failed, this probably means that a second trigger started firing while we were attempting to
        // flag the previously triggered indexer as failed. Exit early as we are already flagged as failed.
        if (taskState.get() == DataFrameTransformTaskState.FAILED) {
            logger.warn("[{}] is already failed but encountered new failure; reason [{}].", getTransformId(), reason);
            listener.onResponse(null);
            return;
        }
        // If the indexer is `STOPPING` this means that `DataFrameTransformTask#stop` was called previously, but something caused
        // the indexer to fail. Since `ClientDataFrameIndexer#doSaveState` will persist the state to the index once the indexer stops,
        // it is probably best to NOT change the internal state of the task and allow the normal stopping logic to continue.
        if (getIndexer() != null && getIndexer().getState() == IndexerState.STOPPING) {
            logger.info("[{}] attempt to fail transform with reason [{}] while it was stopping.", getTransformId(), reason);
            listener.onResponse(null);
            return;
        }
        // If we are stopped, this means that between the failure occurring and being handled, somebody called stop
        // We should just allow that stop to continue
        if (getIndexer() != null && getIndexer().getState() == IndexerState.STOPPED) {
            logger.info("[{}] encountered a failure but indexer is STOPPED; reason [{}].", getTransformId(), reason);
            listener.onResponse(null);
            return;
        }
        auditor.error(transform.getId(), reason);
        // We should not keep retrying. Either the task will be stopped, or started
        // If it is started again, it is registered again.
        deregisterSchedulerJob();
        taskState.set(DataFrameTransformTaskState.FAILED);
        stateReason.set(reason);
        DataFrameTransformState newState = getState();
        // Even though the indexer information is persisted to an index, we still need DataFrameTransformTaskState in the clusterstate
        // This keeps track of STARTED, FAILED, STOPPED
        // This is because a FAILED state could occur because we failed to read the config from the internal index, which would imply that
        //   we could not read the previous state information from said index.
        persistStateToClusterState(newState, ActionListener.wrap(
            r -> listener.onResponse(null),
            e -> {
                String msg = "Failed to persist to cluster state while marking task as failed with reason [" + reason + "].";
                auditor.warning(transform.getId(),
                    msg + " Failure: " + e.getMessage());
                logger.error(new ParameterizedMessage("[{}] {}", getTransformId(), msg),
                    e);
                listener.onFailure(e);
            }
        ));
    }

    /**
     * This is called when the persistent task signals that the allocated task should be terminated.
     * Termination in the task framework is essentially voluntary, as the allocated task can only be
     * shut down from the inside.
     */
    @Override
    public synchronized void onCancelled() {
        logger.info("[{}] received cancellation request for data frame transform, state: [{}].",
            getTransformId(),
            taskState.get());
        if (getIndexer() != null && getIndexer().abort()) {
            // there is no background transform running, we can shutdown safely
            shutdown();
        }
    }

    public DataFrameTransformTask setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
        return this;
    }

    public int getNumFailureRetries() {
        return numFailureRetries;
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
            TimeValue frequency = transform.getFrequency();
            return now + (frequency == null ? SCHEDULER_NEXT_MILLISECONDS : frequency.getMillis());
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
        private DataFrameIndexerPosition initialPosition;
        private DataFrameTransformProgress progress;
        private DataFrameTransformCheckpoint lastCheckpoint;
        private DataFrameTransformCheckpoint nextCheckpoint;

        ClientDataFrameIndexerBuilder(String transformId) {
            this.transformId = transformId;
            this.initialStats = new DataFrameIndexerTransformStats();
        }

        ClientDataFrameIndexer build(DataFrameTransformTask parentTask) {
            CheckpointProvider checkpointProvider = transformsCheckpointService.getCheckpointProvider(transformConfig);

            return new ClientDataFrameIndexer(this.transformId,
                this.transformsConfigManager,
                checkpointProvider,
                new AtomicReference<>(this.indexerState),
                this.initialPosition,
                this.client,
                this.auditor,
                this.initialStats,
                this.transformConfig,
                this.fieldMappings,
                this.progress,
                this.lastCheckpoint,
                this.nextCheckpoint,
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

        ClientDataFrameIndexerBuilder setInitialPosition(DataFrameIndexerPosition initialPosition) {
            this.initialPosition = initialPosition;
            return this;
        }

        ClientDataFrameIndexerBuilder setProgress(DataFrameTransformProgress progress) {
            this.progress = progress;
            return this;
        }

        ClientDataFrameIndexerBuilder setLastCheckpoint(DataFrameTransformCheckpoint lastCheckpoint) {
            this.lastCheckpoint = lastCheckpoint;
            return this;
        }

        ClientDataFrameIndexerBuilder setNextCheckpoint(DataFrameTransformCheckpoint nextCheckpoint) {
            this.nextCheckpoint = nextCheckpoint;
            return this;
        }
    }

    static class ClientDataFrameIndexer extends DataFrameIndexer {

        private long logEvery = 1;
        private long logCount = 0;
        private final Client client;
        private final DataFrameTransformsConfigManager transformsConfigManager;
        private final CheckpointProvider checkpointProvider;
        private final String transformId;
        private final DataFrameTransformTask transformTask;
        private final AtomicInteger failureCount;
        private volatile boolean auditBulkFailures = true;
        // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
        private volatile String lastAuditedExceptionMessage = null;
        private final AtomicBoolean oldStatsCleanedUp = new AtomicBoolean(false);

        ClientDataFrameIndexer(String transformId,
                               DataFrameTransformsConfigManager transformsConfigManager,
                               CheckpointProvider checkpointProvider,
                               AtomicReference<IndexerState> initialState,
                               DataFrameIndexerPosition initialPosition,
                               Client client,
                               DataFrameAuditor auditor,
                               DataFrameIndexerTransformStats initialStats,
                               DataFrameTransformConfig transformConfig,
                               Map<String, String> fieldMappings,
                               DataFrameTransformProgress transformProgress,
                               DataFrameTransformCheckpoint lastCheckpoint,
                               DataFrameTransformCheckpoint nextCheckpoint,
                               DataFrameTransformTask parentTask) {
            super(ExceptionsHelper.requireNonNull(parentTask, "parentTask")
                    .threadPool
                    .executor(ThreadPool.Names.GENERIC),
                ExceptionsHelper.requireNonNull(auditor, "auditor"),
                transformConfig,
                fieldMappings,
                ExceptionsHelper.requireNonNull(initialState, "initialState"),
                initialPosition,
                initialStats == null ? new DataFrameIndexerTransformStats() : initialStats,
                transformProgress,
                lastCheckpoint,
                nextCheckpoint);
            this.transformId = ExceptionsHelper.requireNonNull(transformId, "transformId");
            this.transformsConfigManager = ExceptionsHelper.requireNonNull(transformsConfigManager, "transformsConfigManager");
            this.checkpointProvider = ExceptionsHelper.requireNonNull(checkpointProvider, "checkpointProvider");

            this.client = ExceptionsHelper.requireNonNull(client, "client");
            this.transformTask = parentTask;
            this.failureCount = new AtomicInteger(0);
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("[{}] attempted to start while failed.", transformId);
                listener.onFailure(new ElasticsearchException("Attempted to start a failed transform [{}].", transformId));
                return;
            }
            // On each run, we need to get the total number of docs and reset the count of processed docs
            // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
            // the progress here, and not in the executor.
            ActionListener<Void> updateConfigListener = ActionListener.wrap(
                updateConfigResponse -> {
                    if (initialRun()) {
                        createCheckpoint(ActionListener.wrap(cp -> {
                            nextCheckpoint = cp;
                            // If nextCheckpoint > 1, this means that we are now on the checkpoint AFTER the batch checkpoint
                            // Consequently, the idea of percent complete no longer makes sense.
                            if (nextCheckpoint.getCheckpoint() > 1) {
                                progress = new DataFrameTransformProgress(null, 0L, 0L);
                                super.onStart(now, listener);
                                return;
                            }
                            TransformProgressGatherer.getInitialProgress(this.client, buildFilterQuery(), getConfig(), ActionListener.wrap(
                                newProgress -> {
                                    logger.trace("[{}] reset the progress from [{}] to [{}].", transformId, progress, newProgress);
                                    progress = newProgress;
                                    super.onStart(now, listener);
                                },
                                failure -> {
                                    progress = null;
                                    logger.warn(new ParameterizedMessage("[{}] unable to load progress information for task.",
                                        transformId),
                                        failure);
                                    super.onStart(now, listener);
                                }
                            ));
                        }, listener::onFailure));
                    } else {
                        super.onStart(now, listener);
                    }
                },
                listener::onFailure
            );

            // If we are continuous, we will want to verify we have the latest stored configuration
            ActionListener<Void> changedSourceListener = ActionListener.wrap(
                r -> {
                    if (isContinuous()) {
                        transformsConfigManager.getTransformConfiguration(getJobId(), ActionListener.wrap(
                            config -> {
                                transformConfig = config;
                                logger.debug("[{}] successfully refreshed data frame transform config from index.", transformId);
                                updateConfigListener.onResponse(null);
                            },
                            failure -> {
                                String msg = DataFrameMessages.getMessage(
                                    DataFrameMessages.FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION,
                                    getJobId());
                                logger.error(msg, failure);
                                // If the transform config index or the transform config is gone, something serious occurred
                                // We are in an unknown state and should fail out
                                if (failure instanceof ResourceNotFoundException) {
                                    updateConfigListener.onFailure(new TransformConfigReloadingException(msg, failure));
                                } else {
                                    auditor.warning(getJobId(), msg);
                                    updateConfigListener.onResponse(null);
                                }
                            }
                        ));
                    } else {
                        updateConfigListener.onResponse(null);
                    }
                },
                listener::onFailure
            );

            // If we are not on the initial batch checkpoint and its the first pass of whatever continuous checkpoint we are on,
            // we should verify if there are local changes based on the sync config. If not, do not proceed further and exit.
            if (transformTask.currentCheckpoint.get() > 0 && initialRun()) {
                sourceHasChanged(ActionListener.wrap(
                    hasChanged -> {
                        if (hasChanged) {
                            transformTask.changesLastDetectedAt = Instant.now();
                            logger.debug("[{}] source has changed, triggering new indexer run.", transformId);
                            changedSourceListener.onResponse(null);
                        } else {
                            // No changes, stop executing
                            listener.onResponse(false);
                        }
                    },
                    listener::onFailure
                ));
            } else {
                changedSourceListener.onResponse(null);
            }
        }

        @Override
        protected String getJobId() {
            return transformId;
        }

        public CheckpointProvider getCheckpointProvider() {
            return checkpointProvider;
        }

        @Override
        public synchronized boolean maybeTriggerAsyncJob(long now) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getJobId());
                return false;
            }

            // ignore trigger if indexer is running, prevents log spam in A2P indexer
            IndexerState indexerState = getState();
            if (IndexerState.INDEXING.equals(indexerState) || IndexerState.STOPPING.equals(indexerState)) {
                logger.debug("[{}] indexer for transform has state [{}]. Ignoring trigger.", getJobId(), indexerState);
                return false;
            }

            return super.maybeTriggerAsyncJob(now);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("[{}] attempted to search while failed.", transformId);
                nextPhase.onFailure(new ElasticsearchException("Attempted to do a search request for failed transform [{}].",
                    transformId));
                return;
            }
            ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client,
                    SearchAction.INSTANCE, request, nextPhase);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("[{}] attempted to bulk index while failed.", transformId);
                nextPhase.onFailure(new ElasticsearchException("Attempted to do a bulk index request for failed transform [{}].",
                    transformId));
                return;
            }
            ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(),
                ClientHelper.DATA_FRAME_ORIGIN,
                client,
                BulkAction.INSTANCE,
                request,
                ActionListener.wrap(bulkResponse -> {
                    if (bulkResponse.hasFailures()) {
                        int failureCount = 0;
                        for(BulkItemResponse item : bulkResponse.getItems()) {
                            if (item.isFailed()) {
                                failureCount++;
                            }
                            // TODO gather information on irrecoverable failures and update isIrrecoverableFailure
                        }
                        if (auditBulkFailures) {
                            auditor.warning(transformId,
                                "Experienced at least [" +
                                    failureCount +
                                    "] bulk index failures. See the logs of the node running the transform for details. " +
                                    bulkResponse.buildFailureMessage());
                            auditBulkFailures = false;
                        }
                        // This calls AsyncTwoPhaseIndexer#finishWithIndexingFailure
                        // It increments the indexing failure, and then calls the `onFailure` logic
                        nextPhase.onFailure(
                            new BulkIndexingException("Bulk index experienced failures. " +
                                "See the logs of the node running the transform for details."));
                    } else {
                        auditBulkFailures = true;
                        nextPhase.onResponse(bulkResponse);
                    }
                }, nextPhase::onFailure));
        }

        @Override
        protected void doSaveState(IndexerState indexerState, DataFrameIndexerPosition position, Runnable next) {
            if (transformTask.taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("[{}] attempted to save state and stats while failed.", transformId);
                // If we are failed, we should call next to allow failure handling to occur if necessary.
                next.run();
                return;
            }
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

            DataFrameTransformTaskState taskState = transformTask.taskState.get();

            if (indexerState.equals(IndexerState.STARTED)
                && transformTask.currentCheckpoint.get() == 1
                && this.isContinuous() == false) {
                // set both to stopped so they are persisted as such
                taskState = DataFrameTransformTaskState.STOPPED;
                indexerState = IndexerState.STOPPED;

                auditor.info(transformConfig.getId(), "Data frame finished indexing all data, initiating stop");
                logger.info("[{}] data frame transform finished indexing all data, initiating stop.", transformConfig.getId());
            }

            final DataFrameTransformState state = new DataFrameTransformState(
                taskState,
                indexerState,
                position,
                transformTask.currentCheckpoint.get(),
                transformTask.stateReason.get(),
                getProgress());
            logger.debug("[{}] updating persistent state of transform to [{}].", transformConfig.getId(), state.toString());

            // Persist the current state and stats in the internal index. The interval of this method being
            // called is controlled by AsyncTwoPhaseIndexer#onBulkResponse which calls doSaveState every so
            // often when doing bulk indexing calls or at the end of one indexing run.
            transformsConfigManager.putOrUpdateTransformStoredDoc(
                    new DataFrameTransformStoredDoc(transformId, state, getStats()),
                    ActionListener.wrap(
                            r -> {
                                // for auto stop shutdown the task
                                if (state.getTaskState().equals(DataFrameTransformTaskState.STOPPED)) {
                                    transformTask.shutdown();
                                }
                                // Only do this clean up once, if it succeeded, no reason to do the query again.
                                if (oldStatsCleanedUp.compareAndSet(false, true)) {
                                    transformsConfigManager.deleteOldTransformStoredDocuments(transformId, ActionListener.wrap(
                                        nil -> {
                                            logger.trace("[{}] deleted old transform stats and state document", transformId);
                                            next.run();
                                        },
                                        e -> {
                                            String msg = LoggerMessageFormat.format("[{}] failed deleting old transform configurations.",
                                                transformId);
                                            logger.warn(msg, e);
                                            // If we have failed, we should attempt the clean up again later
                                            oldStatsCleanedUp.set(false);
                                            next.run();
                                        }
                                    ));
                                } else {
                                    next.run();
                                }
                            },
                            statsExc -> {
                                logger.error(new ParameterizedMessage("[{}] updating stats of transform failed.",
                                    transformConfig.getId()),
                                    statsExc);
                                auditor.warning(getJobId(),
                                    "Failure updating stats of transform: " + statsExc.getMessage());
                                // for auto stop shutdown the task
                                if (state.getTaskState().equals(DataFrameTransformTaskState.STOPPED)) {
                                    transformTask.shutdown();
                                }
                                next.run();
                            }
                    ));
        }

        @Override
        protected void onFailure(Exception exc) {
            // the failure handler must not throw an exception due to internal problems
            try {
                handleFailure(exc);
            } catch (Exception e) {
                logger.error(
                    new ParameterizedMessage("[{}] data frame transform encountered an unexpected internal exception: ", transformId),
                    e);
            }
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            try {
                // TODO: needs cleanup super is called with a listener, but listener.onResponse is called below
                // super.onFinish() fortunately ignores the listener
                super.onFinish(listener);
                long checkpoint = transformTask.currentCheckpoint.getAndIncrement();
                lastCheckpoint = getNextCheckpoint();
                nextCheckpoint = null;
                // Reset our failure count as we have finished and may start again with a new checkpoint
                failureCount.set(0);

                // With bucket_selector we could have read all the buckets and completed the transform
                // but not "see" all the buckets since they were filtered out. Consequently, progress would
                // show less than 100% even though we are done.
                // NOTE: this method is called in the same thread as the processing thread.
                // Theoretically, there should not be a race condition with updating progress here.
                // NOTE 2: getPercentComplete should only NOT be null on the first (batch) checkpoint
                if (progress != null && progress.getPercentComplete() != null && progress.getPercentComplete() < 100.0) {
                    progress.incrementDocsProcessed(progress.getTotalDocs() - progress.getDocumentsProcessed());
                }
                // If the last checkpoint is now greater than 1, that means that we have just processed the first
                // continuous checkpoint and should start recording the exponential averages
                if (lastCheckpoint != null && lastCheckpoint.getCheckpoint() > 1) {
                    long docsIndexed = 0;
                    long docsProcessed = 0;
                    // This should not happen as we simply create a new one when we reach continuous checkpoints
                    // but this is a paranoid `null` check
                    if (progress != null) {
                        docsIndexed = progress.getDocumentsIndexed();
                        docsProcessed = progress.getDocumentsProcessed();
                    }
                    long durationMs = System.currentTimeMillis() - lastCheckpoint.getTimestamp();
                    getStats().incrementCheckpointExponentialAverages(durationMs < 0 ? 0 : durationMs, docsIndexed, docsProcessed);
                }
                if (shouldAuditOnFinish(checkpoint)) {
                    auditor.info(transformTask.getTransformId(),
                        "Finished indexing for data frame transform checkpoint [" + checkpoint + "].");
                }
                logger.debug(
                    "[{}] finished indexing for data frame transform checkpoint [{}].", getJobId(), checkpoint);
                auditBulkFailures = true;
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        /**
         * Indicates if an audit message should be written when onFinish is called for the given checkpoint
         * We audit the first checkpoint, and then every 10 checkpoints until completedCheckpoint == 99
         * Then we audit every 100, until completedCheckpoint == 999
         *
         * Then we always audit every 1_000 checkpoints
         *
         * @param completedCheckpoint The checkpoint that was just completed
         * @return {@code true} if an audit message should be written
         */
        protected boolean shouldAuditOnFinish(long completedCheckpoint) {
            if (++logCount % logEvery != 0) {
                return false;
            }
            if (completedCheckpoint == 0) {
                return true;
            }
            int log10Checkpoint = (int) Math.floor(Math.log10(completedCheckpoint));
            logEvery = log10Checkpoint >= 3  ? 1_000 : (int)Math.pow(10.0, log10Checkpoint);
            logCount = 0;
            return true;
        }

        @Override
        protected void onStop() {
            auditor.info(transformConfig.getId(), "Data frame transform has stopped.");
            logger.info("[{}] data frame transform has stopped.", transformConfig.getId());
        }

        @Override
        protected void onAbort() {
            auditor.info(transformConfig.getId(), "Received abort request, stopping data frame transform.");
            logger.info("[{}] data frame transform received abort request. Stopping indexer.", transformConfig.getId());
            transformTask.shutdown();
        }

        @Override
        protected void createCheckpoint(ActionListener<DataFrameTransformCheckpoint> listener) {
            checkpointProvider.createNextCheckpoint(getLastCheckpoint(), ActionListener.wrap(
                    checkpoint -> transformsConfigManager.putTransformCheckpoint(checkpoint,
                        ActionListener.wrap(
                            putCheckPointResponse -> listener.onResponse(checkpoint),
                            createCheckpointException -> {
                                logger.warn(new ParameterizedMessage("[{}] failed to create checkpoint.", transformId),
                                    createCheckpointException);
                                listener.onFailure(
                                    new RuntimeException("Failed to create checkpoint due to " + createCheckpointException.getMessage(),
                                        createCheckpointException));
                            }
                    )),
                    getCheckPointException -> {
                        logger.warn(new ParameterizedMessage("[{}] failed to retrieve checkpoint.", transformId),
                            getCheckPointException);
                        listener.onFailure(
                            new RuntimeException("Failed to retrieve checkpoint due to " + getCheckPointException.getMessage(),
                                getCheckPointException));
                    }
            ));
        }

        @Override
        protected void sourceHasChanged(ActionListener<Boolean> hasChangedListener) {
            checkpointProvider.sourceHasChanged(getLastCheckpoint(),
                ActionListener.wrap(
                    hasChanged -> {
                        logger.trace("[{}] change detected [{}].", transformId, hasChanged);
                        hasChangedListener.onResponse(hasChanged);
                    },
                    e -> {
                        logger.warn(
                            new ParameterizedMessage(
                                "[{}] failed to detect changes for data frame transform. Skipping update till next check.",
                                transformId),
                            e);
                        auditor.warning(transformId,
                            "Failed to detect changes for data frame transform, skipping update till next check. Exception: "
                                + e.getMessage());
                        hasChangedListener.onResponse(false);
                    }));
        }

        private boolean isIrrecoverableFailure(Exception e) {
            return e instanceof IndexNotFoundException
                || e instanceof AggregationResultUtils.AggregationExtractionException
                || e instanceof TransformConfigReloadingException;
        }

        synchronized void handleFailure(Exception e) {
            logger.warn(new ParameterizedMessage("[{}] data frame transform encountered an exception: ",
                transformTask.getTransformId()),
                e);
            if (handleCircuitBreakingException(e)) {
                return;
            }

            if (isIrrecoverableFailure(e) || failureCount.incrementAndGet() > transformTask.getNumFailureRetries()) {
                String failureMessage = isIrrecoverableFailure(e) ?
                    "task encountered irrecoverable failure: " + e.getMessage() :
                    "task encountered more than " + transformTask.getNumFailureRetries() + " failures; latest failure: " + e.getMessage();
                failIndexer(failureMessage);
            } else {
                // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
                // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
                if (e.getMessage().equals(lastAuditedExceptionMessage) == false) {
                    auditor.warning(transformTask.getTransformId(),
                        "Data frame transform encountered an exception: " + e.getMessage() +
                            " Will attempt again at next scheduled trigger.");
                    lastAuditedExceptionMessage = e.getMessage();
                }
            }
        }

        @Override
        protected void failIndexer(String failureMessage) {
            logger.error("[{}] transform has failed; experienced: [{}].", getJobId(), failureMessage);
            auditor.error(transformTask.getTransformId(), failureMessage);
            transformTask.markAsFailed(failureMessage, ActionListener.wrap(
                r -> {
                    // Successfully marked as failed, reset counter so that task can be restarted
                    failureCount.set(0);
                }, e -> {}));
        }
    }

    // Considered a recoverable indexing failure
    private static class BulkIndexingException extends ElasticsearchException {
        BulkIndexingException(String msg, Object... args) {
            super(msg, args);
        }
    }

    private static class TransformConfigReloadingException extends ElasticsearchException {
        TransformConfigReloadingException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }
}
