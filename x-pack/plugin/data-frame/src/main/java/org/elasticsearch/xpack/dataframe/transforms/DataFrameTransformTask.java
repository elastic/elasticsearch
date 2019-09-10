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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.SeqNoPrimaryTermAndIndex;

import java.util.Arrays;
import java.util.Map;
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

    private final SetOnce<ClientDataFrameIndexer> indexer = new SetOnce<>();

    private final AtomicReference<DataFrameTransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    private final AtomicReference<SeqNoPrimaryTermAndIndex> seqNoPrimaryTermAndIndex = new AtomicReference<>(null);
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

    long incrementCheckpoint() {
        return currentCheckpoint.getAndIncrement();
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
                        if (indexer.getChangesLastDetectedAt() == null) {
                            listener.onResponse(info);
                        } else {
                            listener.onResponse(info.setChangesLastDetectedAt(indexer.getChangesLastDetectedAt()));
                        }
                    },
                    listener::onFailure
                ));
    }

    // Here `failOnConflict` is usually true, except when the initial start is called when the task is assigned to the node
    synchronized void start(Long startingCheckpoint, boolean force, boolean failOnConflict, ActionListener<Response> listener) {
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
        // If we are already in a `STARTED` state, we should not attempt to call `.start` on the indexer again.
        if (taskState.get() == DataFrameTransformTaskState.STARTED && failOnConflict) {
            listener.onFailure(new ElasticsearchStatusException(
                "Cannot start transform [{}] as it is already STARTED.",
                RestStatus.CONFLICT,
                getTransformId()
            ));
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
    /**
     * Start the background indexer and set the task's state to started
     * @param startingCheckpoint Set the current checkpoint to this value. If null the
     *                           current checkpoint is not set
     * @param force Whether to force start a failed task or not
     * @param listener Started listener
     */
    public synchronized void start(Long startingCheckpoint, boolean force, ActionListener<Response> listener) {
        start(startingCheckpoint, force, true, listener);
    }

    public synchronized void stop(boolean force) {
        logger.debug("[{}] stop called with force [{}] and state [{}]", getTransformId(), force, getState());
        if (getIndexer() == null) {
            // If there is no indexer the task has not been triggered
            // but it still needs to be stopped and removed
            shutdown();
            return;
        }

        if (getIndexer().getState() == IndexerState.STOPPED || getIndexer().getState() == IndexerState.STOPPING) {
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
        // No reason to keep it in the potentially failed state.
        // Since we have called `stop` against the indexer, we have no more fear of triggering again.
        // But, since `doSaveState` is asynchronous, it is best to set the state as STARTED so that another `start` call cannot be
        // executed while we are wrapping up.
        taskState.compareAndSet(DataFrameTransformTaskState.FAILED, DataFrameTransformTaskState.STARTED);
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

        if (taskState.get() == DataFrameTransformTaskState.FAILED || taskState.get() == DataFrameTransformTaskState.STOPPED) {
            logger.debug("[{}] schedule was triggered for transform but task is [{}]. Ignoring trigger.",
                getTransformId(),
                taskState.get());
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
            logger.debug("[{}] trigger initial run.", getTransformId());
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

    DataFrameTransformTask setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
        return this;
    }

    int getNumFailureRetries() {
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

    void updateSeqNoPrimaryTermAndIndex(SeqNoPrimaryTermAndIndex expectedValue, SeqNoPrimaryTermAndIndex newValue) {
        boolean updated = seqNoPrimaryTermAndIndex.compareAndSet(expectedValue, newValue);
        // This should never happen. We ONLY ever update this value if at initialization or we just finished updating the document
        // famous last words...
        assert updated :
            "[" + getTransformId() + "] unexpected change to seqNoPrimaryTermAndIndex.";
    }

    @Nullable
    SeqNoPrimaryTermAndIndex getSeqNoPrimaryTermAndIndex() {
        return seqNoPrimaryTermAndIndex.get();
    }

    ThreadPool getThreadPool() {
        return threadPool;
    }

    DataFrameTransformTaskState getTaskState() {
        return taskState.get();
    }

    void setStateReason(String reason) {
        stateReason.set(reason);
    }

    String getStateReason() {
        return stateReason.get();
    }
}
