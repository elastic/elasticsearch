/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
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
import org.elasticsearch.xpack.core.common.notifications.Auditor;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction.Response;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.SchemaUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class DataFrameTransformTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformTask.class);
    // TODO consider moving to dynamic cluster setting
    private static final int MAX_CONTINUOUS_FAILURES = 10;
    private static final IndexerState[] RUNNING_STATES = new IndexerState[]{IndexerState.STARTED, IndexerState.INDEXING};
    public static final String SCHEDULE_NAME = DataFrameField.TASK_NAME + "/schedule";

    private final DataFrameTransform transform;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameIndexer indexer;
    private final Auditor<DataFrameAuditMessage> auditor;
    private final DataFrameIndexerTransformStats previousStats;

    private final AtomicReference<DataFrameTransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    // the checkpoint of this data frame, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;
    private final AtomicInteger failureCount;

    public DataFrameTransformTask(long id, String type, String action, TaskId parentTask, DataFrameTransform transform,
                                  DataFrameTransformState state, Client client, DataFrameTransformsConfigManager transformsConfigManager,
                                  DataFrameTransformsCheckpointService transformsCheckpointService,
                                  SchedulerEngine schedulerEngine, Auditor<DataFrameAuditMessage> auditor,
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
        logger.info("[{}] init, got state: [{}]", transform.getId(), state != null);
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

        this.indexer = new ClientDataFrameIndexer(transform.getId(), transformsConfigManager, transformsCheckpointService,
            new AtomicReference<>(initialState), initialPosition, client, auditor);
        this.currentCheckpoint = new AtomicLong(initialGeneration);
        this.previousStats = new DataFrameIndexerTransformStats(transform.getId());
        this.taskState = new AtomicReference<>(initialTaskState);
        this.stateReason = new AtomicReference<>(initialReason);
        this.failureCount = new AtomicInteger(0);
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

    public DataFrameTransformState getState() {
        return new DataFrameTransformState(
                taskState.get(),
                indexer.getState(),
                indexer.getPosition(),
                currentCheckpoint.get(),
                stateReason.get());
    }

    void initializePreviousStats(DataFrameIndexerTransformStats stats) {
        previousStats.merge(stats);
    }

    public DataFrameIndexerTransformStats getStats() {
        return new DataFrameIndexerTransformStats(previousStats).merge(indexer.getStats());
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
        return indexer.getState().equals(IndexerState.INDEXING) ? currentCheckpoint.get() + 1L : 0;
    }

    public boolean isStopped() {
        return indexer.getState().equals(IndexerState.STOPPED);
    }

    public synchronized void start(ActionListener<Response> listener) {
        final IndexerState newState = indexer.start();
        if (Arrays.stream(RUNNING_STATES).noneMatch(newState::equals)) {
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame transform [{}], because state was [{}]",
                    transform.getId(), newState));
            return;
        }
        stateReason.set(null);
        taskState.set(DataFrameTransformTaskState.STARTED);
        failureCount.set(0);

        final DataFrameTransformState state = new DataFrameTransformState(
            DataFrameTransformTaskState.STARTED,
            IndexerState.STOPPED,
            indexer.getPosition(),
            currentCheckpoint.get(),
            null);

        logger.info("Updating state for data frame transform [{}] to [{}]", transform.getId(), state.toString());
        persistStateToClusterState(state, ActionListener.wrap(
            task -> {
                auditor.info(transform.getId(), "Updated state to [" + state.getTaskState() + "]");
                listener.onResponse(new StartDataFrameTransformTaskAction.Response(true));
            },
            exc -> {
                indexer.stop();
                listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform ["
                                    + transform.getId() + "] to [" + state.getIndexerState() + "].", exc));
            }
        ));
    }

    public synchronized void stop(ActionListener<StopDataFrameTransformAction.Response> listener) {
        // taskState is initialized as STOPPED and is updated in tandem with the indexerState
        // Consequently, if it is STOPPED, we consider the whole task STOPPED.
        if (taskState.get() == DataFrameTransformTaskState.STOPPED) {
            listener.onResponse(new StopDataFrameTransformAction.Response(true));
            return;
        }
        final IndexerState newState = indexer.stop();
        switch (newState) {
        case STOPPED:
            // Fall through to `STOPPING` as the behavior is the same for both, we should persist for both
        case STOPPING:
            // update the persistent state to STOPPED. There are two scenarios and both are safe:
            // 1. we persist STOPPED now, indexer continues a bit then sees the flag and checkpoints another STOPPED with the more recent
            // position.
            // 2. we persist STOPPED now, indexer continues a bit but then dies. When/if we resume we'll pick up at last checkpoint,
            // overwrite some docs and eventually checkpoint.
            taskState.set(DataFrameTransformTaskState.STOPPED);
            DataFrameTransformState state = new DataFrameTransformState(
                DataFrameTransformTaskState.STOPPED,
                IndexerState.STOPPED,
                indexer.getPosition(),
                currentCheckpoint.get(),
                stateReason.get());
            persistStateToClusterState(state, ActionListener.wrap(
                task -> {
                    auditor.info(transform.getId(), "Updated state to [" + state.getTaskState() + "]");
                    listener.onResponse(new StopDataFrameTransformAction.Response(true));
                },
                exc -> listener.onFailure(new ElasticsearchException(
                    "Error while updating state for data frame transform [{}] to [{}]", exc,
                    transform.getId(),
                    state.getIndexerState()))));
            break;
        default:
            listener.onFailure(new ElasticsearchException("Cannot stop task for data frame transform [{}], because state was [{}]",
                    transform.getId(), newState));
            break;
        }
    }

    @Override
    public synchronized void triggered(Event event) {
        //  for now no rerun, so only trigger if checkpoint == 0
        if (currentCheckpoint.get() == 0 && event.getJobName().equals(SCHEDULE_NAME + "_" + transform.getId())) {
            logger.debug("Data frame indexer [{}] schedule has triggered, state: [{}]", event.getJobName(), indexer.getState());
            indexer.maybeTriggerAsyncJob(System.currentTimeMillis());
        }
    }

    /**
     * Attempt to gracefully cleanup the data frame transform so it can be terminated.
     * This tries to remove the job from the scheduler, and potentially any other
     * cleanup operations in the future
     */
    synchronized void shutdown() {
        try {
            logger.info("Data frame indexer [" + transform.getId() + "] received abort request, stopping indexer.");
            schedulerEngine.remove(SCHEDULE_NAME + "_" + transform.getId());
            schedulerEngine.unregister(this);
        } catch (Exception e) {
            markAsFailed(e);
            return;
        }
        markAsCompleted();
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

    /**
     * This is called when the persistent task signals that the allocated task should be terminated.
     * Termination in the task framework is essentially voluntary, as the allocated task can only be
     * shut down from the inside.
     */
    @Override
    public synchronized void onCancelled() {
        logger.info(
                "Received cancellation request for data frame transform [" + transform.getId() + "], state: [" + indexer.getState() + "]");
        if (indexer.abort()) {
            // there is no background transform running, we can shutdown safely
            shutdown();
        }
    }

    protected class ClientDataFrameIndexer extends DataFrameIndexer {
        private static final int LOAD_TRANSFORM_TIMEOUT_IN_SECONDS = 30;

        private final Client client;
        private final DataFrameTransformsConfigManager transformsConfigManager;
        private final DataFrameTransformsCheckpointService transformsCheckpointService;
        private final String transformId;
        private volatile DataFrameIndexerTransformStats previouslyPersistedStats = null;
        // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
        private volatile String lastAuditedExceptionMessage = null;
        private Map<String, String> fieldMappings = null;

        private DataFrameTransformConfig transformConfig = null;

        public ClientDataFrameIndexer(String transformId, DataFrameTransformsConfigManager transformsConfigManager,
                                      DataFrameTransformsCheckpointService transformsCheckpointService,
                                      AtomicReference<IndexerState> initialState, Map<String, Object> initialPosition, Client client,
                                      Auditor<DataFrameAuditMessage> auditor) {
            super(threadPool.executor(ThreadPool.Names.GENERIC), auditor, initialState, initialPosition,
                new DataFrameIndexerTransformStats(transformId));
            this.transformId = transformId;
            this.transformsConfigManager = transformsConfigManager;
            this.transformsCheckpointService = transformsCheckpointService;
            this.client = client;
        }

        @Override
        protected DataFrameTransformConfig getConfig() {
            return transformConfig;
        }

        @Override
        protected Map<String, String> getFieldMappings() {
            return fieldMappings;
        }

        @Override
        protected String getJobId() {
            return transformId;
        }

        @Override
        public synchronized boolean maybeTriggerAsyncJob(long now) {
            if (taskState.get() == DataFrameTransformTaskState.FAILED) {
                logger.debug("Schedule was triggered for transform [{}] but task is failed. Ignoring trigger.", getJobId());
                return false;
            }

            if (transformConfig == null) {
                CountDownLatch latch = new CountDownLatch(1);

                transformsConfigManager.getTransformConfiguration(transformId, new LatchedActionListener<>(ActionListener.wrap(
                    config -> transformConfig = config,
                    e -> {
                    throw new RuntimeException(
                            DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_CONFIGURATION, transformId), e);
                }), latch));

                try {
                    latch.await(LOAD_TRANSFORM_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(
                            DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_CONFIGURATION, transformId), e);
                }
            }

            if (transformConfig.isValid() == false) {
                DataFrameConfigurationException exception = new DataFrameConfigurationException(transformId);
                handleFailure(exception);
                throw exception;
            }

            if (fieldMappings == null) {
                CountDownLatch latch = new CountDownLatch(1);
                SchemaUtil.getDestinationFieldMappings(client, transformConfig.getDestination().getIndex(), new LatchedActionListener<>(
                    ActionListener.wrap(
                        destinationMappings -> fieldMappings = destinationMappings,
                        e -> {
                            throw new RuntimeException(
                                DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UNABLE_TO_GATHER_FIELD_MAPPINGS,
                                    transformConfig.getDestination().getIndex()),
                                e);
                        }), latch));
                try {
                    latch.await(LOAD_TRANSFORM_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                   throw new RuntimeException(
                                DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UNABLE_TO_GATHER_FIELD_MAPPINGS,
                                    transformConfig.getDestination().getIndex()),
                                e);
                }
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

            final DataFrameTransformState state = new DataFrameTransformState(
                taskState.get(),
                indexerState,
                getPosition(),
                currentCheckpoint.get(),
                stateReason.get());
            logger.info("Updating persistent state of transform [" + transform.getId() + "] to [" + state.toString() + "]");

            // Persisting stats when we call `doSaveState` should be ok as we only call it on a state transition and
            // only every-so-often when doing the bulk indexing calls.  See AsyncTwoPhaseIndexer#onBulkResponse for current periodicity
            ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>> updateClusterStateListener = ActionListener.wrap(
                task -> {
                    // Make a copy of the previousStats so that they are not constantly updated when `merge` is called
                    DataFrameIndexerTransformStats tempStats = new DataFrameIndexerTransformStats(previousStats).merge(getStats());

                    // Only persist the stats if something has actually changed
                    if (previouslyPersistedStats == null || previouslyPersistedStats.equals(tempStats) == false) {
                        transformsConfigManager.putOrUpdateTransformStats(tempStats,
                            ActionListener.wrap(
                                r -> {
                                    previouslyPersistedStats = tempStats;
                                    next.run();
                                },
                                statsExc -> {
                                    logger.error("Updating stats of transform [" + transform.getId() + "] failed", statsExc);
                                    next.run();
                                }
                            ));
                    // The stats that we have previously written to the doc is the same as as it is now, no need to update it
                    } else {
                        next.run();
                    }
                },
                exc -> {
                    logger.error("Updating persistent state of transform [" + transform.getId() + "] failed", exc);
                    next.run();
                }
            );

            persistStateToClusterState(state, updateClusterStateListener);
        }

        @Override
        protected void onFailure(Exception exc) {
            // the failure handler must not throw an exception due to internal problems
            try {
                logger.warn("Data frame transform [" + transform.getId() + "] encountered an exception: ", exc);

                // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
                // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
                if (exc.getMessage().equals(lastAuditedExceptionMessage) == false) {
                    auditor.warning(transform.getId(), "Data frame transform encountered an exception: " + exc.getMessage());
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
                long checkpoint = currentCheckpoint.incrementAndGet();
                auditor.info(transform.getId(), "Finished indexing for data frame transform checkpoint [" + checkpoint + "]");
                logger.info("Finished indexing for data frame transform [" + transform.getId() + "] checkpoint [" + checkpoint + "]");
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected void onAbort() {
            auditor.info(transform.getId(), "Received abort request, stopping indexer");
            logger.info("Data frame transform [" + transform.getId() + "] received abort request, stopping indexer");
            shutdown();
        }

        @Override
        protected void createCheckpoint(ActionListener<Void> listener) {
            transformsCheckpointService.getCheckpoint(transformConfig, currentCheckpoint.get() + 1, ActionListener.wrap(checkpoint -> {
                transformsConfigManager.putTransformCheckpoint(checkpoint, ActionListener.wrap(putCheckPointResponse -> {
                    listener.onResponse(null);
                }, createCheckpointException -> {
                    listener.onFailure(new RuntimeException("Failed to create checkpoint", createCheckpointException));
                }));
            }, getCheckPointException -> {
                listener.onFailure(new RuntimeException("Failed to retrieve checkpoint", getCheckPointException));
            }));
        }

        private boolean isIrrecoverableFailure(Exception e) {
            return e instanceof IndexNotFoundException || e instanceof DataFrameConfigurationException;
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
            auditor.error(transform.getId(), failureMessage);
            stateReason.set(failureMessage);
            taskState.set(DataFrameTransformTaskState.FAILED);
            persistStateToClusterState(DataFrameTransformTask.this.getState(), ActionListener.wrap(
                r -> failureCount.set(0), // Successfully marked as failed, reset counter so that task can be restarted
                exception -> {} // Noop, internal method logs the failure to update the state
            ));
        }
    }

    class DataFrameConfigurationException extends RuntimeException {

        DataFrameConfigurationException(String transformId) {
            super(DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_INVALID, transformId));
        }

    }
}
