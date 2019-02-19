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
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.action.StartDataFrameTransformAction;
import org.elasticsearch.xpack.dataframe.action.StartDataFrameTransformAction.Response;
import org.elasticsearch.xpack.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DataFrameTransformTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformTask.class);
    public static final String SCHEDULE_NAME = DataFrameField.TASK_NAME + "/schedule";

    private final DataFrameTransform transform;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameIndexer indexer;

    // the generation of this data frame, for v1 there will be only
    // 0: data frame not created or still indexing
    // 1: data frame complete, all data has been indexed
    private final AtomicReference<Long> generation;

    private final AtomicReference<DataFrameTransformTaskState> taskState;

    public DataFrameTransformTask(long id, String type, String action, TaskId parentTask, DataFrameTransform transform,
                                  DataFrameTransformTaskState state, Client client,
                                  DataFrameTransformsConfigManager transformsConfigManager, SchedulerEngine schedulerEngine,
                                  ThreadPool threadPool, Map<String, String> headers) {
        super(id, type, action, DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + transform.getId(), parentTask, headers);
        this.transform = transform;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
        IndexerState initialState = IndexerState.STOPPED;
        long initialGeneration = 0;
        Map<String, Object> initialPosition = null;
        logger.info("[{}] init, got state: [{}]", transform.getId(), state != null);
        if (state != null) {
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
            initialGeneration = state.getGeneration();
            taskState = new AtomicReference<>(state);
        } else {
            taskState = new AtomicReference<>(new DataFrameTransformTaskState(DataFrameTransformState.STOPPED, null, 0, ""));
        }
        this.indexer = new ClientDataFrameIndexer(transform.getId(), transformsConfigManager, new AtomicReference<>(initialState),
                initialPosition, client);
        this.generation = new AtomicReference<>(initialGeneration);
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

    public DataFrameTransformTaskState getState() {
        return taskState.get();
    }

    public DataFrameIndexerTransformStats getStats() {
        return indexer.getStats();
    }

    public long getGeneration() {
        return generation.get();
    }

    public boolean isStopped() {
        return indexer.getState().equals(IndexerState.STOPPED);
    }

    public synchronized void start(ActionListener<Response> listener) {
        final IndexerState prevState = indexer.getState();
        if (prevState != IndexerState.STOPPED) {
            // fails if the task is not STOPPED
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame transform [{}], because state was [{}]",
                    transform.getId(), prevState));
            return;
        }

        final IndexerState newState = indexer.start();
        if (newState != IndexerState.STARTED) {
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame transform [{}], because state was [{}]",
                    transform.getId(), newState));
            return;
        }

        final DataFrameTransformTaskState state =
            new DataFrameTransformTaskState(IndexerState.STOPPED, indexer.getPosition(), generation.get());

        logger.debug("Updating state for data frame transform [{}] to [{}][{}]", transform.getId(), state.getState(),
            state.getPosition());
        setTaskState(state, ActionListener.wrap(
            r -> listener.onResponse(new StartDataFrameTransformAction.Response(true)),
            e -> {
                indexer.stop();
                listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform ["
                                    + transform.getId() + "] to [" + state.getState() + "].", e));
            }
        ));
    }

    public synchronized void stop(ActionListener<StopDataFrameTransformAction.Response> listener) {
        final IndexerState newState = indexer.stop();
        StopDataFrameTransformAction.Response positiveResponse = new StopDataFrameTransformAction.Response(true);
        switch (newState) {
        case STOPPED:
            if (taskState.get().getState() != DataFrameTransformState.STOPPED) {
                DataFrameTransformTaskState transformState =
                    new DataFrameTransformTaskState(IndexerState.STOPPED, indexer.getPosition(), generation.get());
                setTaskState(transformState, ActionListener.wrap(
                    r -> listener.onResponse(positiveResponse),
                    e -> listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform [{}] to [{}]",
                        e, transform.getId(), transformState.getState()))));
            } else {
                listener.onResponse(positiveResponse);
            }
            break;

        case STOPPING:
            // update the persistent state to STOPPED. There are two scenarios and both are safe:
            // 1. we persist STOPPED now, indexer continues a bit then sees the flag and checkpoints another STOPPED with the more recent
            // position.
            // 2. we persist STOPPED now, indexer continues a bit but then dies. When/if we resume we'll pick up at last checkpoint,
            // overwrite some docs and eventually checkpoint.
            DataFrameTransformTaskState state =
                new DataFrameTransformTaskState(IndexerState.STOPPED, indexer.getPosition(), generation.get());
            setTaskState(state, ActionListener.wrap(
                r -> listener.onResponse(positiveResponse),
                e -> listener.onFailure(new ElasticsearchException("Error while updating state for data frame transform [{}] to [{}]",
                    e, transform.getId(), state.getState()))));
            break;

        default:
            listener.onFailure(new ElasticsearchException("Cannot stop task for data frame transform [{}], because state was [{}]",
                    transform.getId(), newState));
            break;
        }
    }

    @Override
    public synchronized void triggered(Event event) {
        logger.info("Triggered called: " + event.getJobName());
        if (generation.get() == 0
            && event.getJobName().equals(SCHEDULE_NAME + "_" + transform.getId())
            && taskState.get().getState() != DataFrameTransformState.FAILED) {
            logger.info("Triggered executed: " + event.getJobName());
            logger.debug("Data frame indexer [" + event.getJobName() + "] schedule has triggered, state: [" + indexer.getState() + "]");
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

    void setTaskState(DataFrameTransformTaskState state) {
        setTaskState(state, ActionListener.wrap(r -> {}, e -> {}));
    }

    void setTaskState(DataFrameTransformTaskState state, ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>> listener) {
        updatePersistentTaskState(state, ActionListener.wrap(
            r -> {
                logger.info("Successfully set persistent state of transform [{}] to [{}]", transform.getId(),
                    state.getState());
                taskState.set(state);
                listener.onResponse(r);
            },
            e -> {
                logger.error("Updating persistent state of transform [" + transform.getId() + "] failed", e);
                listener.onFailure(e);
            }));
    }

    protected class ClientDataFrameIndexer extends DataFrameIndexer {
        private static final int LOAD_TRANSFORM_TIMEOUT_IN_SECONDS = 30;
        private final Client client;
        private final DataFrameTransformsConfigManager transformsConfigManager;
        private final String transformId;

        private DataFrameTransformConfig transformConfig = null;

        public ClientDataFrameIndexer(String transformId, DataFrameTransformsConfigManager transformsConfigManager,
                                      AtomicReference<IndexerState> initialState, Map<String, Object> initialPosition, Client client) {
            super(threadPool.executor(ThreadPool.Names.GENERIC), initialState, initialPosition);
            this.transformId = transformId;
            this.transformsConfigManager = transformsConfigManager;
            this.client = client;
        }

        @Override
        protected DataFrameTransformConfig getConfig() {
            return transformConfig;
        }

        @Override
        protected String getJobId() {
            return transformId;
        }

        @Override
        public synchronized boolean maybeTriggerAsyncJob(long now) {
            if (transformConfig == null) {
                CountDownLatch latch = new CountDownLatch(1);

                transformsConfigManager.getTransformConfiguration(transformId, new LatchedActionListener<>(ActionListener.wrap(config -> {
                    transformConfig = config;
                }, e -> {
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
                String msg = DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_INVALID, transformId);
                DataFrameTransformTaskState state =
                    new DataFrameTransformTaskState(DataFrameTransformState.FAILED, getPosition(), getGeneration(), msg);
                setTaskState(state);
                throw new RuntimeException(msg);
            }
            // TODO return here on taskState.get().getState() == FAILED ?
            return super.maybeTriggerAsyncJob(now);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(transform.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, SearchAction.INSTANCE,
                    request, nextPhase);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(transform.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, BulkAction.INSTANCE,
                    request, nextPhase);
        }

        @Override
        protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
            if (indexerState.equals(IndexerState.ABORTING)) {
                // If we're aborting, just invoke `next` (which is likely an onFailure handler)
                next.run();
                return;
            }

            if(indexerState.equals(IndexerState.STARTED)) {
                // if the indexer resets the state to started, it means it is done, so increment the generation
                generation.compareAndSet(0L, 1L);
            }

            final DataFrameTransformTaskState state = new DataFrameTransformTaskState(indexerState, getPosition(), generation.get());
            logger.info("Updating persistent state of transform [" + transform.getId() + "] to [" + state.getState().toString() + "]");

            setTaskState(state, ActionListener.wrap(r -> next.run(), e -> next.run()));
        }

        @Override
        protected void onFailure(Exception exc) {
            logger.warn("Data frame transform [" + transform.getId() + "] failed with an exception: ", exc);
            final DataFrameTransformTaskState state =
                new DataFrameTransformTaskState(DataFrameTransformState.FAILED, getPosition(), generation.get(), exc.getMessage());
            setTaskState(state);
        }

        @Override
        protected void onFinish() {
            logger.info("Finished indexing for data frame transform [" + transform.getId() + "]");
        }

        @Override
        protected void onAbort() {
            logger.info("Data frame transform [" + transform.getId() + "] received abort request, stopping indexer");
            shutdown();
        }
    }
}
