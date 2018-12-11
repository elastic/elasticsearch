/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.action.StartDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.StopDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.StartDataFrameJobAction.Response;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DataFrameJobTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(DataFrameJobTask.class);

    private final DataFrameJob job;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameIndexer indexer;

    // the generation of this data frame, for v1 there will be only
    // 0: data frame not created or still indexing
    // 1: data frame complete, all data has been indexed
    private final AtomicReference<Long> generation;

    static final String SCHEDULE_NAME = DataFrame.TASK_NAME + "/schedule";

    public DataFrameJobTask(long id, String type, String action, TaskId parentTask, DataFrameJob job,
            DataFrameJobState state, Client client, SchedulerEngine schedulerEngine, ThreadPool threadPool,
            Map<String, String> headers) {
        super(id, type, action, DataFrameJob.PERSISTENT_TASK_DESCRIPTION_PREFIX + job.getConfig().getId(), parentTask, headers);
        this.job = job;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
        IndexerState initialState = IndexerState.STOPPED;
        Map<String, Object> initialPosition = null;
        logger.info("[{}] init, got state: [{}]", job.getConfig().getId(), state != null);
        if (state != null) {
            final IndexerState existingState = state.getIndexerState();
            logger.info("[{}] Loading existing state: [{}], position [{}]", job.getConfig().getId(), existingState, state.getPosition());
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
        }

        this.indexer = new ClientDataFrameIndexer(job, new AtomicReference<>(initialState), initialPosition, client);
        this.generation = new AtomicReference<>(0L);
    }

    public DataFrameJobConfig getConfig() {
        return job.getConfig();
    }

    /**
     * Enable Task API to return detailed status information
     */
    @Override
    public Status getStatus() {
        return getState();
    }

    public DataFrameJobState getState() {
        return new DataFrameJobState(indexer.getState(), indexer.getPosition(), generation.get());
    }

    public DataFrameIndexerJobStats getStats() {
        return indexer.getStats();
    }

    public long getGeneration() {
        return generation.get();
    }

    public synchronized void start(ActionListener<Response> listener) {
        final IndexerState prevState = indexer.getState();
        if (prevState != IndexerState.STOPPED) {
            // fails if the task is not STOPPED
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame job [{}], because state was [{}]",
                    job.getConfig().getId(), prevState));
            return;
        }

        final IndexerState newState = indexer.start();
        if (newState != IndexerState.STARTED) {
            listener.onFailure(new ElasticsearchException("Cannot start task for data frame job [{}], because state was [{}]",
                    job.getConfig().getId(), newState));
            return;
        }

        final DataFrameJobState state = new DataFrameJobState(IndexerState.STOPPED, indexer.getPosition(), generation.get());

        logger.debug("Updating state for data frame job [{}] to [{}][{}]", job.getConfig().getId(), state.getIndexerState(),
                state.getPosition());
        updatePersistentTaskState(state,
                ActionListener.wrap(
                        (task) -> {
                            logger.debug("Successfully updated state for data frame job [" + job.getConfig().getId() + "] to ["
                                    + state.getIndexerState() + "][" + state.getPosition() + "]");
                            listener.onResponse(new StartDataFrameJobAction.Response(true));
                        }, (exc) -> {
                            // We were unable to update the persistent status, so we need to shutdown the indexer too.
                            indexer.stop();
                            listener.onFailure(new ElasticsearchException("Error while updating state for data frame job ["
                                    + job.getConfig().getId() + "] to [" + state.getIndexerState() + "].", exc));
                        })
        );
    }

    public synchronized void stop(ActionListener<StopDataFrameJobAction.Response> listener) {
        final IndexerState newState = indexer.stop();
        switch (newState) {
        case STOPPED:
            listener.onResponse(new StopDataFrameJobAction.Response(true));
            break;

        case STOPPING:
            // update the persistent state to STOPPED. There are two scenarios and both are safe:
            // 1. we persist STOPPED now, indexer continues a bit then sees the flag and checkpoints another STOPPED with the more recent
            // position.
            // 2. we persist STOPPED now, indexer continues a bit but then dies. When/if we resume we'll pick up at last checkpoint,
            // overwrite some docs and eventually checkpoint.
            DataFrameJobState state = new DataFrameJobState(IndexerState.STOPPED, indexer.getPosition(), generation.get());
            updatePersistentTaskState(state, ActionListener.wrap((task) -> {
                logger.debug("Successfully updated state for data frame job [{}] to [{}]", job.getConfig().getId(),
                        state.getIndexerState());
                listener.onResponse(new StopDataFrameJobAction.Response(true));
            }, (exc) -> {
                listener.onFailure(new ElasticsearchException("Error while updating state for data frame job [{}] to [{}]", exc,
                        job.getConfig().getId(), state.getIndexerState()));
            }));
            break;

        default:
            listener.onFailure(new ElasticsearchException("Cannot stop task for data frame job [{}], because state was [{}]",
                    job.getConfig().getId(), newState));
            break;
        }
    }

    @Override
    public synchronized void triggered(Event event) {
        if (generation.get() == 0 && event.getJobName().equals(SCHEDULE_NAME + "_" + job.getConfig().getId())) {
            logger.debug("Data frame indexer [" + event.getJobName() + "] schedule has triggered, state: [" + indexer.getState() + "]");
            indexer.maybeTriggerAsyncJob(System.currentTimeMillis());
        }
    }

    /**
     * Attempt to gracefully cleanup the data frame job so it can be terminated.
     * This tries to remove the job from the scheduler, and potentially any other
     * cleanup operations in the future
     */
    synchronized void shutdown() {
        try {
            logger.info("Data frame indexer [" + job.getConfig().getId() + "] received abort request, stopping indexer.");
            schedulerEngine.remove(SCHEDULE_NAME + "_" + job.getConfig().getId());
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
                "Received cancellation request for data frame job [" + job.getConfig().getId() + "], state: [" + indexer.getState() + "]");
        if (indexer.abort()) {
            // there is no background job running, we can shutdown safely
            shutdown();
        }
    }

    protected class ClientDataFrameIndexer extends DataFrameIndexer {
        private final Client client;

        public ClientDataFrameIndexer(DataFrameJob job, AtomicReference<IndexerState> initialState,
                Map<String, Object> initialPosition, Client client) {
            super(threadPool.executor(ThreadPool.Names.GENERIC), job, initialState, initialPosition);
            this.client = client;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, SearchAction.INSTANCE, request,
                    nextPhase);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, BulkAction.INSTANCE, request,
                    nextPhase);
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

            final DataFrameJobState state = new DataFrameJobState(indexerState, getPosition(), generation.get());
            logger.info("Updating persistent state of job [" + job.getConfig().getId() + "] to [" + state.toString() + "]");

            updatePersistentTaskState(state, ActionListener.wrap(task -> next.run(), exc -> {
                logger.error("Updating persistent state of job [" + job.getConfig().getId() + "] failed", exc);
                next.run();
            }));
        }

        @Override
        protected void onFailure(Exception exc) {
            logger.warn("Data frame job [" + job.getConfig().getId() + "] failed with an exception: ", exc);
        }

        @Override
        protected void onFinish() {
            logger.info("Finished indexing for data frame job [" + job.getConfig().getId() + "]");
        }

        @Override
        protected void onAbort() {
            logger.info("Data frame job [" + job.getConfig().getId() + "] received abort request, stopping indexer");
            shutdown();
        }
    }
}
