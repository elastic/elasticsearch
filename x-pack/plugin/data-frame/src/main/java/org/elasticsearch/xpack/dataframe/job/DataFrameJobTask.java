/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    static final String SCHEDULE_NAME = DataFrame.TASK_NAME + "/schedule";

    public DataFrameJobTask(long id, String type, String action, TaskId parentTask, DataFrameJob job,
            DataFrameJobState state, Client client, SchedulerEngine schedulerEngine, ThreadPool threadPool,
            Map<String, String> headers) {
        super(id, type, action, DataFrameJob.PERSISTENT_TASK_DESCRIPTION_PREFIX + job.getConfig().getId(), parentTask, headers);
        this.job = job;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
        logger.info("construct job task");
        // todo: simplistic implementation for now
        IndexerState initialState = IndexerState.STOPPED;
        Map<String, Object> initialPosition = null;
        this.indexer = new ClientDataFrameIndexer(job, new AtomicReference<>(initialState), initialPosition, client);
    }

    public DataFrameJobConfig getConfig() {
        return job.getConfig();
    }

    public DataFrameJobState getState() {
        return new DataFrameJobState(indexer.getState(), indexer.getPosition());
    }

    public DataFrameIndexerJobStats getStats() {
        return indexer.getStats();
    }

    public synchronized void start(ActionListener<Response> listener) {
        // TODO: safeguards missing, see rollup code
        indexer.start();
        listener.onResponse(new StartDataFrameJobAction.Response(true));
    }

    public void stop(ActionListener<StopDataFrameJobAction.Response> listener) {
        // TODO: safeguards missing, see rollup code
        indexer.stop();
        listener.onResponse(new StopDataFrameJobAction.Response(true));
    }

    @Override
    public void triggered(Event event) {
        if (event.getJobName().equals(SCHEDULE_NAME + "_" + job.getConfig().getId())) {
            logger.debug(
                    "Data frame indexer [" + event.getJobName() + "] schedule has triggered, state: [" + indexer.getState() + "]");
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
            } else {
                final DataFrameJobState state = new DataFrameJobState(indexerState, getPosition());
                logger.info("Updating persistent state of job [" + job.getConfig().getId() + "] to [" + state.toString() + "]");

                // TODO: we can not persist the state right now, need to be called from the task
                updatePersistentTaskState(state, ActionListener.wrap(task -> next.run(), exc -> {
                    // We failed to update the persistent task for some reason,
                    // set our flag back to what it was before
                    next.run();
                }));
            }
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
        }
    }
}
