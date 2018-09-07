/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.apache.log4j.Logger;
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
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StartFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StartFeatureIndexBuilderJobAction.Response;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class FeatureIndexBuilderJobTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {

    private static final Logger logger = Logger.getLogger(FeatureIndexBuilderJobTask.class.getName());

    private final FeatureIndexBuilderJob job;
    private final ThreadPool threadPool;
    private final FeatureIndexBuilderIndexer indexer;

    static final String SCHEDULE_NAME = "xpack/feature_index_builder/job" + "/schedule";

    public FeatureIndexBuilderJobTask(long id, String type, String action, TaskId parentTask, FeatureIndexBuilderJob job,
            FeatureIndexBuilderJobState state, Client client, SchedulerEngine schedulerEngine, ThreadPool threadPool,
            Map<String, String> headers) {
        super(id, type, action, "" + "_" + job.getConfig().getId(), parentTask, headers);
        this.job = job;
        this.threadPool = threadPool;
        logger.info("construct job task");
        // todo: simplistic implementation for now
        IndexerState initialState = IndexerState.STOPPED;
        Map<String, Object> initialPosition = null;
        this.indexer = new ClientFeatureIndexBuilderIndexer(job, new AtomicReference<>(initialState), initialPosition, client);
    }

    public FeatureIndexBuilderJobConfig getConfig() {
        return job.getConfig();
    }

    public synchronized void start(ActionListener<Response> listener) {
        indexer.start();
        listener.onResponse(new StartFeatureIndexBuilderJobAction.Response(true));
    }

    @Override
    public void triggered(Event event) {
        if (event.getJobName().equals(SCHEDULE_NAME + "_" + job.getConfig().getId())) {
            logger.debug(
                    "FeatureIndexBuilder indexer [" + event.getJobName() + "] schedule has triggered, state: [" + indexer.getState() + "]");
            indexer.maybeTriggerAsyncJob(System.currentTimeMillis());
        }
    }

    protected class ClientFeatureIndexBuilderIndexer extends FeatureIndexBuilderIndexer {
        private final Client client;

        public ClientFeatureIndexBuilderIndexer(FeatureIndexBuilderJob job, AtomicReference<IndexerState> initialState,
                Map<String, Object> initialPosition, Client client) {
            super(threadPool.executor(ThreadPool.Names.GENERIC), job, initialState, initialPosition);
            this.client = client;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE, request,
                    nextPhase);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.ML_ORIGIN, client, BulkAction.INSTANCE, request, nextPhase);
        }

        @Override
        protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
            if (indexerState.equals(IndexerState.ABORTING)) {
                // If we're aborting, just invoke `next` (which is likely an onFailure handler)
                next.run();
            } else {
                // to be implemented

                final FeatureIndexBuilderJobState state = new FeatureIndexBuilderJobState(indexerState);
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
            logger.warn("FeatureIndexBuilder job [" + job.getConfig().getId() + "] failed with an exception: ", exc);
        }

        @Override
        protected void onFinish() {
            logger.info("Finished indexing for job [" + job.getConfig().getId() + "]");
        }

        @Override
        protected void onAbort() {
            logger.info("FeatureIndexBuilder job [" + job.getConfig().getId() + "] received abort request, stopping indexer");
        }
    }
}
