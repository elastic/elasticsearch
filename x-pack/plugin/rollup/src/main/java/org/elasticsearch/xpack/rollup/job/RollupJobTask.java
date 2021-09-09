/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains the high-level logic that drives the rollup job. The allocated task contains transient state
 * which drives the indexing, and periodically updates it's parent PersistentTask with the indexing's current position.
 *
 * Each RollupJobTask also registers itself into the Scheduler so that it can be triggered on the cron's interval.
 */
public class RollupJobTask extends AllocatedPersistentTask implements SchedulerEngine.Listener {
    private static final Logger logger = LogManager.getLogger(RollupJobTask.class.getName());

    static final String SCHEDULE_NAME = RollupField.TASK_NAME + "/schedule";

    public static class RollupJobPersistentTasksExecutor extends PersistentTasksExecutor<RollupJob> {
        private final Client client;
        private final SchedulerEngine schedulerEngine;
        private final ThreadPool threadPool;

        public RollupJobPersistentTasksExecutor(Client client, SchedulerEngine schedulerEngine, ThreadPool threadPool) {
            super(RollupField.TASK_NAME, ThreadPool.Names.GENERIC);
            this.client = client;
            this.schedulerEngine = schedulerEngine;
            this.threadPool = threadPool;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, @Nullable RollupJob params, PersistentTaskState state) {
            RollupJobTask rollupJobTask = (RollupJobTask) task;
            SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(
                SCHEDULE_NAME + "_" + params.getConfig().getId(),
                new CronSchedule(params.getConfig().getCron())
            );

            // Note that while the task is added to the scheduler here, the internal state will prevent
            // it from doing any work until the task is "started" via the StartJob api
            schedulerEngine.register(rollupJobTask);
            schedulerEngine.add(schedulerJob);

            logger.info("Rollup job [" + params.getConfig().getId() + "] created.");
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<RollupJob> persistentTask,
            Map<String, String> headers
        ) {
            return new RollupJobTask(
                id,
                type,
                action,
                parentTaskId,
                persistentTask.getParams(),
                (RollupJobStatus) persistentTask.getState(),
                client,
                schedulerEngine,
                threadPool,
                headers
            );
        }
    }

    /**
     * An implementation of {@link RollupIndexer} that uses a {@link Client} to perform search
     * and bulk requests.
     * It uses the {@link ThreadPool.Names#GENERIC} thread pool to fire the first request in order
     * to make sure that we never use the same thread than the persistent task to execute the rollup.
     * The execution in the generic thread pool should terminate quickly since we use async call in the {@link Client}
     * to perform all requests.
     */
    protected class ClientRollupPageManager extends RollupIndexer {
        private final Client client;
        private final RollupJob job;

        ClientRollupPageManager(RollupJob job, IndexerState initialState, Map<String, Object> initialPosition, Client client) {
            super(threadPool, job, new AtomicReference<>(initialState), initialPosition);
            this.client = client;
            this.job = job;
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(
                job.getHeaders(),
                ClientHelper.ROLLUP_ORIGIN,
                client,
                SearchAction.INSTANCE,
                buildSearchRequest(),
                nextPhase
            );
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ClientHelper.executeWithHeadersAsync(
                job.getHeaders(),
                ClientHelper.ROLLUP_ORIGIN,
                client,
                BulkAction.INSTANCE,
                request,
                nextPhase
            );
        }

        @Override
        protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
            if (indexerState.equals(IndexerState.ABORTING)) {
                // If we're aborting, just invoke `next` (which is likely an onFailure handler)
                next.run();
            } else {
                // Otherwise, attempt to persist our state
                final RollupJobStatus state = new RollupJobStatus(indexerState, getPosition());
                logger.debug("Updating persistent state of job [" + job.getConfig().getId() + "] to [" + indexerState.toString() + "]");
                updatePersistentTaskState(state, ActionListener.wrap(task -> next.run(), exc -> next.run()));
            }
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            logger.debug("Finished indexing for job [" + job.getConfig().getId() + "]");
            listener.onResponse(null);
        }

        @Override
        protected void onFailure(Exception exc) {
            logger.warn("Rollup job [" + job.getConfig().getId() + "] failed with an exception: ", exc);
        }

        @Override
        protected void onAbort() {
            shutdown();
        }
    }

    private final RollupJob job;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final Client client;
    private final IndexerState initialIndexerState;
    private final Map<String, Object> initialPosition;
    private RollupIndexer indexer;

    RollupJobTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        RollupJob job,
        RollupJobStatus state,
        Client client,
        SchedulerEngine schedulerEngine,
        ThreadPool threadPool,
        Map<String, String> headers
    ) {
        super(id, type, action, RollupField.NAME + "_" + job.getConfig().getId(), parentTask, headers);
        this.job = job;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
        this.client = client;
        if (state == null) {
            this.initialIndexerState = null;
            this.initialPosition = null;
        } else {
            this.initialIndexerState = state.getIndexerState();
            this.initialPosition = state.getPosition();
        }

    }

    @Override
    protected void init(
        PersistentTasksService persistentTasksService,
        TaskManager taskManager,
        String persistentTaskId,
        long allocationId
    ) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId);

        // If initial position is not null, we are resuming rather than starting fresh.
        IndexerState indexerState = IndexerState.STOPPED;
        if (initialIndexerState != null) {
            logger.debug(
                "We have existing state, setting state to ["
                    + initialIndexerState
                    + "] "
                    + "and current position to ["
                    + initialPosition
                    + "] for job ["
                    + job.getConfig().getId()
                    + "]"
            );
            if (initialIndexerState.equals(IndexerState.INDEXING)) {
                /*
                 * If we were indexing, we have to reset back to STARTED otherwise the indexer will be "stuck" thinking
                 * it is indexing but without the actual indexing thread running.
                 */
                indexerState = IndexerState.STARTED;

            } else if (initialIndexerState.equals(IndexerState.ABORTING) || initialIndexerState.equals(IndexerState.STOPPING)) {
                // It shouldn't be possible to persist ABORTING, but if for some reason it does,
                // play it safe and restore the job as STOPPED. An admin will have to clean it up,
                // but it won't be running, and won't delete itself either. Safest option.
                // If we were STOPPING, that means it persisted but was killed before finally stopped... so ok
                // to restore as STOPPED
                indexerState = IndexerState.STOPPED;
            } else {
                indexerState = initialIndexerState;
            }

        }
        this.indexer = new ClientRollupPageManager(
            job,
            indexerState,
            initialPosition,
            new ParentTaskAssigningClient(client, getParentTaskId())
        );
    }

    @Override
    public Status getStatus() {
        return new RollupJobStatus(indexer.getState(), indexer.getPosition());
    }

    /**
     * Gets the stats for this task.
     * @return The stats of this task
     */
    public RollupIndexerJobStats getStats() {
        return indexer.getStats();
    }

    /**
     * The config of this task
     * @return The config for this task
     */
    public RollupJobConfig getConfig() {
        return job.getConfig();
    }

    /**
     * Attempt to start the indexer.
     * - If the indexer is started/indexing, returns OK
     * - If the indexer is stopped, starts task, updates persistent task's status, returns ok
     * - Anything else returns error
     *
     * Note that while the job is started, the indexer will not necessarily run immediately.  That
     * will only occur when the scheduler triggers it based on the cron
     *
     * @param listener The listener that started the action, so that we can signal completion/failure
     */
    public synchronized void start(ActionListener<StartRollupJobAction.Response> listener) {
        final IndexerState prevState = indexer.getState();

        if (prevState == IndexerState.STARTED || prevState == IndexerState.INDEXING) {
            // We're already running so just return acknowledgement
            logger.debug("Indexer already running (State: [" + prevState + "]), acknowledging start without change.");
            listener.onResponse(new StartRollupJobAction.Response(true));
            return;
        } else if (prevState != IndexerState.STOPPED) {
            // if we're not already started/indexing, we must be STOPPED to get started
            listener.onFailure(
                new ElasticsearchException(
                    "Cannot start task for Rollup Job [" + job.getConfig().getId() + "] because" + " state was [" + prevState + "]"
                )
            );
            return;
        }

        final IndexerState newState = indexer.start();
        if (newState != IndexerState.STARTED) {
            listener.onFailure(
                new ElasticsearchException(
                    "Cannot start task for Rollup Job [" + job.getConfig().getId() + "] because" + " new state was [" + newState + "]"
                )
            );
            return;
        }

        final RollupJobStatus state = new RollupJobStatus(IndexerState.STARTED, indexer.getPosition());
        logger.debug(
            "Updating state for rollup job ["
                + job.getConfig().getId()
                + "] to ["
                + state.getIndexerState()
                + "]["
                + state.getPosition()
                + "]"
        );
        updatePersistentTaskState(state, ActionListener.wrap((task) -> {
            logger.debug(
                "Successfully updated state for rollup job ["
                    + job.getConfig().getId()
                    + "] to ["
                    + state.getIndexerState()
                    + "]["
                    + state.getPosition()
                    + "]"
            );
            listener.onResponse(new StartRollupJobAction.Response(true));
        }, (exc) -> {
            // We were unable to update the persistent status, so we need to shutdown the indexer too.
            indexer.stop();
            listener.onFailure(
                new ElasticsearchException(
                    "Error while updating state for rollup job [" + job.getConfig().getId() + "] to [" + state.getIndexerState() + "].",
                    exc
                )
            );
        }));
    }

    /**
     * Attempt to stop the indexer if it is idle or actively indexing.
     * If the indexer is aborted this will fail with an exception.
     *
     * Note that stopping the job is not immediate.  It updates the persistent task's status, but then the allocated
     * task has to notice and stop itself (which may take some time, depending on where in the indexing cycle it is).
     *
     * This method will, however, return as soon as the persistent task has acknowledge the status update.
     *
     * @param listener The listener that is requesting the stop, so that we can signal completion/failure
     */
    public synchronized void stop(ActionListener<StopRollupJobAction.Response> listener) {

        final IndexerState newState = indexer.stop();
        switch (newState) {
            case STOPPED:
                listener.onResponse(new StopRollupJobAction.Response(true));
                break;

            case STOPPING:
                // update the persistent state to STOPPED. There are two scenarios and both are safe:
                // 1. we persist STOPPED now, indexer continues a bit then sees the flag and checkpoints another
                // STOPPED with the more recent position.
                // 2. we persist STOPPED now, indexer continues a bit but then dies. When/if we resume we'll pick up
                // at last checkpoint, overwrite some docs and eventually checkpoint.
                RollupJobStatus state = new RollupJobStatus(IndexerState.STOPPED, indexer.getPosition());
                updatePersistentTaskState(state, ActionListener.wrap((task) -> {
                    logger.debug(
                        "Successfully updated state for rollup job [" + job.getConfig().getId() + "] to [" + state.getIndexerState() + "]"
                    );
                    listener.onResponse(new StopRollupJobAction.Response(true));
                }, (exc) -> {
                    listener.onFailure(
                        new ElasticsearchException(
                            "Error while updating state for rollup job ["
                                + job.getConfig().getId()
                                + "] to ["
                                + state.getIndexerState()
                                + "].",
                            exc
                        )
                    );
                }));
                break;

            default:
                listener.onFailure(
                    new ElasticsearchException(
                        "Cannot stop task for Rollup Job [" + job.getConfig().getId() + "] because" + " state was [" + newState + "]"
                    )
                );
                break;
        }
    }

    /**
     * Attempt to gracefully cleanup the rollup job so it can be terminated.
     * This tries to remove the job from the scheduler, and potentially any other
     * cleanup operations in the future
     */
    synchronized void shutdown() {
        try {
            logger.info("Rollup indexer [" + job.getConfig().getId() + "] received abort request, stopping indexer.");
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
        logger.info("Received cancellation request for Rollup job [" + job.getConfig().getId() + "], state: [" + indexer.getState() + "]");
        if (indexer.abort()) {
            // there is no background job running, we can shutdown safely
            shutdown();
        }
    }

    /**
     * This is called by the ScheduleEngine when the cron triggers.
     *
     * @param event The event that caused the trigger
     */
    @Override
    public synchronized void triggered(SchedulerEngine.Event event) {
        // Verify this is actually the event that we care about, then trigger the indexer.
        // Note that the status of the indexer is checked in the indexer itself
        if (event.getJobName().equals(SCHEDULE_NAME + "_" + job.getConfig().getId())) {
            logger.debug("Rollup indexer [" + event.getJobName() + "] schedule has triggered, state: [" + indexer.getState() + "]");
            indexer.maybeTriggerAsyncJob(System.currentTimeMillis());
        }
    }
}
