/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.List;
import java.util.function.BooleanSupplier;

public class TransportStopRollupAction extends TransportTasksAction<
    RollupJobTask,
    StopRollupJobAction.Request,
    StopRollupJobAction.Response,
    StopRollupJobAction.Response> {

    private final ThreadPool threadPool;

    @Inject
    public TransportStopRollupAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        super(
            StopRollupJobAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            StopRollupJobAction.Request::new,
            StopRollupJobAction.Response::new,
            StopRollupJobAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.threadPool = threadPool;
    }

    @Override
    protected List<RollupJobTask> processTasks(StopRollupJobAction.Request request) {
        return TransportTaskHelper.doProcessTasks(request.getId(), taskManager);
    }

    @Override
    protected void doExecute(Task task, StopRollupJobAction.Request request, ActionListener<StopRollupJobAction.Response> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(
        Task actionTask,
        StopRollupJobAction.Request request,
        RollupJobTask jobTask,
        ActionListener<StopRollupJobAction.Response> listener
    ) {
        if (jobTask.getConfig().getId().equals(request.getId())) {
            jobTask.stop(maybeWrapWithBlocking(request, jobTask, listener, threadPool));
        } else {
            listener.onFailure(
                new RuntimeException(
                    "ID of rollup task [" + jobTask.getConfig().getId() + "] does not match request's ID [" + request.getId() + "]"
                )
            );
        }
    }

    private static ActionListener<StopRollupJobAction.Response> maybeWrapWithBlocking(
        StopRollupJobAction.Request request,
        RollupJobTask jobTask,
        ActionListener<StopRollupJobAction.Response> listener,
        ThreadPool threadPool
    ) {
        if (request.waitForCompletion()) {
            return ActionListener.wrap(response -> {
                if (response.isStopped()) {
                    // The Task acknowledged that it is stopped/stopping... wait until the status actually
                    // changes over before returning. Switch over to Generic threadpool so
                    // we don't block the network thread
                    threadPool.generic().execute(() -> {
                        try {
                            boolean stopped = awaitBusy(
                                () -> ((RollupJobStatus) jobTask.getStatus()).getIndexerState().equals(IndexerState.STOPPED),
                                request.timeout()
                            );

                            if (stopped) {
                                // We have successfully confirmed a stop, send back the response
                                listener.onResponse(response);
                            } else {
                                listener.onFailure(
                                    new ElasticsearchTimeoutException(
                                        "Timed out after ["
                                            + request.timeout().getStringRep()
                                            + "] while waiting for rollup job ["
                                            + request.getId()
                                            + "] to stop. State was ["
                                            + ((RollupJobStatus) jobTask.getStatus()).getIndexerState()
                                            + "]"
                                    )
                                );
                            }
                        } catch (InterruptedException e) {
                            listener.onFailure(e);
                        } catch (Exception e) {
                            listener.onFailure(
                                new ElasticsearchTimeoutException(
                                    "Encountered unexpected error while waiting for "
                                        + "rollup job ["
                                        + request.getId()
                                        + "] to stop.  State was ["
                                        + ((RollupJobStatus) jobTask.getStatus()).getIndexerState()
                                        + "].",
                                    e
                                )
                            );
                        }
                    });

                } else {
                    // Did not acknowledge stop, just return the response
                    listener.onResponse(response);
                }
            }, listener::onFailure);
        }
        // No request to block, execute async
        return listener;
    }

    /**
     * Lifted from ESTestCase, must stay private and do not reuse!  This is temporary until
     * the Rollup state refactor makes it unnecessary to await on a status change
     */
    private static boolean awaitBusy(BooleanSupplier breakSupplier, TimeValue maxWaitTime) throws InterruptedException {
        long maxTimeInMillis = maxWaitTime.getMillis();
        long timeInMillis = 1;
        long sum = 0;
        while (sum + timeInMillis < maxTimeInMillis) {
            if (breakSupplier.getAsBoolean()) {
                return true;
            }
            Thread.sleep(timeInMillis);
            sum += timeInMillis;
            timeInMillis = Math.min(1000L, timeInMillis * 2);
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakSupplier.getAsBoolean();
    }

    @Override
    protected StopRollupJobAction.Response newResponse(
        StopRollupJobAction.Request request,
        List<StopRollupJobAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {

        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the job doesn't exist (the user didn't create it yet) or was deleted after the Stop API executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for Rollup Job [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStopped = tasks.stream().allMatch(StopRollupJobAction.Response::isStopped);
        return new StopRollupJobAction.Response(allStopped);
    }

}
