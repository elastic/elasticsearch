/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportStopDatafeedAction extends TransportTasksAction<TransportStartDatafeedAction.DatafeedTask, StopDatafeedAction.Request,
        StopDatafeedAction.Response, StopDatafeedAction.Response> {

    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportStopDatafeedAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                       ClusterService clusterService, PersistentTasksService persistentTasksService) {
        super(StopDatafeedAction.NAME, clusterService, transportService, actionFilters, StopDatafeedAction.Request::new,
            StopDatafeedAction.Response::new, StopDatafeedAction.Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
    }

    /**
     * Resolve the requested datafeeds and add their IDs to one of the list
     * arguments depending on datafeed state.
     *
     * @param request The stop datafeed request
     * @param mlMetadata ML Metadata
     * @param tasks Persistent task meta data
     * @param startedDatafeedIds Started datafeed ids are added to this list
     * @param stoppingDatafeedIds Stopping datafeed ids are added to this list
     */
    static void resolveDataFeedIds(StopDatafeedAction.Request request, MlMetadata mlMetadata,
                                   PersistentTasksCustomMetaData tasks,
                                   List<String> startedDatafeedIds,
                                   List<String> stoppingDatafeedIds) {

        Set<String> expandedDatafeedIds = mlMetadata.expandDatafeedIds(request.getDatafeedId(), request.allowNoDatafeeds());
        for (String expandedDatafeedId : expandedDatafeedIds) {
            validateDatafeedTask(expandedDatafeedId, mlMetadata);
            addDatafeedTaskIdAccordingToState(expandedDatafeedId, MlTasks.getDatafeedState(expandedDatafeedId, tasks),
                    startedDatafeedIds, stoppingDatafeedIds);
        }
    }

    private static void addDatafeedTaskIdAccordingToState(String datafeedId,
                                                          DatafeedState datafeedState,
                                                          List<String> startedDatafeedIds,
                                                          List<String> stoppingDatafeedIds) {
        switch (datafeedState) {
            case STARTED:
                startedDatafeedIds.add(datafeedId);
                break;
            case STOPPED:
                break;
            case STOPPING:
                stoppingDatafeedIds.add(datafeedId);
                break;
            default:
                break;
        }
    }

    /**
     * Validate the stop request.
     * Throws an {@code ResourceNotFoundException} if there is no datafeed
     * with id {@code datafeedId}
     * @param datafeedId The datafeed Id
     * @param mlMetadata ML meta data
     */
    static void validateDatafeedTask(String datafeedId, MlMetadata mlMetadata) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
        }
    }

    @Override
    protected void doExecute(Task task, StopDatafeedAction.Request request, ActionListener<StopDatafeedAction.Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates stop datafeed to elected master node, so it becomes the coordinating node.
            // See comment in StartDatafeedAction.Transport class for more information.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master node"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, StopDatafeedAction.Response::new));
            }
        } else {
            MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
            PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

            List<String> startedDatafeeds = new ArrayList<>();
            List<String> stoppingDatafeeds = new ArrayList<>();
            resolveDataFeedIds(request, mlMetadata, tasks, startedDatafeeds, stoppingDatafeeds);
            if (startedDatafeeds.isEmpty() && stoppingDatafeeds.isEmpty()) {
                listener.onResponse(new StopDatafeedAction.Response(true));
                return;
            }
            request.setResolvedStartedDatafeedIds(startedDatafeeds.toArray(new String[startedDatafeeds.size()]));

            if (request.isForce()) {
                forceStopDatafeed(request, listener, tasks, startedDatafeeds);
            } else {
                normalStopDatafeed(task, request, listener, tasks, startedDatafeeds, stoppingDatafeeds);
            }
        }
    }

    private void normalStopDatafeed(Task task, StopDatafeedAction.Request request, ActionListener<StopDatafeedAction.Response> listener,
                                    PersistentTasksCustomMetaData tasks,
                                    List<String> startedDatafeeds, List<String> stoppingDatafeeds) {
        Set<String> executorNodes = new HashSet<>();
        for (String datafeedId : startedDatafeeds) {
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask == null || datafeedTask.isAssigned() == false) {
                String message = "Cannot stop datafeed [" + datafeedId + "] because the datafeed does not have an assigned node." +
                        " Use force stop to stop the datafeed";
                listener.onFailure(ExceptionsHelper.conflictStatusException(message));
                return;
            } else {
                executorNodes.add(datafeedTask.getExecutorNode());
            }
        }

        request.setNodes(executorNodes.toArray(new String[executorNodes.size()]));

        // wait for started and stopping datafeeds
        // Map datafeedId -> datafeed task Id.
        List<String> allDataFeedsToWaitFor = Stream.concat(
                startedDatafeeds.stream().map(MlTasks::datafeedTaskId),
                stoppingDatafeeds.stream().map(MlTasks::datafeedTaskId))
                .collect(Collectors.toList());

        ActionListener<StopDatafeedAction.Response> finalListener = ActionListener.wrap(
                r -> waitForDatafeedStopped(allDataFeedsToWaitFor, request, r, listener),
                listener::onFailure);

        super.doExecute(task, request, finalListener);
    }

    private void forceStopDatafeed(final StopDatafeedAction.Request request, final ActionListener<StopDatafeedAction.Response> listener,
                                   PersistentTasksCustomMetaData tasks, final List<String> startedDatafeeds) {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(startedDatafeeds.size());

        for (String datafeedId : startedDatafeeds) {
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask != null) {
                persistentTasksService.sendRemoveRequest(datafeedTask.getId(),
                        new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                        if (counter.incrementAndGet() == startedDatafeeds.size()) {
                            sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final int slot = counter.incrementAndGet();
                        failures.set(slot - 1, e);
                        if (slot == startedDatafeeds.size()) {
                            sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                        }
                    }
                });
            } else {
                String msg = "Requested datafeed [" + request.getDatafeedId() + "] be force-stopped, but " +
                        "datafeed's task could not be found.";
                logger.warn(msg);
                final int slot = counter.incrementAndGet();
                failures.set(slot - 1, new RuntimeException(msg));
                if (slot == startedDatafeeds.size()) {
                    sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                }
            }
        }
    }

    @Override
    protected void taskOperation(StopDatafeedAction.Request request, TransportStartDatafeedAction.DatafeedTask datafeedTask,
                                 ActionListener<StopDatafeedAction.Response> listener) {
        DatafeedState taskState = DatafeedState.STOPPING;
        datafeedTask.updatePersistentTaskState(taskState, ActionListener.wrap(task -> {
                    // we need to fork because we are now on a network threadpool
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }

                        @Override
                        protected void doRun() throws Exception {
                            datafeedTask.stop("stop_datafeed (api)", request.getStopTimeout());
                            listener.onResponse(new StopDatafeedAction.Response(true));
                        }
                    });
                },
                e -> {
                    if (e instanceof ResourceNotFoundException) {
                        // the task has disappeared so must have stopped
                        listener.onResponse(new StopDatafeedAction.Response(true));
                    } else {
                        listener.onFailure(e);
                    }
                }
        ));
    }

    private void sendResponseOrFailure(String datafeedId, ActionListener<StopDatafeedAction.Response> listener,
                                       AtomicArray<Exception> failures) {
        List<Exception> catchedExceptions = failures.asList();
        if (catchedExceptions.size() == 0) {
            listener.onResponse(new StopDatafeedAction.Response(true));
            return;
        }

        String msg = "Failed to stop datafeed [" + datafeedId + "] with [" + catchedExceptions.size()
            + "] failures, rethrowing last, all Exceptions: ["
            + catchedExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
            + "]";

        ElasticsearchException e = new ElasticsearchException(msg,
                catchedExceptions.get(0));
        listener.onFailure(e);
    }

    // Wait for datafeed to be marked as stopped in cluster state, which means the datafeed persistent task has been removed
    // This api returns when task has been cancelled, but that doesn't mean the persistent task has been removed from cluster state,
    // so wait for that to happen here.
    void waitForDatafeedStopped(List<String> datafeedPersistentTaskIds, StopDatafeedAction.Request request,
                                StopDatafeedAction.Response response,
                                ActionListener<StopDatafeedAction.Response> listener) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetaData -> {
            for (String persistentTaskId: datafeedPersistentTaskIds) {
                if (persistentTasksCustomMetaData.getTask(persistentTaskId) != null) {
                    return false;
                }
            }
            return true;
        }, request.getTimeout(), new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean result) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected StopDatafeedAction.Response newResponse(StopDatafeedAction.Request request, List<StopDatafeedAction.Response> tasks,
                                                      List<TaskOperationFailure> taskOperationFailures,
                                                      List<FailedNodeException> failedNodeExceptions) {
        // number of resolved data feeds should be equal to the number of
        // tasks, otherwise something went wrong
        if (request.getResolvedStartedDatafeedIds().length != tasks.size()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(failedNodeExceptions.get(0));
            } else {
                // This can happen we the actual task in the node no longer exists,
                // which means the datafeed(s) have already been closed.
                return new StopDatafeedAction.Response(true);
            }
        }

        return new StopDatafeedAction.Response(tasks.stream().allMatch(StopDatafeedAction.Response::isStopped));
    }

}
