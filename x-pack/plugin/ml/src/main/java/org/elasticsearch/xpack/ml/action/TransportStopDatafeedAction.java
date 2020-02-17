/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportStopDatafeedAction extends TransportTasksAction<TransportStartDatafeedAction.DatafeedTask, StopDatafeedAction.Request,
        StopDatafeedAction.Response, StopDatafeedAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopDatafeedAction.class);

    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;
    private final DatafeedConfigProvider datafeedConfigProvider;

    @Inject
    public TransportStopDatafeedAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                       ClusterService clusterService, PersistentTasksService persistentTasksService,
                                       DatafeedConfigProvider datafeedConfigProvider) {
        super(StopDatafeedAction.NAME, clusterService, transportService, actionFilters, StopDatafeedAction.Request::new,
            StopDatafeedAction.Response::new, StopDatafeedAction.Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
        this.datafeedConfigProvider = datafeedConfigProvider;

    }

    /**
     * Sort the datafeed IDs the their task state and add to one
     * of the list arguments depending on the state.
     *
     * @param expandedDatafeedIds The expanded set of IDs
     * @param tasks Persistent task meta data
     * @param startedDatafeedIds Started datafeed ids are added to this list
     * @param stoppingDatafeedIds Stopping datafeed ids are added to this list
     * @param notStoppedDatafeedIds Datafeed ids are added to this list for all datafeeds that are not stopped
     */
    static void sortDatafeedIdsByTaskState(Collection<String> expandedDatafeedIds,
                                           PersistentTasksCustomMetaData tasks,
                                           List<String> startedDatafeedIds,
                                           List<String> stoppingDatafeedIds,
                                           List<String> notStoppedDatafeedIds) {

        for (String expandedDatafeedId : expandedDatafeedIds) {
            addDatafeedTaskIdAccordingToState(expandedDatafeedId, MlTasks.getDatafeedState(expandedDatafeedId, tasks),
                    startedDatafeedIds, stoppingDatafeedIds, notStoppedDatafeedIds);
        }
    }

    private static void addDatafeedTaskIdAccordingToState(String datafeedId,
                                                          DatafeedState datafeedState,
                                                          List<String> startedDatafeedIds,
                                                          List<String> stoppingDatafeedIds,
                                                          List<String> notStoppedDatafeedIds) {
        switch (datafeedState) {
            case STARTING:
                // The STARTING state is not used anywhere at present, so this should never happen.
                // At present datafeeds that have a persistent task that hasn't yet been assigned
                // a state are reported as STOPPED (which is not great).  It could be considered a
                // breaking change to introduce the STARTING state though, so let's aim to do it in
                // version 8.  Also consider treating STARTING like STARTED for stop API behaviour.
                notStoppedDatafeedIds.add(datafeedId);
                break;
            case STARTED:
                startedDatafeedIds.add(datafeedId);
                notStoppedDatafeedIds.add(datafeedId);
                break;
            case STOPPED:
                break;
            case STOPPING:
                stoppingDatafeedIds.add(datafeedId);
                notStoppedDatafeedIds.add(datafeedId);
                break;
            default:
                assert false : "Unexpected datafeed state " + datafeedState;
                break;
        }
    }

    @Override
    protected void doExecute(Task task, StopDatafeedAction.Request request, ActionListener<StopDatafeedAction.Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates stop datafeed to elected master node, so it becomes the coordinating node.
            // See comment in TransportStartDatafeedAction for more information.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, StopDatafeedAction.Response::new));
            }
        } else {
            datafeedConfigProvider.expandDatafeedIds(request.getDatafeedId(), request.allowNoDatafeeds(), ActionListener.wrap(
                    expandedIds -> {
                        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

                        List<String> startedDatafeeds = new ArrayList<>();
                        List<String> stoppingDatafeeds = new ArrayList<>();
                        List<String> notStoppedDatafeeds = new ArrayList<>();
                        sortDatafeedIdsByTaskState(expandedIds, tasks, startedDatafeeds, stoppingDatafeeds, notStoppedDatafeeds);
                        if (startedDatafeeds.isEmpty() && stoppingDatafeeds.isEmpty()) {
                            listener.onResponse(new StopDatafeedAction.Response(true));
                            return;
                        }
                        request.setResolvedStartedDatafeedIds(startedDatafeeds.toArray(new String[startedDatafeeds.size()]));

                        if (request.isForce()) {
                            forceStopDatafeed(request, listener, tasks, notStoppedDatafeeds);
                        } else {
                            normalStopDatafeed(task, request, listener, tasks, nodes, startedDatafeeds, stoppingDatafeeds);
                        }
                    },
                    listener::onFailure
            ));
        }
    }

    private void normalStopDatafeed(Task task, StopDatafeedAction.Request request, ActionListener<StopDatafeedAction.Response> listener,
                                    PersistentTasksCustomMetaData tasks, DiscoveryNodes nodes,
                                    List<String> startedDatafeeds, List<String> stoppingDatafeeds) {
        final Set<String> executorNodes = new HashSet<>();
        for (String datafeedId : startedDatafeeds) {
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask == null) {
                // This should not happen, because startedDatafeeds was derived from the same tasks that is passed to this method
                String msg = "Requested datafeed [" + datafeedId + "] be stopped, but datafeed's task could not be found.";
                assert datafeedTask != null : msg;
                logger.error(msg);
            } else if (PersistentTasksClusterService.needsReassignment(datafeedTask.getAssignment(), nodes) == false) {
                executorNodes.add(datafeedTask.getExecutorNode());
            } else {
                // This is the easy case - the datafeed is not currently assigned to a valid node,
                // so can be gracefully stopped simply by removing its persistent task.  (Usually
                // a graceful stop cannot be achieved by simply removing the persistent task, but
                // if the datafeed has no running code then graceful/forceful are the same.)
                // The listener here can be a no-op, as waitForDatafeedStopped() already waits for
                // these persistent tasks to disappear.
                persistentTasksService.sendRemoveRequest(datafeedTask.getId(), ActionListener.wrap(r -> {}, e -> {}));
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
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof FailedNodeException) {
                        // A node has dropped out of the cluster since we started executing the requests.
                        // Since stopping an already stopped datafeed is not an error we can try again.
                        // The datafeeds that were running on the node that dropped out of the cluster
                        // will just have their persistent tasks cancelled.  Datafeeds that were stopped
                        // by the previous attempt will be noops in the subsequent attempt.
                        doExecute(task, request, listener);
                    } else {
                        listener.onFailure(e);
                    }
                });

        super.doExecute(task, request, finalListener);
    }

    private void forceStopDatafeed(final StopDatafeedAction.Request request, final ActionListener<StopDatafeedAction.Response> listener,
                                   PersistentTasksCustomMetaData tasks, final List<String> notStoppedDatafeeds) {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(notStoppedDatafeeds.size());

        for (String datafeedId : notStoppedDatafeeds) {
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask != null) {
                persistentTasksService.sendRemoveRequest(datafeedTask.getId(),
                        new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                        if (counter.incrementAndGet() == notStoppedDatafeeds.size()) {
                            sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final int slot = counter.incrementAndGet();
                        // We validated that the datafeed names supplied in the request existed when we started processing the action.
                        // If the related tasks don't exist at this point then they must have been stopped by a simultaneous stop request.
                        // This is not an error.
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException == false) {
                            failures.set(slot - 1, e);
                        }
                        if (slot == notStoppedDatafeeds.size()) {
                            sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                        }
                    }
                });
            } else {
                // This should not happen, because startedDatafeeds and stoppingDatafeeds
                // were derived from the same tasks that were passed to this method
                String msg = "Requested datafeed [" + datafeedId + "] be force-stopped, but datafeed's task could not be found.";
                assert datafeedTask != null : msg;
                logger.error(msg);
                final int slot = counter.incrementAndGet();
                failures.set(slot - 1, new RuntimeException(msg));
                if (slot == notStoppedDatafeeds.size()) {
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
                            // We validated that the datafeed names supplied in the request existed when we started processing the action.
                            // If the related task for one of them doesn't exist at this point then it must have been removed by a
                            // simultaneous force stop request.  This is not an error.
                            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                                listener.onResponse(new StopDatafeedAction.Response(true));
                            } else {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        protected void doRun() {
                            datafeedTask.stop("stop_datafeed (api)", request.getStopTimeout());
                            listener.onResponse(new StopDatafeedAction.Response(true));
                        }
                    });
                },
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
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
        List<Exception> caughtExceptions = failures.asList();
        if (caughtExceptions.size() == 0) {
            listener.onResponse(new StopDatafeedAction.Response(true));
            return;
        }

        String msg = "Failed to stop datafeed [" + datafeedId + "] with [" + caughtExceptions.size()
            + "] failures, rethrowing last, all Exceptions: ["
            + caughtExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
            + "]";

        ElasticsearchException e = new ElasticsearchException(msg, caughtExceptions.get(0));
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
                // This can happen when the actual task in the node no longer exists,
                // which means the datafeed(s) have already been stopped.
                return new StopDatafeedAction.Response(true);
            }
        }

        return new StopDatafeedAction.Response(tasks.stream().allMatch(StopDatafeedAction.Response::isStopped));
    }

}
