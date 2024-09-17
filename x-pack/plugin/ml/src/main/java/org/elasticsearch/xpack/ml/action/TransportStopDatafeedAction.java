/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ml.utils.ExceptionCollectionHandling.exceptionArrayToStatusException;

public class TransportStopDatafeedAction extends TransportTasksAction<
    TransportStartDatafeedAction.DatafeedTask,
    StopDatafeedAction.Request,
    StopDatafeedAction.Response,
    StopDatafeedAction.Response> {

    private static final int MAX_ATTEMPTS = 10;

    private static final Logger logger = LogManager.getLogger(TransportStopDatafeedAction.class);

    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final AnomalyDetectionAuditor auditor;
    private final OriginSettingClient client;

    @Inject
    public TransportStopDatafeedAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        DatafeedConfigProvider datafeedConfigProvider,
        AnomalyDetectionAuditor auditor,
        Client client
    ) {
        super(
            StopDatafeedAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            StopDatafeedAction.Request::new,
            StopDatafeedAction.Response::new,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
        this.threadPool = Objects.requireNonNull(threadPool);
        this.persistentTasksService = Objects.requireNonNull(persistentTasksService);
        this.datafeedConfigProvider = Objects.requireNonNull(datafeedConfigProvider);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = new OriginSettingClient(client, ML_ORIGIN);
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
    static void sortDatafeedIdsByTaskState(
        Collection<String> expandedDatafeedIds,
        PersistentTasksCustomMetadata tasks,
        List<String> startedDatafeedIds,
        List<String> stoppingDatafeedIds,
        List<String> notStoppedDatafeedIds
    ) {

        for (String expandedDatafeedId : expandedDatafeedIds) {
            addDatafeedTaskIdAccordingToState(
                expandedDatafeedId,
                MlTasks.getDatafeedState(expandedDatafeedId, tasks),
                startedDatafeedIds,
                stoppingDatafeedIds,
                notStoppedDatafeedIds
            );
        }
    }

    private static void addDatafeedTaskIdAccordingToState(
        String datafeedId,
        DatafeedState datafeedState,
        List<String> startedDatafeedIds,
        List<String> stoppingDatafeedIds,
        List<String> notStoppedDatafeedIds
    ) {
        switch (datafeedState) {
            // Treat STARTING like STARTED for stop API behaviour.
            case STARTING:
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
        doExecute(task, request, listener, 1);
    }

    private void doExecute(
        Task task,
        StopDatafeedAction.Request request,
        ActionListener<StopDatafeedAction.Response> listener,
        int attempt
    ) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates stop datafeed to elected master node, so it becomes the coordinating node.
            // See comment in TransportStartDatafeedAction for more information.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(
                        listener,
                        StopDatafeedAction.Response::new,
                        TransportResponseHandler.TRANSPORT_WORKER
                    )
                );
            }
        } else {
            PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            datafeedConfigProvider.expandDatafeedIds(
                request.getDatafeedId(),
                request.allowNoMatch(),
                tasks,
                request.isForce(),
                null,
                ActionListener.wrap(expandedIds -> {
                    List<String> startedDatafeeds = new ArrayList<>();
                    List<String> stoppingDatafeeds = new ArrayList<>();
                    List<String> notStoppedDatafeeds = new ArrayList<>();
                    sortDatafeedIdsByTaskState(expandedIds, tasks, startedDatafeeds, stoppingDatafeeds, notStoppedDatafeeds);
                    if (startedDatafeeds.isEmpty() && stoppingDatafeeds.isEmpty()) {
                        listener.onResponse(new StopDatafeedAction.Response(true));
                        return;
                    }

                    if (request.isForce()) {
                        forceStopDatafeed(request, listener, tasks, nodes, notStoppedDatafeeds);
                    } else {
                        normalStopDatafeed(task, request, listener, tasks, nodes, startedDatafeeds, stoppingDatafeeds, attempt);
                    }
                }, listener::onFailure)
            );
        }
    }

    private void normalStopDatafeed(
        Task task,
        StopDatafeedAction.Request request,
        ActionListener<StopDatafeedAction.Response> listener,
        PersistentTasksCustomMetadata tasks,
        DiscoveryNodes nodes,
        List<String> startedDatafeeds,
        List<String> stoppingDatafeeds,
        int attempt
    ) {
        final Set<String> executorNodes = new HashSet<>();
        final List<String> startedDatafeedsJobs = new ArrayList<>();
        final List<String> resolvedStartedDatafeeds = new ArrayList<>();
        final List<PersistentTasksCustomMetadata.PersistentTask<?>> allDataFeedsToWaitFor = new ArrayList<>();
        for (String datafeedId : startedDatafeeds) {
            PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask == null) {
                // This should not happen, because startedDatafeeds was derived from the same tasks that is passed to this method
                String msg = "Requested datafeed [" + datafeedId + "] be stopped, but datafeed's task could not be found.";
                assert datafeedTask != null : msg;
                logger.error(msg);
            } else if (PersistentTasksClusterService.needsReassignment(datafeedTask.getAssignment(), nodes) == false) {
                startedDatafeedsJobs.add(((StartDatafeedAction.DatafeedParams) datafeedTask.getParams()).getJobId());
                resolvedStartedDatafeeds.add(datafeedId);
                executorNodes.add(datafeedTask.getExecutorNode());
                allDataFeedsToWaitFor.add(datafeedTask);
            } else {
                // This is the easy case - the datafeed is not currently assigned to a valid node,
                // so can be gracefully stopped simply by removing its persistent task. (Usually
                // a graceful stop cannot be achieved by simply removing the persistent task, but
                // if the datafeed has no running code then graceful/forceful are the same.)
                // The listener here doesn't need to call the final listener, as waitForDatafeedStopped()
                // already waits for these persistent tasks to disappear.
                persistentTasksService.sendRemoveRequest(
                    datafeedTask.getId(),
                    null,
                    ActionListener.wrap(
                        r -> auditDatafeedStopped(datafeedTask),
                        e -> logger.error("[" + datafeedId + "] failed to remove task to stop unassigned datafeed", e)
                    )
                );
                allDataFeedsToWaitFor.add(datafeedTask);
            }
        }

        for (String datafeedId : stoppingDatafeeds) {
            PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            assert datafeedTask != null : "Requested datafeed [" + datafeedId + "] be stopped, but datafeed's task could not be found.";
            allDataFeedsToWaitFor.add(datafeedTask);
        }

        request.setResolvedStartedDatafeedIds(resolvedStartedDatafeeds.toArray(new String[0]));
        request.setNodes(executorNodes.toArray(new String[0]));

        final Set<String> movedDatafeeds = ConcurrentCollections.newConcurrentSet();

        ActionListener<StopDatafeedAction.Response> finalListener = ActionListener.wrap(
            response -> waitForDatafeedStopped(allDataFeedsToWaitFor, request, response, ActionListener.wrap(finished -> {
                for (String datafeedId : movedDatafeeds) {
                    PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
                    persistentTasksService.sendRemoveRequest(
                        datafeedTask.getId(),
                        null,
                        ActionListener.wrap(r -> auditDatafeedStopped(datafeedTask), e -> {
                            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                                logger.debug("[{}] relocated datafeed task already removed", datafeedId);
                            } else {
                                logger.error("[" + datafeedId + "] failed to remove task to stop relocated datafeed", e);
                            }
                        })
                    );
                }
                if (startedDatafeedsJobs.isEmpty()) {
                    listener.onResponse(finished);
                    return;
                }
                client.admin()
                    .indices()
                    .prepareRefresh(startedDatafeedsJobs.stream().map(AnomalyDetectorsIndex::jobResultsAliasedName).toArray(String[]::new))
                    .execute(ActionListener.wrap(_unused -> listener.onResponse(finished), ex -> {
                        logger.warn(
                            () -> format(
                                "failed to refresh job [%s] results indices when stopping datafeeds [%s]",
                                startedDatafeedsJobs,
                                startedDatafeeds
                            ),
                            ex
                        );
                        listener.onResponse(finished);
                    }));
            }, listener::onFailure), movedDatafeeds),
            e -> {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof FailedNodeException) {
                    // A node has dropped out of the cluster since we started executing the requests.
                    // Since stopping an already stopped datafeed is not an error we can try again.
                    // The datafeeds that were running on the node that dropped out of the cluster
                    // will just have their persistent tasks cancelled. Datafeeds that were stopped
                    // by the previous attempt will be noops in the subsequent attempt.
                    if (attempt <= MAX_ATTEMPTS) {
                        logger.warn(
                            "Node [{}] failed while processing stop datafeed request - retrying",
                            ((FailedNodeException) unwrapped).nodeId()
                        );
                        doExecute(task, request, listener, attempt + 1);
                    } else {
                        listener.onFailure(e);
                    }
                } else if (unwrapped instanceof RetryStopDatafeedException) {
                    // This is for the case where a local task wasn't yet running at the moment a
                    // request to stop it arrived at its node. This can happen when the cluster
                    // state says a persistent task should be running on a particular node but that
                    // node hasn't yet had time to start the corresponding local task.
                    if (attempt <= MAX_ATTEMPTS) {
                        logger.info(
                            "Insufficient responses while processing stop datafeed request [{}] - retrying",
                            unwrapped.getMessage()
                        );
                        // Unlike the failed node case above, in this case we should wait a little
                        // before retrying because we need to allow time for the local task to
                        // start on the node it's supposed to be running on.
                        threadPool.schedule(
                            () -> doExecute(task, request, listener, attempt + 1),
                            TimeValue.timeValueMillis(100L * attempt),
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        );
                    } else {
                        listener.onFailure(
                            ExceptionsHelper.serverError(
                                "Failed to stop datafeed ["
                                    + request.getDatafeedId()
                                    + "] after "
                                    + MAX_ATTEMPTS
                                    + " due to inconsistencies between local and persistent tasks within the cluster"
                            )
                        );
                    }
                } else {
                    listener.onFailure(e);
                }
            }
        );

        super.doExecute(task, request, finalListener);
    }

    private void auditDatafeedStopped(PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask) {
        @SuppressWarnings("unchecked")
        String jobId = ((PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>) datafeedTask).getParams()
            .getJobId();
        auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STOPPED));
    }

    private void forceStopDatafeed(
        final StopDatafeedAction.Request request,
        final ActionListener<StopDatafeedAction.Response> listener,
        PersistentTasksCustomMetadata tasks,
        DiscoveryNodes nodes,
        final List<String> notStoppedDatafeeds
    ) {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(notStoppedDatafeeds.size());

        for (String datafeedId : notStoppedDatafeeds) {
            PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            if (datafeedTask != null) {
                persistentTasksService.sendRemoveRequest(datafeedTask.getId(), null, ActionListener.wrap(persistentTask -> {
                    // For force stop, only audit here if the datafeed was unassigned at the time of the stop, hence inactive.
                    // If the datafeed was active then it audits itself on being cancelled.
                    if (PersistentTasksClusterService.needsReassignment(datafeedTask.getAssignment(), nodes)) {
                        auditDatafeedStopped(datafeedTask);
                    }
                    if (counter.incrementAndGet() == notStoppedDatafeeds.size()) {
                        sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                    }
                }, e -> {
                    final int slot = counter.incrementAndGet();
                    // We validated that the datafeed names supplied in the request existed when we started processing the action.
                    // If the related tasks don't exist at this point then they must have been stopped by a simultaneous stop
                    // request.
                    // This is not an error.
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException == false) {
                        failures.set(slot - 1, e);
                    }
                    if (slot == notStoppedDatafeeds.size()) {
                        sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                    }
                }));
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
    protected void taskOperation(
        CancellableTask actionTask,
        StopDatafeedAction.Request request,
        TransportStartDatafeedAction.DatafeedTask datafeedTask,
        ActionListener<StopDatafeedAction.Response> listener
    ) {
        DatafeedState taskState = DatafeedState.STOPPING;
        datafeedTask.updatePersistentTaskState(taskState, ActionListener.wrap(task -> {
            // we need to fork because we are now on a network threadpool
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    // We validated that the datafeed names supplied in the request existed when we started processing the action.
                    // If the related task for one of them doesn't exist at this point then it must have been removed by a
                    // simultaneous force stop request. This is not an error.
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
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                // the task has disappeared so must have stopped
                listener.onResponse(new StopDatafeedAction.Response(true));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private static void sendResponseOrFailure(
        String datafeedId,
        ActionListener<StopDatafeedAction.Response> listener,
        AtomicArray<Exception> failures
    ) {
        List<Exception> caughtExceptions = failures.asList();
        if (caughtExceptions.isEmpty()) {
            listener.onResponse(new StopDatafeedAction.Response(true));
            return;
        }

        String msg = "Failed to stop datafeed ["
            + datafeedId
            + "] with ["
            + caughtExceptions.size()
            + "] failures, rethrowing first. All Exceptions: ["
            + caughtExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
            + "]";

        ElasticsearchStatusException e = exceptionArrayToStatusException(failures, msg);
        listener.onFailure(e);
    }

    /**
     * Wait for datafeed to be marked as stopped in cluster state, which means the datafeed persistent task has been removed.
     * This api returns when task has been cancelled, but that doesn't mean the persistent task has been removed from cluster state,
     * so wait for that to happen here.
     *
     * Since the stop datafeed action consists of a chain of async callbacks, it's possible that datafeeds have moved nodes since we
     * decided what to do with them at the beginning of the chain.  We cannot simply wait for these, as the request to stop them will
     * have been sent to the wrong node and ignored there, so we'll just spin until the timeout expires.
     */
    void waitForDatafeedStopped(
        List<PersistentTasksCustomMetadata.PersistentTask<?>> datafeedPersistentTasks,
        StopDatafeedAction.Request request,
        StopDatafeedAction.Response response,
        ActionListener<StopDatafeedAction.Response> listener,
        Set<String> movedDatafeeds
    ) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetadata -> {
            for (PersistentTasksCustomMetadata.PersistentTask<?> originalPersistentTask : datafeedPersistentTasks) {
                String originalPersistentTaskId = originalPersistentTask.getId();
                PersistentTasksCustomMetadata.PersistentTask<?> currentPersistentTask = persistentTasksCustomMetadata.getTask(
                    originalPersistentTaskId
                );
                if (currentPersistentTask != null) {
                    if (Objects.equals(originalPersistentTask.getExecutorNode(), currentPersistentTask.getExecutorNode())
                        && originalPersistentTask.getAllocationId() == currentPersistentTask.getAllocationId()) {
                        return false;
                    }
                    StartDatafeedAction.DatafeedParams params = (StartDatafeedAction.DatafeedParams) originalPersistentTask.getParams();
                    if (movedDatafeeds.add(params.getDatafeedId())) {
                        logger.info("Datafeed [{}] changed assignment while waiting for it to be stopped", params.getDatafeedId());
                    }
                }
            }
            return true;
        }, request.getTimeout(), listener.safeMap(result -> response));
    }

    @Override
    protected StopDatafeedAction.Response newResponse(
        StopDatafeedAction.Request request,
        List<StopDatafeedAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        // number of resolved (i.e. running on a node) started data feeds should be equal to the number of
        // tasks, otherwise something went wrong
        if (request.getResolvedStartedDatafeedIds().length != tasks.size()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw ExceptionsHelper.taskOperationFailureToStatusException(taskOperationFailures.get(0));
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw failedNodeExceptions.get(0);
            } else {
                // This can happen when the local task in the node no longer exists,
                // which means the datafeed(s) have already been stopped. It can
                // also happen if the local task hadn't yet been created when the
                // stop request hit the executor node. In this second case we need
                // to retry, otherwise the wait for completion will wait until it
                // times out. We cannot tell which case it is, but it doesn't hurt
                // to retry in both cases since stopping a stopped datafeed is a
                // no-op.
                throw new RetryStopDatafeedException(request.getResolvedStartedDatafeedIds().length, tasks.size());
            }
        }

        return new StopDatafeedAction.Response(tasks.stream().allMatch(StopDatafeedAction.Response::isStopped));
    }

    /**
     * A special exception to indicate that we should retry stopping the datafeeds.
     * This exception is not transportable, so should only be thrown in situations
     * where it will be caught on the same node.
     */
    static class RetryStopDatafeedException extends RuntimeException {

        RetryStopDatafeedException(int numResponsesExpected, int numResponsesReceived) {
            super("expected " + numResponsesExpected + " responses, got " + numResponsesReceived);
        }
    }
}
