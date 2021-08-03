/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformNodeAssignments;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.transform.TransformMessages.CANNOT_STOP_FAILED_TRANSFORM;

public class TransportStopTransformAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopTransformAction.class);

    private final ThreadPool threadPool;
    private final TransformConfigManager transformConfigManager;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportStopTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        PersistentTasksService persistentTasksService,
        TransformServices transformServices,
        Client client
    ) {
        this(
            StopTransformAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            threadPool,
            persistentTasksService,
            transformServices,
            client
        );
    }

    protected TransportStopTransformAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        PersistentTasksService persistentTasksService,
        TransformServices transformServices,
        Client client
    ) {
        super(name, clusterService, transportService, actionFilters, Request::new, Response::new, Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.transformConfigManager = transformServices.getConfigManager();
        this.persistentTasksService = persistentTasksService;
    }

    static void validateTaskState(ClusterState state, List<String> transformIds, boolean isForce) {
        PersistentTasksCustomMetadata tasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (isForce == false && tasks != null) {
            List<String> failedTasks = new ArrayList<>();
            List<String> failedReasons = new ArrayList<>();
            for (String transformId : transformIds) {
                PersistentTasksCustomMetadata.PersistentTask<?> dfTask = tasks.getTask(transformId);
                if (dfTask != null
                    && dfTask.getState() instanceof TransformState
                    && ((TransformState) dfTask.getState()).getTaskState() == TransformTaskState.FAILED) {
                    failedTasks.add(transformId);
                    failedReasons.add(((TransformState) dfTask.getState()).getReason());
                }
            }
            if (failedTasks.isEmpty() == false) {
                String msg = failedTasks.size() == 1
                    ? TransformMessages.getMessage(CANNOT_STOP_FAILED_TRANSFORM, failedTasks.get(0), failedReasons.get(0))
                    : "Unable to stop transforms. The following transforms are in a failed state "
                        + failedTasks
                        + " with reasons "
                        + failedReasons
                        + ". Use force stop to stop the transforms.";
                throw new ElasticsearchStatusException(msg, RestStatus.CONFLICT);
            }
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates stop transform to elected master node so it becomes the coordinating node.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(listener, Response::new)
                );
            }
        } else {
            TransformNodes.warnIfNoTransformNodes(state);

            final ActionListener<Response> finalListener;
            if (request.waitForCompletion()) {
                finalListener = waitForStopListener(request, listener);
            } else {
                finalListener = listener;
            }

            transformConfigManager.expandTransformIds(
                request.getId(),
                new PageParams(0, 10_000),
                request.isAllowNoMatch(),
                ActionListener.wrap(hitsAndIds -> {
                    validateTaskState(state, hitsAndIds.v2().v1(), request.isForce());
                    request.setExpandedIds(new HashSet<>(hitsAndIds.v2().v1()));
                    final TransformNodeAssignments transformNodeAssignments =
                        TransformNodes.transformTaskNodes(hitsAndIds.v2().v1(), state);

                    final ActionListener<Response> doExecuteListener;
                    if (transformNodeAssignments.getWaitingForAssignment().size() > 0) {
                        doExecuteListener = cancelTransformTasksWithNoAssignment(finalListener, transformNodeAssignments);
                    } else {
                        doExecuteListener = finalListener;
                    }

                    if (transformNodeAssignments.getExecutorNodes().size() > 0) {
                        request.setNodes(transformNodeAssignments.getExecutorNodes().toArray(new String[0]));
                        super.doExecute(task, request, doExecuteListener);
                    } else {
                        doExecuteListener.onResponse(new Response(true));
                    }
                }, e -> {
                    if (e instanceof ResourceNotFoundException) {
                        final TransformNodeAssignments transformNodeAssignments = TransformNodes.findPersistentTasks(
                            request.getId(),
                            state
                        );

                        if (transformNodeAssignments.getAssigned().isEmpty()
                            && transformNodeAssignments.getWaitingForAssignment().isEmpty()) {
                            listener.onFailure(e);
                            // found transforms without a config
                        } else if (request.isForce()) {
                            final ActionListener<Response> doExecuteListener;

                            if (transformNodeAssignments.getWaitingForAssignment().size() > 0) {
                                doExecuteListener = cancelTransformTasksWithNoAssignment(finalListener, transformNodeAssignments);
                            } else {
                                doExecuteListener = finalListener;
                            }

                            if (transformNodeAssignments.getExecutorNodes().size() > 0) {
                                request.setExpandedIds(transformNodeAssignments.getAssigned());
                                request.setNodes(transformNodeAssignments.getExecutorNodes().toArray(new String[0]));
                                super.doExecute(task, request, doExecuteListener);
                            } else {
                                doExecuteListener.onResponse(new Response(true));
                            }
                        } else {
                            Set<String> transformsWithoutConfig = Stream.concat(
                                transformNodeAssignments.getAssigned().stream(),
                                transformNodeAssignments.getWaitingForAssignment().stream()
                            ).collect(Collectors.toSet());

                            listener.onFailure(
                                new ElasticsearchStatusException(
                                    TransformMessages.getMessage(
                                        TransformMessages.REST_STOP_TRANSFORM_WITHOUT_CONFIG,
                                        Strings.collectionToCommaDelimitedString(transformsWithoutConfig)
                                    ),
                                    RestStatus.CONFLICT
                                )
                            );
                        }
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        }
    }

    @Override
    protected void taskOperation(Request request, TransformTask transformTask, ActionListener<Response> listener) {

        Set<String> ids = request.getExpandedIds();
        if (ids == null) {
            listener.onFailure(new IllegalStateException("Request does not have expandedIds set"));
            return;
        }

        if (ids.contains(transformTask.getTransformId())) {
            transformTask.setShouldStopAtCheckpoint(request.isWaitForCheckpoint(), ActionListener.wrap(r -> {
                try {
                    transformTask.stop(request.isForce(), request.isWaitForCheckpoint());
                    listener.onResponse(new Response(true));
                } catch (ElasticsearchException ex) {
                    listener.onFailure(ex);
                }
            },
                e -> listener.onFailure(
                    new ElasticsearchStatusException(
                        "Failed to update transform task [{}] state value should_stop_at_checkpoint from [{}] to [{}]",
                        RestStatus.CONFLICT,
                        e,
                        transformTask.getTransformId(),
                        transformTask.getState().shouldStopAtNextCheckpoint(),
                        request.isWaitForCheckpoint()
                    )
                )
            ));
        } else {
            listener.onFailure(
                new RuntimeException(
                    "ID of transform task [" + transformTask.getTransformId() + "] does not match request's ID [" + request.getId() + "]"
                )
            );
        }
    }

    @Override
    protected StopTransformAction.Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {

        if (taskOperationFailures.isEmpty() == false || failedNodeExceptions.isEmpty() == false) {
            return new Response(taskOperationFailures, failedNodeExceptions, false);
        }

        // if tasks is empty allMatch is 'vacuously satisfied'
        return new Response(tasks.stream().allMatch(Response::isAcknowledged));
    }

    private ActionListener<Response> waitForStopListener(Request request, ActionListener<Response> listener) {

        ActionListener<Response> onStopListener = ActionListener.wrap(
            waitResponse -> transformConfigManager.refresh(ActionListener.wrap(r -> listener.onResponse(waitResponse), e -> {
                if ((ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) == false) {
                    logger.warn("Could not refresh state, state information might be outdated", e);
                }
                listener.onResponse(waitResponse);
            })),
            listener::onFailure
        );
        return ActionListener.wrap(
            response -> {
                // If there were failures attempting to stop the tasks, we don't know if they will actually stop.
                // It is better to respond to the user now than allow for the persistent task waiting to timeout
                if (response.getTaskFailures().isEmpty() == false || response.getNodeFailures().isEmpty() == false) {
                    RestStatus status = firstNotOKStatus(response.getTaskFailures(), response.getNodeFailures());
                    listener.onFailure(buildException(response.getTaskFailures(), response.getNodeFailures(), status));
                    return;
                }
                // Wait until the persistent task is stopped
                // Switch over to Generic threadpool so we don't block the network thread
                threadPool.generic()
                    .execute(
                        () -> waitForTransformStopped(request.getExpandedIds(), request.getTimeout(), request.isForce(), onStopListener)
                    );
            },
            listener::onFailure
        );
    }

    static ElasticsearchStatusException buildException(
        List<TaskOperationFailure> taskOperationFailures,
        List<ElasticsearchException> elasticsearchExceptions,
        RestStatus status
    ) {
        List<Exception> exceptions = Stream.concat(
            taskOperationFailures.stream().map(TaskOperationFailure::getCause),
            elasticsearchExceptions.stream()
        ).collect(Collectors.toList());

        ElasticsearchStatusException elasticsearchStatusException = new ElasticsearchStatusException(
            exceptions.get(0).getMessage(),
            status
        );

        for (int i = 1; i < exceptions.size(); i++) {
            elasticsearchStatusException.addSuppressed(exceptions.get(i));
        }
        return elasticsearchStatusException;
    }

    static RestStatus firstNotOKStatus(List<TaskOperationFailure> taskOperationFailures, List<ElasticsearchException> exceptions) {
        RestStatus status = RestStatus.OK;

        for (TaskOperationFailure taskOperationFailure : taskOperationFailures) {
            status = taskOperationFailure.getStatus();
            if (RestStatus.OK.equals(status) == false) {
                break;
            }
        }
        if (status == RestStatus.OK) {
            for (ElasticsearchException exception : exceptions) {
                // As it stands right now, this will ALWAYS be INTERNAL_SERVER_ERROR.
                // FailedNodeException does not overwrite the `status()` method and the logic in ElasticsearchException
                // Just returns an INTERNAL_SERVER_ERROR
                status = exception.status();
                if (RestStatus.OK.equals(status) == false) {
                    break;
                }
            }
        }
        // If all the previous exceptions don't have a valid status, we have an unknown error.
        return status == RestStatus.OK ? RestStatus.INTERNAL_SERVER_ERROR : status;
    }

    private void waitForTransformStopped(
        Set<String> persistentTaskIds,
        TimeValue timeout,
        boolean force,
        ActionListener<Response> listener
    ) {
        // This map is accessed in the predicate and the listener callbacks
        final Map<String, ElasticsearchException> exceptions = new ConcurrentHashMap<>();
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetadata -> {
            if (persistentTasksCustomMetadata == null) {
                return true;
            }
            for (String persistentTaskId : persistentTaskIds) {
                PersistentTasksCustomMetadata.PersistentTask<?> transformsTask = persistentTasksCustomMetadata.getTask(persistentTaskId);
                // Either the task has successfully stopped or we have seen that it has failed
                if (transformsTask == null || exceptions.containsKey(persistentTaskId)) {
                    continue;
                }

                // If force is true, then it should eventually go away, don't add it to the collection of failures.
                TransformState taskState = (TransformState) transformsTask.getState();
                if (force == false && taskState != null && taskState.getTaskState() == TransformTaskState.FAILED) {
                    exceptions.put(
                        persistentTaskId,
                        new ElasticsearchStatusException(
                            TransformMessages.getMessage(CANNOT_STOP_FAILED_TRANSFORM, persistentTaskId, taskState.getReason()),
                            RestStatus.CONFLICT
                        )
                    );

                    // If all the tasks are now flagged as failed, do not wait for another ClusterState update.
                    // Return to the caller as soon as possible
                    return persistentTasksCustomMetadata.tasks().stream().allMatch(p -> exceptions.containsKey(p.getId()));
                }
                return false;
            }
            return true;
        }, timeout, ActionListener.wrap(r -> {
            // No exceptions AND the tasks have gone away
            if (exceptions.isEmpty()) {
                listener.onResponse(new Response(Boolean.TRUE));
                return;
            }

            // We are only stopping one task, so if there is a failure, it is the only one
            if (persistentTaskIds.size() == 1) {
                listener.onFailure(exceptions.get(persistentTaskIds.iterator().next()));
                return;
            }

            Set<String> stoppedTasks = new HashSet<>(persistentTaskIds);
            stoppedTasks.removeAll(exceptions.keySet());
            String message = stoppedTasks.isEmpty()
                ? "Could not stop any of the tasks as all were failed. Use force stop to stop the transforms."
                : LoggerMessageFormat.format(
                    "Successfully stopped [{}] transforms. "
                        + "Could not stop the transforms {} as they were failed. Use force stop to stop the transforms.",
                    stoppedTasks.size(),
                    exceptions.keySet()
                );

            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.CONFLICT));
        }, e -> {
            // waitForPersistentTasksCondition throws a IllegalStateException on timeout
            if (e instanceof IllegalStateException && e.getMessage().startsWith("Timed out")) {
                PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
                    .metadata()
                    .custom(PersistentTasksCustomMetadata.TYPE);

                if (persistentTasksCustomMetadata == null) {
                    listener.onResponse(new Response(Boolean.TRUE));
                    return;
                }

                // collect which tasks are still running
                Set<String> stillRunningTasks = new HashSet<>();
                for (String persistentTaskId : persistentTaskIds) {
                    if (persistentTasksCustomMetadata.getTask(persistentTaskId) != null) {
                        stillRunningTasks.add(persistentTaskId);
                    }
                }

                if (stillRunningTasks.isEmpty()) {
                    // should not happen
                    listener.onResponse(new Response(Boolean.TRUE));
                    return;
                } else {
                    StringBuilder message = new StringBuilder();
                    if (persistentTaskIds.size() - stillRunningTasks.size() - exceptions.size() > 0) {
                        message.append("Successfully stopped [");
                        message.append(persistentTaskIds.size() - stillRunningTasks.size() - exceptions.size());
                        message.append("] transforms. ");
                    }

                    if (exceptions.size() > 0) {
                        message.append("Could not stop the transforms ");
                        message.append(exceptions.keySet());
                        message.append(" as they were failed. Use force stop to stop the transforms. ");
                    }

                    if (stillRunningTasks.size() > 0) {
                        message.append("Could not stop the transforms ");
                        message.append(stillRunningTasks);
                        message.append(" as they timed out [");
                        message.append(timeout.toString());
                        message.append("].");
                    }

                    listener.onFailure(new ElasticsearchStatusException(message.toString(), RestStatus.REQUEST_TIMEOUT));
                    return;
                }
            }
            listener.onFailure(e);
        }));
    }

    private ActionListener<Response> cancelTransformTasksWithNoAssignment(
        final ActionListener<Response> finalListener,
        final TransformNodeAssignments transformNodeAssignments
    ) {
        final ActionListener<Response> doExecuteListener = ActionListener.wrap(response -> {
            GroupedActionListener<PersistentTask<?>> groupedListener = new GroupedActionListener<>(
                ActionListener.wrap(r -> { finalListener.onResponse(response); }, finalListener::onFailure),
                transformNodeAssignments.getWaitingForAssignment().size()
            );

            for (String unassignedTaskId : transformNodeAssignments.getWaitingForAssignment()) {
                persistentTasksService.sendRemoveRequest(unassignedTaskId, groupedListener);
            }

        }, e -> {
            GroupedActionListener<PersistentTask<?>> groupedListener = new GroupedActionListener<>(
                ActionListener.wrap(r -> { finalListener.onFailure(e); }, finalListener::onFailure),
                transformNodeAssignments.getWaitingForAssignment().size()
            );

            for (String unassignedTaskId : transformNodeAssignments.getWaitingForAssignment()) {
                persistentTasksService.sendRemoveRequest(unassignedTaskId, groupedListener);
            }
        });
        return doExecuteListener;
    }
}
