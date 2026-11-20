/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Deletes a stored ES|QL stream: cancels its query if it is still running on this node, then removes what is stored. The
 * counterpart of {@code DeleteAsyncResultsService}, with the same order (cancel first, so the writer stops before its rows are
 * removed) and the same access rule (the creator, or a user with the {@code cancel_task} privilege).
 * <p>
 * Two things differ from the async service:
 * <ul>
 *   <li>Everything that is not a stream the caller may delete is a {@link ResourceNotFoundException} with the id as its message:
 *   a malformed id, an unknown id, a stream of another user.</li>
 *   <li>A failure of the storage or of the cancellation is passed on (a {@code 500}). The async service answers {@code 404} to
 *   every failure, which tells a client the result is gone while it may still be stored.</li>
 *   <li>A query that something else has already cancelled is waited for too. The async service deletes at once in that case,
 *   which is fine for a result that is written once at the end. A stream is written page by page while its query runs, and a
 *   cancelled query may not have stopped yet.</li>
 * </ul>
 */
public class DeleteStreamResultsService {

    private static final Logger logger = LogManager.getLogger(DeleteStreamResultsService.class);

    static final String CANCEL_REASON = "cancelled by user";

    private final StreamStorage storage;
    private final StreamSearchSecurity security;
    private final TaskManager taskManager;
    private final Supplier<String> localNodeId;

    /**
     * @param localNodeId the id of this node. A task is cancelled only if the stream id names this node, because a task number
     *                    means nothing on another node
     */
    public DeleteStreamResultsService(
        StreamStorage storage,
        StreamSearchSecurity security,
        TaskManager taskManager,
        Supplier<String> localNodeId
    ) {
        this.storage = Objects.requireNonNull(storage);
        this.security = Objects.requireNonNull(security);
        this.taskManager = Objects.requireNonNull(taskManager);
        this.localNodeId = Objects.requireNonNull(localNodeId);
    }

    /**
     * Deletes the stream named by the request. The listener gets {@link AcknowledgedResponse#TRUE}, a
     * {@link ResourceNotFoundException} when there is no such stream for the caller, or the failure of the storage or of the
     * cancellation.
     */
    public void deleteStream(DeleteStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        final StreamId streamId;
        try {
            streamId = StreamId.decode(request.id());
        } catch (IllegalArgumentException e) {
            listener.onFailure(new ResourceNotFoundException(request.id()));
            return;
        }
        deleteStream(streamId, listener);
    }

    /**
     * Deletes the stream with the given id, for a caller that has already decoded the id of the request, as the transport action
     * does to find the node of the query.
     */
    public void deleteStream(StreamId streamId, ActionListener<AcknowledgedResponse> listener) {
        final String id = streamId.encoded();
        security.currentUserHasCancelTaskPrivilege(
            hasCancelTaskPrivilege -> call(
                (ActionListener<StreamMetadata> l) -> storage.getMetadata(id, l),
                metadata -> cancelThenDelete(streamId, metadata, hasCancelTaskPrivilege, listener),
                e -> fail(id, "read", e, listener)
            )
        );
    }

    private void cancelThenDelete(
        StreamId streamId,
        @Nullable StreamMetadata metadata,
        boolean hasCancelTaskPrivilege,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String id = streamId.encoded();
        if (metadata == null) {
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        if (hasCancelTaskPrivilege == false) {
            try {
                // Not checking the expiry: a stream that has expired but is still stored can be deleted, as an async result can.
                security.getStreamAndCheckAuthentication(id, metadata);
            } catch (Exception e) {
                // Not found for a stream of another user. Anything else is the stored authentication that could not be read.
                fail(id, "check the access to", e, listener);
                return;
            }
        }
        final CancellableTask task = findQueryTask(streamId);
        if (task != null) {
            // Wait for the cancellation before deleting, so the query has stopped writing when its rows are removed. This is done
            // for a task that is already cancelled as well (an earlier delete, a cancellation of the task, the closed connection of
            // the POST): it may still be running. Cancelling again changes nothing, and the listener is called when the task ends.
            call(
                (ActionListener<Void> l) -> taskManager.cancelTaskAndDescendants(task, CANCEL_REASON, true, l),
                ignored -> deleteFromStorage(id, true, listener),
                e -> fail(id, "cancel the query of", e, listener)
            );
        } else {
            deleteFromStorage(id, false, listener);
        }
    }

    /**
     * The running query of the stream, or {@code null}.
     * <p>
     * TODO: match the task by its {@link StreamId}, before a storage that outlives the node is used.
     * <p>
     * Today the task is matched by its number and its action only, because the task of the stream POST does not hold a stream id.
     * Task numbers start again when a node restarts while the node id stays, so an id stored before the restart can name the query
     * of another stream, of another user, and deleting the old stream would cancel it. It cannot happen while the storage dies with
     * the node.
     * <p>
     * What the stream POST has to do, as the async query does in {@code EsqlQueryRequest#createTask}:
     * <ul>
     *   <li>create the {@link StreamId} on the server when the task is created, with a new random key, and keep it on the task</li>
     *   <li>return and store the stream under that same {@link StreamId#encoded()} string</li>
     * </ul>
     * Then replace the check of the action below with {@code task.streamId().equals(streamId)}, the guard of
     * {@code AsyncTaskIndexService#getTask}. The whole id must be compared: the task ids are equal by construction, the key is the
     * part that differs. Keep the check of the node. {@code DeleteStreamResultsServiceTests#testTaskOfAnotherStreamIsCancelled} pins
     * today's behaviour and has to be turned around.
     */
    @Nullable
    private CancellableTask findQueryTask(StreamId streamId) {
        if (streamId.taskId().getNodeId().equals(localNodeId.get()) == false) {
            // The query ran on a node that has left the cluster, there is nothing to cancel here.
            return null;
        }
        Task task = taskManager.getTask(streamId.taskId().getId());
        if (task instanceof CancellableTask cancellable && EsqlStreamQueryAction.NAME.equals(task.getAction())) {
            return cancellable;
        }
        return null;
    }

    private void deleteFromStorage(String id, boolean taskWasFound, ActionListener<AcknowledgedResponse> listener) {
        call((ActionListener<Void> l) -> storage.delete(id, l), ignored -> listener.onResponse(AcknowledgedResponse.TRUE), e -> {
            if (taskWasFound && isNotFound(e)) {
                // The query was cancelled, which is what the caller asked for, even if nothing was stored (any more).
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                fail(id, "delete", e, listener);
            }
        });
    }

    /**
     * Runs one asynchronous step and hands its outcome to exactly one of the two handlers, whether the step reports a failure to
     * its listener or throws it.
     * <p>
     * {@link ActionListener#wrap} is not used for this, because it passes an exception thrown by the response handler to the failure
     * handler. Both handlers end by completing the listener of the delete, so that would complete it a second time, and log a
     * failure of the step for something that happened after the step succeeded. Here the first outcome the step reports is the only
     * one that is handled. An exception thrown by a handler is not an outcome of the step: it is rethrown, and if the step catches
     * it and reports it as its own failure, as a step built on {@link ActionListener#wrap} does, that second report is dropped.
     */
    private static <T> void call(Consumer<ActionListener<T>> step, Consumer<T> onResponse, Consumer<Exception> onFailure) {
        final AtomicBoolean reported = new AtomicBoolean();
        try {
            step.accept(new ActionListener<>() {
                @Override
                public void onResponse(T response) {
                    if (reported.compareAndSet(false, true)) {
                        onResponse.accept(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (reported.compareAndSet(false, true)) {
                        onFailure.accept(e);
                    }
                }
            });
        } catch (Exception e) {
            if (reported.compareAndSet(false, true) == false) {
                throw e;
            }
            onFailure.accept(e);
        }
    }

    /**
     * Completes the listener with a failure. A not-found becomes the same {@link ResourceNotFoundException} whatever its origin.
     * Any other failure is logged and passed on unchanged, so the caller sees that the stream may still be stored.
     */
    private static void fail(String id, String operation, Exception e, ActionListener<AcknowledgedResponse> listener) {
        if (isNotFound(e)) {
            listener.onFailure(new ResourceNotFoundException(id));
        } else {
            logger.warn(() -> Strings.format("failed to %s stream [%s]", operation, id), e);
            listener.onFailure(e);
        }
    }

    private static boolean isNotFound(Exception e) {
        return ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)) == RestStatus.NOT_FOUND;
    }
}
