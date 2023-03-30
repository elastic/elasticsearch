/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.util.function.Consumer;

/**
 * Service that is capable of retrieving and cleaning up AsyncTasks regardless of their state. It works with the TaskManager, if a task
 * is still running and AsyncTaskIndexService if task results already stored there.
 */
public class DeleteAsyncResultsService {
    private final Logger logger = LogManager.getLogger(DeleteAsyncResultsService.class);
    private final TaskManager taskManager;
    private final AsyncTaskIndexService<? extends AsyncResponse<?>> store;

    /**
     * Creates async results service
     *
     * @param store          AsyncTaskIndexService for the response we are working with
     * @param taskManager    task manager
     */
    public DeleteAsyncResultsService(AsyncTaskIndexService<? extends AsyncResponse<?>> store, TaskManager taskManager) {
        this.taskManager = taskManager;
        this.store = store;
    }

    public void deleteResponse(DeleteAsyncResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        hasCancelTaskPrivilegeAsync(resp -> deleteResponseAsync(request, resp, listener));
    }

    /**
     * Checks if the authenticated user has the right privilege (cancel_task) to
     * delete async search submitted by another user.
     */
    private void hasCancelTaskPrivilegeAsync(Consumer<Boolean> consumer) {
        final Authentication current = store.getSecurityContext().getAuthentication();
        if (current != null) {
            HasPrivilegesRequest req = new HasPrivilegesRequest();
            req.username(current.getEffectiveSubject().getUser().principal());
            req.clusterPrivileges(ClusterPrivilegeResolver.CANCEL_TASK.name());
            req.indexPrivileges(new RoleDescriptor.IndicesPrivileges[] {});
            req.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[] {});
            try {
                store.getClient()
                    .execute(
                        HasPrivilegesAction.INSTANCE,
                        req,
                        ActionListener.wrap(resp -> consumer.accept(resp.isCompleteMatch()), exc -> consumer.accept(false))
                    );
            } catch (Exception exc) {
                consumer.accept(false);
            }
        } else {
            consumer.accept(false);
        }
    }

    private void deleteResponseAsync(
        DeleteAsyncResultRequest request,
        boolean hasCancelTaskPrivilege,
        ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            AsyncTask task = hasCancelTaskPrivilege
                ? AsyncTaskIndexService.getTask(taskManager, searchId, AsyncTask.class)
                : store.getTaskAndCheckAuthentication(taskManager, searchId, AsyncTask.class);
            if (task != null) {
                // the task was found and gets cancelled. The response may or may not be found, but we will return 200 anyways.
                task.cancelTask(taskManager, () -> deleteResponseFromIndex(searchId, true, listener), "cancelled by user");
            } else {
                if (hasCancelTaskPrivilege) {
                    deleteResponseFromIndex(searchId, false, listener);
                } else {
                    store.ensureAuthenticatedUserCanDeleteFromIndex(
                        searchId,
                        ActionListener.wrap(res -> deleteResponseFromIndex(searchId, false, listener), listener::onFailure)
                    );
                }
            }
        } catch (Exception exc) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
        }
    }

    private void deleteResponseFromIndex(AsyncExecutionId taskId, boolean taskWasFound, ActionListener<AcknowledgedResponse> listener) {
        store.deleteResponse(taskId, ActionListener.wrap(resp -> {
            if (resp.status() == RestStatus.OK || taskWasFound) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(new ResourceNotFoundException(taskId.getEncoded()));
            }
        }, exc -> {
            RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
            // the index may not be there (no initial async search response stored yet?): we still want to return 200
            // note that index missing comes back as 200 hence it's handled in the onResponse callback
            if (status == RestStatus.NOT_FOUND && taskWasFound) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                logger.error(() -> "failed to clean async result [" + taskId.getEncoded() + "]", exc);
                listener.onFailure(new ResourceNotFoundException(taskId.getEncoded()));
            }
        }));
    }
}
