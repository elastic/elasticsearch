/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;

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
    public DeleteAsyncResultsService(AsyncTaskIndexService<? extends AsyncResponse<?>> store,
                                     TaskManager taskManager) {
        this.taskManager = taskManager;
        this.store = store;

    }

    public void deleteResult(DeleteAsyncResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            AsyncTask task = store.getTask(taskManager, searchId, AsyncTask.class);
            if (task != null) {
                //the task was found and gets cancelled. The response may or may not be found, but we will return 200 anyways.
                task.cancelTask(taskManager, () -> store.deleteResponse(searchId,
                    ActionListener.wrap(
                        r -> listener.onResponse(new AcknowledgedResponse(true)),
                        exc -> {
                            RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
                            //the index may not be there (no initial async search response stored yet?): we still want to return 200
                            //note that index missing comes back as 200 hence it's handled in the onResponse callback
                            if (status == RestStatus.NOT_FOUND) {
                                listener.onResponse(new AcknowledgedResponse(true));
                            } else {
                                logger.error(() -> new ParameterizedMessage("failed to clean async result [{}]",
                                    searchId.getEncoded()), exc);
                                listener.onFailure(exc);
                            }
                        })), "cancelled by user"
                );
            } else {
                // the task was not found (already cancelled, already completed, or invalid id?)
                // we fail if the response is not found in the index
                ActionListener<DeleteResponse> deleteListener = ActionListener.wrap(
                    resp -> {
                        if (resp.status() == RestStatus.NOT_FOUND) {
                            listener.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                        } else {
                            listener.onResponse(new AcknowledgedResponse(true));
                        }
                    },
                    exc -> {
                        logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", searchId.getEncoded()), exc);
                        listener.onFailure(exc);
                    }
                );
                //we get before deleting to verify that the user is authorized
                store.authorizeResponse(searchId, false,
                    ActionListener.wrap(res -> store.deleteResponse(searchId, deleteListener), listener::onFailure));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }
}
