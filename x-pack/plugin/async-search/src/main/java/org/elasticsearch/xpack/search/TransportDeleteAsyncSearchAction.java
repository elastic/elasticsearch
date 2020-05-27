/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<DeleteAsyncSearchAction.Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteAsyncSearchAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AsyncTaskIndexService<AsyncSearchResponse> store;

    @Inject
    public TransportDeleteAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            NamedWriteableRegistry registry,
                                            Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, DeleteAsyncSearchAction.Request::new);
        this.store = new AsyncTaskIndexService<>(AsyncSearch.INDEX, clusterService, threadPool.getThreadContext(), client,
            ASYNC_SEARCH_ORIGIN, AsyncSearchResponse::new, registry);
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncSearchAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
                cancelTaskAndDeleteResult(searchId, listener);
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                transportService.sendRequest(node, DeleteAsyncSearchAction.NAME, request, builder.build(),
                    new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new, ThreadPool.Names.SAME));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    void cancelTaskAndDeleteResult(AsyncExecutionId searchId, ActionListener<AcknowledgedResponse> listener) throws IOException {
        AsyncSearchTask task = store.getTask(taskManager, searchId, AsyncSearchTask.class);
        if (task != null) {
            //the task was found and gets cancelled. The response may or may not be found, but we will return 200 anyways.
            task.cancelTask(() -> store.deleteResponse(searchId,
                ActionListener.wrap(
                    r -> listener.onResponse(new AcknowledgedResponse(true)),
                    exc -> {
                        RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
                        //the index may not be there (no initial async search response stored yet?): we still want to return 200
                        //note that index missing comes back as 200 hence it's handled in the onResponse callback
                        if (status == RestStatus.NOT_FOUND) {
                            listener.onResponse(new AcknowledgedResponse(true));
                        } else {
                            logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", searchId.getEncoded()), exc);
                            listener.onFailure(exc);
                        }
                    })));
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
            store.getResponse(searchId, false,
                ActionListener.wrap(res -> store.deleteResponse(searchId, deleteListener), listener::onFailure));
        }
    }
}
