/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.esql.action.EsqlStreamDeleteAction;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.InMemoryStreamStore;
import org.elasticsearch.xpack.esql.streaming.NoopStreamStore;
import org.elasticsearch.xpack.esql.streaming.StreamDeleteService;
import org.elasticsearch.xpack.esql.streaming.StreamId;
import org.elasticsearch.xpack.esql.streaming.StreamSearchSecurity;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;

/**
 * Deletes a stored stream, the counterpart of {@code TransportDeleteAsyncResultAction}.
 * <p>
 * Like the async action it runs on the node that runs the query, because only that node can cancel it: the node is read from the
 * stream id and the request is forwarded there. If that node has left the cluster there is nothing to cancel, and the node that
 * received the request deletes what is stored.
 */
public class TransportEsqlStreamDeleteAction extends HandledTransportAction<DeleteStreamResultRequest, AcknowledgedResponse> {

    private final StreamDeleteService deleteService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    @SuppressWarnings("this-escape")
    public TransportEsqlStreamDeleteAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client
    ) {
        super(
            EsqlStreamDeleteAction.NAME,
            transportService,
            actionFilters,
            DeleteStreamResultRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.deleteService = new StreamDeleteService(
            createStreamStorage(clusterService, threadPool, client),
            new StreamSearchSecurity(new SecurityContext(clusterService.getSettings(), threadPool.getThreadContext()), client),
            transportService.getTaskManager(),
            () -> clusterService.localNode().getId()
        );
    }

    /**
     * Creates the storage the streams are deleted from. The system-index backed storage does not exist yet, so this is a
     * {@link NoopStreamStore}, which knows no stream: until it is replaced every stream is not found. This is the one place to change
     * when the real storage exists, and it has the inputs the async storage is built from (cluster service, thread context and client).
     * Tests override it to use an {@link InMemoryStreamStore}.
     */
    protected StreamStorage createStreamStorage(ClusterService clusterService, ThreadPool threadPool, Client client) {
        return NoopStreamStore.INSTANCE;
    }

    @Override
    protected void doExecute(Task task, DeleteStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        final StreamId streamId;
        try {
            streamId = StreamId.decode(request.id());
        } catch (IllegalArgumentException e) {
            // The same answer as for an unknown stream, which is what a stream GET gives for any string that is not a stored stream.
            listener.onFailure(new ResourceNotFoundException(request.id()));
            return;
        }
        String nodeId = streamId.taskId().getNodeId();
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (clusterService.localNode().getId().equals(nodeId) || node == null) {
            deleteService.deleteStream(streamId, listener);
        } else {
            transportService.sendRequest(
                node,
                EsqlStreamDeleteAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::readFrom, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
