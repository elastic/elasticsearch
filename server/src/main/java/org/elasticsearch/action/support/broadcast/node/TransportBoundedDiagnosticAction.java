/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * A bounded diagnostic action represents an action that aggregates information from all nodes, mainly for diagnostic purposes, and there is
 * a limit on how many such requests a node can coordinate concurrently. The need for such a class of actions is to protect the node from
 * getting overwhelmed when another load is slow to respond. The focus is on diagnostic requests because they tend to be executed
 * periodically.
 *
 * @param <Request>              the underlying client request
 * @param <Response>             the response to the client request
 * @param <ShardOperationResult> per-shard operation results
 */
public abstract class TransportBoundedDiagnosticAction<
    Request extends BroadcastRequest<Request>,
    Response extends BroadcastResponse,
    ShardOperationResult extends Writeable> extends TransportBroadcastByNodeAction<Request, Response, ShardOperationResult> {

    private final BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits;

    public TransportBoundedDiagnosticAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor,
        BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits
    ) {
        this(
            actionName,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            request,
            executor,
            true,
            boundedDiagnosticRequestPermits
        );
    }

    public TransportBoundedDiagnosticAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor,
        boolean canTripCircuitBreaker,
        BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits
    ) {
        super(
            actionName,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            request,
            executor,
            canTripCircuitBreaker
        );

        this.boundedDiagnosticRequestPermits = boundedDiagnosticRequestPermits;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (boundedDiagnosticRequestPermits.tryAcquire()) {
            final Runnable release = new RunOnce(boundedDiagnosticRequestPermits::release);
            boolean success = false;
            try {
                super.doExecute(task, request, ActionListener.runBefore(listener, release::run));
                success = true;
            } finally {
                if (success == false) {
                    release.run();
                }
            }
        } else {
            listener.onFailure(new EsRejectedExecutionException("too many bounded diagnostic requests"));
        }
    }
}
