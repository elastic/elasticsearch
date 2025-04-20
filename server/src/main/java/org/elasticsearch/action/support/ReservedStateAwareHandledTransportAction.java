/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.reservedstate.ActionWithReservedState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * An extension of the {@link HandledTransportAction} class, which wraps the doExecute call with a check for clashes
 * with the reserved cluster state.
 */
public abstract class ReservedStateAwareHandledTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response>
    implements
        ActionWithReservedState<Request> {
    private final ClusterService clusterService;

    protected ReservedStateAwareHandledTransportAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader
    ) {
        super(actionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
    }

    /**
     * A doExecute method wrapped with a check for clashes with updates to the reserved cluster state
     */
    protected abstract void doExecuteProtected(Task task, Request request, ActionListener<Response> listener);

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        assert reservedStateHandlerName().isPresent();

        validateForReservedState(
            clusterService.state().metadata().reservedStateMetadata().values(),
            reservedStateHandlerName().get(),
            modifiedKeys(request),
            request.toString()
        );
        doExecuteProtected(task, request, listener);
    }
}
