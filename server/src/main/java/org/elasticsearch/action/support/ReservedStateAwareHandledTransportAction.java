/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

import static org.elasticsearch.reservedstate.service.ReservedClusterStateService.validateForReservedState;

/**
 * An extension of the {@link HandledTransportAction} class, which wraps the doExecute call with a check for clashes
 * with the reserved cluster state.
 */
public abstract class ReservedStateAwareHandledTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {
    private final ClusterService clusterService;

    protected ReservedStateAwareHandledTransportAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.clusterService = clusterService;
    }

    /**
     * Override this method with the related {@link ReservedClusterStateHandler} information.
     * <p>
     * We need to check if certain settings or entities are allowed to be modified by the transport node
     * action, depending on if they are set as reserved in 'operator' mode (file based settings, modules, plugins).
     *
     * @return a String of the {@link ReservedClusterStateHandler} name
     */
    protected abstract String reservedStateHandlerName();

    /**
     * Override this method to return the keys of the cluster state or cluster entities that are modified by
     * the Request object.
     * <p>
     * This method is used by the reserved state handler logic (see {@link ReservedClusterStateHandler})
     * to verify if the keys don't conflict with an existing key set as reserved.
     *
     * @param request the TransportMasterNode request
     * @return set of String keys intended to be modified/set/deleted by this request
     */
    protected abstract Set<String> modifiedKeys(Request request);

    /**
     * A doExecute method wrapped with a check for clashes with updates to the reserved cluster state
     */
    protected abstract void doExecuteProtected(Task task, Request request, ActionListener<Response> listener);

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        validateForReservedState(clusterService.state(), reservedStateHandlerName(), modifiedKeys(request), request.toString());
        doExecuteProtected(task, request, listener);
    }
}
