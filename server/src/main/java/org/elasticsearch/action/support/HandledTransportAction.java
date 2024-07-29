/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * A {@link TransportAction} which, on creation, registers a handler for its own {@link #actionName} with the transport service.
 */
public abstract class HandledTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends TransportAction<
    Request,
    Response> {

    protected HandledTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Executor executor
    ) {
        this(actionName, true, transportService, actionFilters, requestReader, executor);
    }

    @SuppressWarnings("this-escape")
    protected HandledTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Executor executor
    ) {
        super(actionName, actionFilters, transportService.getTaskManager(), executor);
        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            canTripCircuitBreaker,
            requestReader,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }
}
