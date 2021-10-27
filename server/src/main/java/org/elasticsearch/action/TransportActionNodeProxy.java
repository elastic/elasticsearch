/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 * A generic proxy that will execute the given action against a specific node.
 */
public class TransportActionNodeProxy<Request extends ActionRequest, Response extends ActionResponse> {

    private final TransportService transportService;
    private final ActionType<Response> action;
    private final TransportRequestOptions transportOptions;

    public TransportActionNodeProxy(ActionType<Response> action, TransportService transportService) {
        this.action = action;
        this.transportService = transportService;
        this.transportOptions = action.transportOptions();
    }

    public void execute(final DiscoveryNode node, final Request request, final ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        transportService.sendRequest(
            node,
            action.name(),
            request,
            transportOptions,
            new ActionListenerResponseHandler<>(listener, action.getResponseReader())
        );
    }
}
