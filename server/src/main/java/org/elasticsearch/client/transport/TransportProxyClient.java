/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transport;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.TransportActionNodeProxy;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

final class TransportProxyClient {

    private final TransportClientNodesService nodesService;
    private final Map<ActionType<?>, TransportActionNodeProxy<?, ?>> proxies;

    @SuppressWarnings({"rawtypes", "unchecked"})
    TransportProxyClient(TransportService transportService, TransportClientNodesService nodesService, List<ActionType<?>> actions) {
        this.nodesService = nodesService;
        Map<ActionType<?>, TransportActionNodeProxy<?, ?>> proxies = new HashMap<>();
        for (ActionType<?> action : actions) {
            proxies.put(action, new TransportActionNodeProxy(action, transportService));
        }
        this.proxies = unmodifiableMap(proxies);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
        ActionRequestBuilder<Request, Response>> void execute(final ActionType<Response> action,
                                                                              final Request request, ActionListener<Response> listener) {
        @SuppressWarnings("unchecked")
        final TransportActionNodeProxy<Request, Response> proxy = (TransportActionNodeProxy<Request, Response>) proxies.get(action);
        assert proxy != null : "no proxy found for action: " + action;
        nodesService.execute((n, l) -> proxy.execute(n, request, l), listener);
    }
}
