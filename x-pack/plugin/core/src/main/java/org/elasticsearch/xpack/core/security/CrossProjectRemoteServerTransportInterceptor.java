/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

public interface CrossProjectRemoteServerTransportInterceptor {
    // TODO probably don't want this
    boolean enabled();

    // TODO this should be a wrapper around TransportInterceptor.AsyncSender instead
    <T extends TransportResponse> void sendRequest(
        TransportInterceptor.AsyncSender sender,
        Transport.Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    );

    CustomServerTransportFilter getFilter();

    class Default implements CrossProjectRemoteServerTransportInterceptor {
        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public <T extends TransportResponse> void sendRequest(
            TransportInterceptor.AsyncSender sender,
            Transport.Connection connection,
            String action,
            TransportRequest request,
            TransportRequestOptions options,
            TransportResponseHandler<T> handler
        ) {
            sender.sendRequest(connection, action, request, options, handler);
        }

        @Override
        public CustomServerTransportFilter getFilter() {
            return new CustomServerTransportFilter.Default();
        }
    }
}
