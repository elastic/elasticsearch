/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.RewritableIndicesRequest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

public interface CustomIndicesRequestRewriter {
    void rewrite(RewritableIndicesRequest request);

    // TODO doesn't belong here
    boolean enabled();

    // TODO also doesn't belong here nor is this the right signature
    <T extends TransportResponse> void sendRequest(
        TransportInterceptor.AsyncSender sender,
        Transport.Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    );

    class Default implements CustomIndicesRequestRewriter {
        @Override
        public void rewrite(RewritableIndicesRequest request) {
            // No rewriting by default
            // This is a no-op implementation
        }

        @Override
        public boolean enabled() {
            return false; // Default implementation is not enabled
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

        }
    }
}
