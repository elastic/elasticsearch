/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.transport.TransportRequest;

public interface ServerTransportFilter {

    /**
     * Called just after the given request was received by the transport. Any exception
     * thrown by this method will stop the request from being handled and the error will
     * be sent back to the sender.
     */
    void inbound(String action, TransportRequest request);

    /**
     * The server trasnport filter that should be used in transport clients
     */
    public static class TransportClient implements ServerTransportFilter {

        @Override
        public void inbound(String action, TransportRequest request) {
        }
    }

    /**
     * The server trasnport filter that should be used in nodes
     */
    public static class Node implements ServerTransportFilter {

        private final AuthenticationService authcService;
        private final AuthorizationService authzService;

        @Inject
        public Node(AuthenticationService authcService, AuthorizationService authzService) {
            this.authcService = authcService;
            this.authzService = authzService;
        }

        @Override
        public void inbound(String action, TransportRequest request) {
            /**
                here we don't have a fallback user, as all incoming request are
                expected to have a user attached (either in headers or in context)
                We can make this assumption because in nodes we also have the
                {@link ClientTransportFilter.Node} that makes sure all outgoing requsts
                from all the nodes are attached with a user (either a serialize user
                an authentication token
             */
            User user = authcService.authenticate(action, request, null);
            authzService.authorize(user, action, request);
        }
    }

    /**
     * A server transport filter rejects internal calls, which should be used on connections
     * where only clients connect to
     */
    public static class RejectInternalActionsFilter extends ServerTransportFilter.Node {

        @Inject
        public RejectInternalActionsFilter(AuthenticationService authcService, AuthorizationService authzService) {
            super(authcService, authzService);
        }

        @Override
        public void inbound(String action, TransportRequest request) {
            // TODO is ']' sufficient to mark as shard action?
            boolean isInternalOrShardAction = action.startsWith("internal:") || action.endsWith("]");
            if (isInternalOrShardAction) {
                throw new AuthenticationException("Not allowed to execute internal/shard actions");
            }
            super.inbound(action, request);
        }
    }

}
