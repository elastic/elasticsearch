/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * This interface allows clients, that connect to an elasticsearch cluster, to execute
 * additional logic before an operation is sent.
 *
 * This interface only applies to outgoing messages
 */
public interface ClientTransportFilter {

    /**
     * Called just before the given request is sent by the transport. Any exception
     * thrown by this method will stop the request from being sent and the error will
     * be sent back to the sender.
     */
    void outbound(String action, TransportRequest request) throws IOException;

    /**
     * The client transport filter that should be used in transport clients
     */
    public static class TransportClient implements ClientTransportFilter {

        @Override
        public void outbound(String action, TransportRequest request) {
        }
    }

    /**
     * The client transport filter that should be used in nodes
     */
    public static class Node implements ClientTransportFilter {

        private final AuthenticationService authcService;

        @Inject
        public Node(AuthenticationService authcService) {
            this.authcService = authcService;
        }

        @Override
        public void outbound(String action, TransportRequest request) throws IOException {
            /**
                this will check if there's a user associated with the request. If there isn't,
                the system user will be attached. There cannot be a request outgoing from this
                node that is not associated with a user.
             */
            authcService.attachUserHeaderIfMissing(SystemUser.INSTANCE);
        }
    }
}
