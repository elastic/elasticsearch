/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public interface ClientTransportFilter {

    static final ClientTransportFilter NOOP = new ClientTransportFilter() {
        @Override
        public void outbound(String action, TransportRequest request) {}
    };

    /**
     * Called just before the given request is sent by the transport. Any exception
     * thrown by this method will stop the request from being sent and the error will
     * be sent back to the sender.
     */
    void outbound(String action, TransportRequest request);

}
