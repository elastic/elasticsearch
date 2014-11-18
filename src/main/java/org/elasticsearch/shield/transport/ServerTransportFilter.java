/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.transport.TransportRequest;

public interface ServerTransportFilter {

    static final ServerTransportFilter NOOP = new ServerTransportFilter() {
        @Override
        public void inbound(String action, TransportRequest request) {}
    };

    /**
     * Called just after the given request was received by the transport. Any exception
     * thrown by this method will stop the request from being handled and the error will
     * be sent back to the sender.
     */
    void inbound(String action, TransportRequest request);

}
