/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;

/**
 * Transport filter which is called after a transport request is received by the transport service.
 */
public interface ServerTransportFilter {

    /**
     * Called just after the given request was received by the transport service.
     * <p>
     * Any exception thrown by this method will stop the request from being handled
     * and the error will be sent back to the sender.
     */
    void inbound(String action, TransportRequest request, TransportChannel transportChannel, ActionListener<Void> listener);
}
