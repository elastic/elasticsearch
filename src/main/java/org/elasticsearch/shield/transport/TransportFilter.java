/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

/**
 *
 */
public interface TransportFilter {

    /**
     * Called just before the given request is about to be sent. Any exception thrown
     * by this method will stop the request from being sent.
     */
    void outboundRequest(String action, TransportRequest request);

    /**
     * Called just after the given request was received by the transport. Any exception
     * thrown by this method will stop the request from being handled and the error will
     * be sent back to the sender.
     */
    void inboundRequest(String action, TransportRequest request);

    /**
     * Called just before the given response is about to be sent. Any exception thrown
     * by this method will stop the response from being sent and an error will be sent
     * instead.
     */
    void outboundResponse(String action, TransportResponse response);

    /**
     * Called just after the given response was received by the transport. Any exception
     * thrown by this method will stop the response from being handled normally and instead
     * the error will be used as the response.
     */
    void inboundResponse(TransportResponse response);

    public static class Base implements TransportFilter {

        @Override
        public void outboundRequest(String action, TransportRequest request) {
        }

        @Override
        public void inboundRequest(String action, TransportRequest request) {
        }

        @Override
        public void outboundResponse(String action, TransportResponse response) {
        }

        @Override
        public void inboundResponse(TransportResponse response) {
        }
    }

}
