/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 *
 */
public class RemoteHostHeader {

    static final String KEY = "_rest_remote_address";

    /**
     * Extracts the remote address from the given rest request and puts in the request context. This will
     * then be copied to the subsequent action requests.
     */
    public static void process(RestRequest request) {
        request.putInContext(KEY, request.getRemoteAddress());
    }

    /**
     * Extracts the rest remote address from the message context. If not found, returns {@code null}. transport
     * messages that were created by rest handlers, should have this in their context.
     */
    public static InetSocketAddress restRemoteAddress(TransportMessage message) {
        SocketAddress address = message.getFromContext(KEY);
        if (address != null && address instanceof InetSocketAddress) {
            return (InetSocketAddress) address;
        }
        return null;
    }

    public static void putRestRemoteAddress(TransportMessage message, SocketAddress address) {
        message.putInContext(KEY, address);
    }
}
