/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import io.netty.channel.Channel;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RemoteHostHeader {

    static final String KEY = "_rest_remote_address";

    /**
     * Extracts the remote address from the given netty channel and puts it in the request context. This will
     * then be copied to the subsequent action handler contexts.
     */
    public static void process(Channel channel, ThreadContext threadContext) {
        threadContext.putTransient(KEY, channel.remoteAddress());
    }

    /**
     * Extracts the rest remote address from the message context. If not found, returns {@code null}.
     * Transport messages that were created by rest handlers should have this in their context.
     */
    public static InetSocketAddress restRemoteAddress(ThreadContext threadContext) {
        SocketAddress address = threadContext.getTransient(KEY);
        if (address != null && address instanceof InetSocketAddress) {
            return (InetSocketAddress) address;
        }
        return null;
    }
}
