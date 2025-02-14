/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;

import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.net.InetSocketAddress;
import java.util.function.BiPredicate;

@ChannelHandler.Sharable
public class AcceptChannelHandler extends AbstractRemoteAddressFilter<InetSocketAddress> {

    private final BiPredicate<String, InetSocketAddress> predicate;
    private final String profile;
    private final ThreadContext threadContext;

    public AcceptChannelHandler(
        final BiPredicate<String, InetSocketAddress> predicate,
        final String profile,
        final ThreadContext threadContext
    ) {
        this.predicate = predicate;
        this.profile = profile;
        this.threadContext = threadContext;
    }

    @Override
    protected boolean accept(final ChannelHandlerContext ctx, final InetSocketAddress remoteAddress) throws Exception {
        // this prevents thread-context changes to propagate beyond the channel accept test, as netty worker threads are reused
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext()) {
            return predicate.test(profile, remoteAddress);
        }
    }

    public interface AcceptPredicate extends BiPredicate<String, InetSocketAddress> {

        void setBoundAddress(BoundTransportAddress boundHttpTransportAddress);
    }
}
