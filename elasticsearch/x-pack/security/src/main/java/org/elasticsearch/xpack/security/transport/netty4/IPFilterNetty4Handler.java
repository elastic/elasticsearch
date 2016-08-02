/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.net.InetSocketAddress;

/**
 *
 */
public class IPFilterNetty4Handler extends AbstractRemoteAddressFilter<InetSocketAddress> {

    private final IPFilter filter;
    private final String profile;

    public IPFilterNetty4Handler(IPFilter filter, String profile) {
        this.filter = filter;
        this.profile = profile;
    }

    @Override
    protected boolean accept(ChannelHandlerContext ctx, InetSocketAddress inetSocketAddress) throws Exception {
        return filter.accept(profile, inetSocketAddress.getAddress());
    }
}
