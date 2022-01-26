/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;

import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.net.InetSocketAddress;

@ChannelHandler.Sharable
class IpFilterRemoteAddressFilter extends AbstractRemoteAddressFilter<InetSocketAddress> {

    private final IPFilter filter;
    private final String profile;

    IpFilterRemoteAddressFilter(final IPFilter filter, final String profile) {
        this.filter = filter;
        this.profile = profile;
    }

    @Override
    protected boolean accept(final ChannelHandlerContext ctx, final InetSocketAddress remoteAddress) throws Exception {
        // at this stage no auth has happened, so we do not have any principal anyway
        return filter.accept(profile, remoteAddress);
    }

}
