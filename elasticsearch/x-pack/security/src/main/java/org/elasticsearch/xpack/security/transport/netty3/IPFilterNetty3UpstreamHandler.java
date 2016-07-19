/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty3;

import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.ipfilter.IpFilteringHandlerImpl;

import java.net.InetSocketAddress;

/**
 *
 */
@ChannelHandler.Sharable
public class IPFilterNetty3UpstreamHandler extends IpFilteringHandlerImpl {

    private final IPFilter filter;
    private final String profile;

    public IPFilterNetty3UpstreamHandler(IPFilter filter, String profile) {
        this.filter = filter;
        this.profile = profile;
    }

    @Override
    protected boolean accept(ChannelHandlerContext channelHandlerContext, ChannelEvent channelEvent, InetSocketAddress inetSocketAddress)
            throws Exception {
        // at this stage no auth has happened, so we do not have any principal anyway
        return filter.accept(profile, inetSocketAddress.getAddress());
    }

}
