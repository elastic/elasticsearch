/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.http.netty;

import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ExceptionEvent;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;

/**
 * Makes the exceptionCaught method of {@link org.elasticsearch.http.netty.NettyHttpServerTransport} visible
 * to overriding classes.
 *
 * TODO: Fix core to make methods protected instead of package private and remove this class
 */
public class VisibleNettyHttpServerTransport extends NettyHttpServerTransport {

    public VisibleNettyHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays) {
        super(settings, networkService, bigArrays);
    }

    @Override
    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        super.exceptionCaught(ctx, e);
    }

}
