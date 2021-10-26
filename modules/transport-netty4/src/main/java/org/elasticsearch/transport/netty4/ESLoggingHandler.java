/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

final class ESLoggingHandler extends LoggingHandler {

    ESLoggingHandler() {
        super(LogLevel.TRACE);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // We do not want to log read complete events because we log inbound messages in the TcpTransport.
        ctx.fireChannelReadComplete();
    }
}
