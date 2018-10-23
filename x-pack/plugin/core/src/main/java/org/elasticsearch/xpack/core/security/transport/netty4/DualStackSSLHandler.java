/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.util.List;

public class DualStackSSLHandler extends SslHandler {

    private boolean firstRead = true;
    private boolean isTLS = true;

    public DualStackSSLHandler(SSLEngine engine) {
        super(engine);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws SSLException {
        if (firstRead) {
            attemptHandshakeReads(ctx, in, out);
        }

        if (isTLS) {
            super.decode(ctx, in, out);
        } else {
            ctx.fireChannelRead(in);
        }

    }

    private void attemptHandshakeReads(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws SSLException {
        int initialReaderIndex = in.readerIndex();
        try {
            super.decode(ctx, in, out);
            firstRead = super.handshakeFuture().isDone() == false;
        } catch (NotSslRecordException e) {
            isTLS = false;
            firstRead = false;
            in.readerIndex(initialReaderIndex);
            ctx.fireChannelRead(in);
        }
    }
}
