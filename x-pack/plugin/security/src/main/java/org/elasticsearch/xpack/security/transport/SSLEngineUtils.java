/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.http.nio.NioHttpChannel;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.nio.NioTcpChannel;
import org.elasticsearch.xpack.security.transport.nio.SSLChannelContext;

import javax.net.ssl.SSLEngine;

public class SSLEngineUtils {

    public static SSLEngine getSSLEngine(HttpChannel httpChannel) {
        if (httpChannel instanceof Netty4HttpChannel) {
            Channel nettyChannel = ((Netty4HttpChannel) httpChannel).getNettyChannel();
            SslHandler handler = nettyChannel.pipeline().get(SslHandler.class);
            assert handler != null : "Must have SslHandler";
            return handler.engine();
        } else if (httpChannel instanceof NioHttpChannel) {
            SocketChannelContext context = ((NioHttpChannel) httpChannel).getContext();
            assert context instanceof SSLChannelContext : "Must be SSLChannelContext.class, found:  " + context.getClass();
            return ((SSLChannelContext) context).getSSLEngine();
        } else {
            throw new AssertionError("Unknown channel class type: " + httpChannel.getClass());
        }
    }

    public static SSLEngine getSSLEngine(TcpChannel tcpChannel) {
        if (tcpChannel instanceof Netty4TcpChannel) {
            Channel nettyChannel = ((Netty4TcpChannel) tcpChannel).getNettyChannel();
            SslHandler handler = nettyChannel.pipeline().get(SslHandler.class);
            assert handler != null : "Must have SslHandler";
            return handler.engine();
        } else if (tcpChannel instanceof NioTcpChannel) {
            SocketChannelContext context = ((NioTcpChannel) tcpChannel).getContext();
            assert context instanceof SSLChannelContext : "Must be SSLChannelContext.class, found:  " + context.getClass();
            return ((SSLChannelContext) context).getSSLEngine();
        } else {
            throw new AssertionError("Unknown channel class type: " + tcpChannel.getClass());
        }
    }
}
