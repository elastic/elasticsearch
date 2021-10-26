/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.http.nio.NioHttpChannel;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.nio.NioTcpChannel;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.transport.nio.SSLChannelContext;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

public class SSLEngineUtils {

    private SSLEngineUtils() {}

    public static void extractClientCertificates(Logger logger, ThreadContext threadContext, HttpChannel httpChannel) {
        SSLEngine sslEngine = getSSLEngine(httpChannel);
        extract(logger, threadContext, sslEngine, httpChannel);
    }

    public static void extractClientCertificates(Logger logger, ThreadContext threadContext, TcpChannel tcpChannel) {
        SSLEngine sslEngine = getSSLEngine(tcpChannel);
        extract(logger, threadContext, sslEngine, tcpChannel);
    }

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
            if (handler == null) {
                if (nettyChannel.isOpen()) {
                    assert false : "Must have SslHandler";
                } else {
                    throw new ChannelException("Channel is closed.");
                }
            }
            return handler.engine();
        } else if (tcpChannel instanceof NioTcpChannel) {
            SocketChannelContext context = ((NioTcpChannel) tcpChannel).getContext();
            assert context instanceof SSLChannelContext : "Must be SSLChannelContext.class, found:  " + context.getClass();
            return ((SSLChannelContext) context).getSSLEngine();
        } else {
            throw new AssertionError("Unknown channel class type: " + tcpChannel.getClass());
        }
    }

    private static void extract(Logger logger, ThreadContext threadContext, SSLEngine sslEngine, Object channel) {
        try {
            Certificate[] certs = sslEngine.getSession().getPeerCertificates();
            if (certs instanceof X509Certificate[]) {
                threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, certs);
            }
        } catch (SSLPeerUnverifiedException e) {
            // this happens when client authentication is optional and the client does not provide credentials. If client
            // authentication was required then this connection should be closed before ever getting into this class
            assert sslEngine.getNeedClientAuth() == false;
            assert sslEngine.getWantClientAuth();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "SSL Peer did not present a certificate on channel [{}]", channel), e);
            } else if (logger.isDebugEnabled()) {
                logger.debug("SSL Peer did not present a certificate on channel [{}]", channel);
            }
        }
    }
}
