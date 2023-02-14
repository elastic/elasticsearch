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
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;

public class SSLEngineUtils {

    private SSLEngineUtils() {}

    public static void extractClientCertificates(Logger logger, ThreadContext threadContext, TcpChannel tcpChannel) {
        extractClientCertificates(logger, threadContext, getNettyChannel(tcpChannel));
    }

    public static void extractClientCertificates(Logger logger, ThreadContext threadContext, Channel channel) {
        SSLEngine sslEngine = getSSLEngine(channel);
        extract(logger, threadContext, sslEngine, channel);
    }

    public static Channel getNettyChannel(HttpChannel httpChannel) {
        if (httpChannel instanceof Netty4HttpChannel) {
            return ((Netty4HttpChannel) httpChannel).getNettyChannel();
        } else {
            throw new AssertionError("Unknown channel class type: " + httpChannel.getClass());
        }
    }

    public static Channel getNettyChannel(TcpChannel tcpChannel) {
        if (tcpChannel instanceof Netty4TcpChannel) {
            return ((Netty4TcpChannel) tcpChannel).getNettyChannel();
        } else {
            throw new AssertionError("Unknown channel class type: " + tcpChannel.getClass());
        }
    }

    public static SSLEngine getSSLEngine(TcpChannel tcpChannel) {
        return getSSLEngine(getNettyChannel(tcpChannel));
    }

    private static SSLEngine getSSLEngine(Channel nettyChannel) {
        SslHandler handler = nettyChannel.pipeline().get(SslHandler.class);
        if (handler == null) {
            if (nettyChannel.isOpen()) {
                assert false : "Must have SslHandler";
            } else {
                throw new ChannelException("Channel is closed.");
            }
        }
        return handler.engine();
    }

    private static void extract(Logger logger, ThreadContext threadContext, SSLEngine sslEngine, Channel channel) {
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
                logger.trace((Supplier<?>) () -> "SSL Peer did not present a certificate on channel [" + channel + "]", e);
            } else if (logger.isDebugEnabled()) {
                logger.debug("SSL Peer did not present a certificate on channel [{}]", channel);
            }
        }
    }
}
