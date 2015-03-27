/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.authc.pki.PkiRealm;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

// TODO remove this class and add ability for transport filters to access netty channels
public class ShieldMessageChannelHandler extends MessageChannelHandler {

    protected final boolean extractClientCert;

    public ShieldMessageChannelHandler(NettyTransport transport, ESLogger logger, String profileName, boolean extractClientCert) {
        super(transport, logger, profileName);
        this.extractClientCert = extractClientCert;
    }

    protected String handleRequest(Channel channel, StreamInput buffer, long requestId, Version version) throws IOException {
        final String action = buffer.readString();
        transportServiceAdapter.onRequestReceived(requestId, action);
        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, transportServiceAdapter, action, channel, requestId, version, profileName);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action, version);
            if (handler == null) {
                throw new ActionNotFoundTransportException(action);
            }
            final TransportRequest request = handler.newInstance();
            request.remoteAddress(new InetSocketTransportAddress((InetSocketAddress) channel.getRemoteAddress()));
            request.readFrom(buffer);

            if (extractClientCert) {
                SslHandler sslHandler = channel.getPipeline().get(SslHandler.class);
                assert sslHandler != null;
                Certificate[] certs = sslHandler.getEngine().getSession().getPeerCertificates();
                if (certs instanceof X509Certificate[]) {
                    request.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, certs);
                }
            }

            if (ThreadPool.Names.SAME.equals(handler.executor())) {
                //noinspection unchecked
                handler.messageReceived(request, transportChannel);
            } else {
                threadPool.executor(handler.executor()).execute(new RequestHandler(handler, request, transportChannel, action));
            }
        } catch (Throwable e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
        return action;
    }
}
