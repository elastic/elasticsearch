/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;
import org.elasticsearch.xpack.core.ssl.TLSv1DeprecationHandler;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.transport.ServerTransportFilter;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService service;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final boolean extractClientCertificate;
    private final TLSv1DeprecationHandler tlsDeprecationHandler;

    public SecurityRestFilter(XPackLicenseState licenseState, ThreadContext threadContext, AuthenticationService service,
                              RestHandler restHandler, boolean extractClientCertificate, TLSv1DeprecationHandler tlsDeprecationHandler) {
        this.restHandler = restHandler;
        this.service = service;
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.extractClientCertificate = extractClientCertificate;
        this.tlsDeprecationHandler = tlsDeprecationHandler;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (tlsDeprecationHandler.shouldLogWarnings()) {
            // This is handled here so that the deprecation warnings are returned to the HTTP client.
            // If it were done in the network layer (SecurityNetty4HttpServerTransport) then the deprecation warning
            //  would be cleared from the thread context when the request was handled
            final SSLEngine engine = getSslEngine((Netty4HttpRequest) request);
            // Set the description to include the remote host (because it's hard to fix clients if you don't know who they are
            // But don't include the port because then every connection would have a different deprecation key
            tlsDeprecationHandler.checkAndLog(engine.getSession(), () -> "HTTP connection from " + remoteHost(request));
        }

        if (licenseState.isAuthAllowed() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            if (extractClientCertificate) {
                Netty4HttpRequest nettyHttpRequest = (Netty4HttpRequest) request;
                final SSLEngine engine = getSslEngine(nettyHttpRequest);
                ServerTransportFilter.extractClientCertificates(logger, threadContext, engine, nettyHttpRequest.getChannel());
            }
            service.authenticate(maybeWrapRestRequest(request), ActionListener.wrap(
                authentication -> {
                    RemoteHostHeader.process(request, threadContext);
                    restHandler.handleRequest(request, channel, client);
                }, e -> {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error((Supplier<?>) () ->
                            new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
                    }
            }));
        } else {
            restHandler.handleRequest(request, channel, client);
        }
    }

    private String remoteHost(RestRequest request) {
        final SocketAddress address = request.getRemoteAddress();
        if (address instanceof InetSocketAddress) {
            InetSocketAddress inet = (InetSocketAddress) address;
            return inet.getHostString();
        } else {
            return address.toString();
        }
    }

    private SSLEngine getSslEngine(Netty4HttpRequest nettyHttpRequest) {
        SslHandler handler = nettyHttpRequest.getChannel().pipeline().get(SslHandler.class);
        assert handler != null;
        return handler.engine();
    }

    RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter)restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }
}
