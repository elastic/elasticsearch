/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.transport.ServerTransportFilter;
import org.elasticsearch.xpack.ssl.SSLService;

import java.io.IOException;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = ESLoggerFactory.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService service;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final boolean extractClientCertificate;

    public SecurityRestFilter(Settings settings, XPackLicenseState licenseState, SSLService sslService,
                              ThreadContext threadContext, AuthenticationService service, RestHandler restHandler) {
        this.restHandler = restHandler;
        this.service = service;
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
        this.extractClientCertificate = ssl && sslService.isSSLClientAuthEnabled(httpSSLSettings);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (licenseState.isAuthAllowed() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            if (extractClientCertificate) {
                Netty4HttpRequest nettyHttpRequest = (Netty4HttpRequest) request;
                SslHandler handler = nettyHttpRequest.getChannel().pipeline().get(SslHandler.class);
                assert handler != null;
                ServerTransportFilter.extactClientCertificates(logger, threadContext, handler.engine(), nettyHttpRequest.getChannel());
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

    RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter)restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }
}
