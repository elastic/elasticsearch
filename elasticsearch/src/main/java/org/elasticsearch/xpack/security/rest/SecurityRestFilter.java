/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilter;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.transport.ServerTransportFilter;
import org.elasticsearch.xpack.ssl.SSLService;


import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;

public class SecurityRestFilter extends RestFilter {

    private final AuthenticationService service;
    private final Logger logger;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final RestController restController;
    private final boolean extractClientCertificate;

    @Inject
    public SecurityRestFilter(AuthenticationService service, RestController controller, Settings settings,
            ThreadPool threadPool, XPackLicenseState licenseState, SSLService sslService) {
        this.service = service;
        this.licenseState = licenseState;
        this.threadContext = threadPool.getThreadContext();
        this.logger = Loggers.getLogger(getClass(), settings);
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
        this.extractClientCertificate = ssl && sslService.isSSLClientAuthEnabled(httpSSLSettings);
        controller.registerFilter(this);
        this.restController = controller;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws Exception {

        if (licenseState.isAuthAllowed() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            if (extractClientCertificate) {
                Netty4HttpRequest nettyHttpRequest = (Netty4HttpRequest) request;
                SslHandler handler = nettyHttpRequest.getChannel().pipeline().get(SslHandler.class);
                assert handler != null;
                ServerTransportFilter.extactClientCertificates(logger, threadContext, handler.engine(), nettyHttpRequest.getChannel());
            }
            service.authenticate(request, ActionListener.wrap((authentication) -> {
                    RemoteHostHeader.process(request, threadContext);
                    filterChain.continueProcessing(request, channel, client);
                }, (e) -> restController.sendErrorResponse(request, channel, e)));
        } else {
            filterChain.continueProcessing(request, channel, client);
        }
    }
}
