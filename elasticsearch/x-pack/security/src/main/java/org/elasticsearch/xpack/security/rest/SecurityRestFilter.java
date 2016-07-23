/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty3.Netty3HttpRequest;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilter;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 *
 */
public class SecurityRestFilter extends RestFilter {

    private final AuthenticationService service;
    private final ESLogger logger;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final boolean extractClientCertificate;

    @Inject
    public SecurityRestFilter(AuthenticationService service, RestController controller, Settings settings,
                              ThreadPool threadPool, XPackLicenseState licenseState) {
        this.service = service;
        this.licenseState = licenseState;
        this.threadContext = threadPool.getThreadContext();
        controller.registerFilter(this);
        boolean ssl = SecurityNetty3HttpServerTransport.SSL_SETTING.get(settings);
        extractClientCertificate = ssl && SecurityNetty3HttpServerTransport.CLIENT_AUTH_SETTING.get(settings).enabled();
        logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws Exception {

        if (licenseState.isAuthAllowed()) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            if (request.method() != RestRequest.Method.OPTIONS) {
                if (extractClientCertificate) {
                    putClientCertificateInContext(request, threadContext, logger);
                }
                service.authenticate(request);
            }

            RemoteHostHeader.process(request, threadContext);
        }

        filterChain.continueProcessing(request, channel, client);
    }

    static void putClientCertificateInContext(RestRequest request, ThreadContext threadContext, ESLogger logger) throws Exception {
        assert request instanceof Netty3HttpRequest;
        Netty3HttpRequest nettyHttpRequest = (Netty3HttpRequest) request;

        SslHandler handler = nettyHttpRequest.getChannel().getPipeline().get(SslHandler.class);
        assert handler != null;
        try {
            Certificate[] certs = handler.getEngine().getSession().getPeerCertificates();
            if (certs instanceof X509Certificate[]) {
                threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, certs);
            }
        } catch (SSLPeerUnverifiedException e) {
            // this happens when we only request client authentication and the client does not provide it
            if (logger.isTraceEnabled()) {
                logger.trace("SSL Peer did not present a certificate on channel [{}]", e, nettyHttpRequest.getChannel());
            } else if (logger.isDebugEnabled()) {
                logger.debug("SSL Peer did not present a certificate on channel [{}]", nettyHttpRequest.getChannel());
            }
        }
    }
}
