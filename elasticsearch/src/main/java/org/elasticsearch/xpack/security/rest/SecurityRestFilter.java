/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty3.Netty3HttpRequest;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilter;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.ssl.SSLService;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;

public class SecurityRestFilter extends RestFilter {

    private final AuthenticationService service;
    private final Logger logger;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
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
                service.authenticate(request).getUser();
            }

            RemoteHostHeader.process(request, threadContext);
        }

        filterChain.continueProcessing(request, channel, client);
    }

    static void putClientCertificateInContext(RestRequest request, ThreadContext threadContext, Logger logger) throws Exception {
        assert request instanceof Netty3HttpRequest || request instanceof Netty4HttpRequest;
        if (request instanceof Netty3HttpRequest) {
            Netty3HttpRequest nettyHttpRequest = (Netty3HttpRequest) request;

            SslHandler handler = nettyHttpRequest.getChannel().getPipeline().get(SslHandler.class);
            assert handler != null;
            extractClientCerts(handler.getEngine(), nettyHttpRequest.getChannel(), threadContext, logger);
        } else if (request instanceof Netty4HttpRequest) {
            Netty4HttpRequest nettyHttpRequest = (Netty4HttpRequest) request;

            io.netty.handler.ssl.SslHandler handler = nettyHttpRequest.getChannel().pipeline().get(io.netty.handler.ssl.SslHandler.class);
            assert handler != null;
            extractClientCerts(handler.engine(), nettyHttpRequest.getChannel(), threadContext, logger);
        }

    }

    private static void extractClientCerts(SSLEngine sslEngine, Object channel, ThreadContext threadContext, Logger logger) {
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
                        (Supplier<?>) () -> new ParameterizedMessage("SSL Peer did not present a certificate on channel [{}]", channel), e);
            } else if (logger.isDebugEnabled()) {
                logger.debug("SSL Peer did not present a certificate on channel [{}]", channel);
            }
        }
    }
}
