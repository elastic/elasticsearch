/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.http.nio.NioHttpRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.FilterRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;

import java.io.IOException;

public class SecurityRestFilter extends FilterRestHandler implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final AuthenticationService authenticationService;

    private final SecondaryAuthenticator secondaryAuthenticator;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;

    public SecurityRestFilter(
        XPackLicenseState licenseState,
        AuthenticationService authenticationService,
        SecondaryAuthenticator secondaryAuthenticator,
        AuditTrailService auditTrailService,
        RestHandler restHandler
    ) {
        super(restHandler);
        this.licenseState = licenseState;
        this.authenticationService = authenticationService;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.auditTrailService = auditTrailService;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (licenseState.isSecurityEnabled() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request

            final String requestUri = request.uri();
            final RestRequest wrappedRequest = maybeWrapRestRequest(request);
            authenticateOnlyNioHttpRequests(wrappedRequest.getHttpRequest(), ActionListener.wrap(ignored -> {
                auditTrailService.get().authenticationSuccess(wrappedRequest);
                secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
                    if (secondaryAuthentication != null) {
                        logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, requestUri);
                    }
                    getDelegate().handleRequest(request, channel, client);
                }, e -> handleException("Secondary authentication", request, channel, e)));
            }, e -> handleException("Authentication", request, channel, e)));
        } else {
            // requests with the OPTIONS method should be handled elsewhere, and not by calling {@code RestHandler#handleRequest}
            // authn is bypassed for HTTP reqs with the OPTIONS method, so this sanity check prevents dispatching unauthenticated reqs
            if (licenseState.isSecurityEnabled()) {
                handleException(
                    "Options with body",
                    request,
                    channel,
                    new ElasticsearchSecurityException("Cannot dispatch OPTIONS request, as they are not authenticated")
                );
                return;
            }
            if (request.method() != Method.OPTIONS) {
                HeaderWarning.addWarning(
                    "Elasticsearch built-in security features are not enabled. Without "
                        + "authentication, your cluster could be accessible to anyone. See "
                        + "https://www.elastic.co/guide/en/elasticsearch/reference/"
                        + Version.CURRENT.major
                        + "."
                        + Version.CURRENT.minor
                        + "/security-minimal-setup.html to enable security."
                );
            }
            getDelegate().handleRequest(request, channel, client);
        }
    }

    private void authenticateOnlyNioHttpRequests(HttpRequest request, ActionListener<Void> listener) {
        if (request instanceof HttpPipelinedRequest) {
            request = ((HttpPipelinedRequest) request).getDelegateRequest();
        }
        if (request instanceof NioHttpRequest) {
            this.authenticationService.authenticate(
                request,
                ActionListener.wrap(ignored -> listener.onResponse(null), listener::onFailure)
            );
        } else {
            assert request instanceof Netty4HttpRequest;
            // this type of request is authenticated elsewhere, see: {@code Security#getHttpServerTransportWithHeadersValidator}
            listener.onResponse(null);
        }
    }

    private void handleException(String actionType, RestRequest request, RestChannel channel, Exception e) {
        logger.debug(new ParameterizedMessage("{} failed for REST request [{}]", actionType, request.uri()), e);
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error(
                (Supplier<?>) () -> new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()),
                inner
            );
        }
    }

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        final RestHandler handler = getConcreteRestHandler();
        if (handler instanceof RestRequestFilter) {
            return ((RestRequestFilter) handler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }
}
