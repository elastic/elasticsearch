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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.MediaTypeRegistry;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.transport.SSLEngineUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService authenticationService;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final Settings settings;
    private final ThreadContext threadContext;
    private final boolean extractClientCertificate;

    public enum ActionType {
        Authentication("Authentication"),
        SecondaryAuthentication("Secondary authentication"),
        RequestHandling("Request handling");

        private final String name;
        ActionType(String name) { this.name = name; }
        @Override
        public String toString() { return name; }
    }

    public SecurityRestFilter(Settings settings, ThreadContext threadContext, AuthenticationService authenticationService,
                              SecondaryAuthenticator secondaryAuthenticator, RestHandler restHandler, boolean extractClientCertificate) {
        this.settings = settings;
        this.threadContext = threadContext;
        this.authenticationService = authenticationService;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.restHandler = restHandler;
        this.extractClientCertificate = extractClientCertificate;
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return restHandler.allowSystemIndexAccessByDefault();
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (request.method() == Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            restHandler.handleRequest(request, channel, client);
            return;
        }

        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            if (extractClientCertificate) {
                HttpChannel httpChannel = request.getHttpChannel();
                SSLEngineUtils.extractClientCertificates(logger, threadContext, httpChannel);
            }

            final String requestUri = request.uri();
            authenticationService.authenticate(maybeWrapRestRequest(request), ActionListener.wrap(
                authentication -> {
                    if (authentication == null) {
                        logger.trace("No authentication available for REST request [{}]", requestUri);
                    } else {
                        logger.trace("Authenticated REST request [{}] as {}", requestUri, authentication);
                    }
                    secondaryAuthenticator.authenticateAndAttachToContext(request, ActionListener.wrap(
                        secondaryAuthentication -> {
                            if (secondaryAuthentication != null) {
                                logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, requestUri);
                            }
                            RemoteHostHeader.process(request, threadContext);
                            try {
                                restHandler.handleRequest(request, channel, client);
                            } catch (Exception e) {
                                handleException(ActionType.RequestHandling, request, channel, e);
                            }
                        },
                        e -> handleException(ActionType.SecondaryAuthentication, request, channel, e)));
                }, e -> handleException(ActionType.Authentication, request, channel, e)));
        } else {
            restHandler.handleRequest(request, channel, client);
        }
    }

    protected void handleException(ActionType actionType, RestRequest request, RestChannel channel, Exception e) {
        logger.debug(new ParameterizedMessage("{} failed for REST request [{}]", actionType, request.uri()), e);
        final RestStatus restStatus = ExceptionsHelper.status(e);
        try {
            channel.sendResponse(new BytesRestResponse(channel, restStatus, e) {

                @Override
                protected boolean skipStackTrace() { return restStatus == RestStatus.UNAUTHORIZED; }

                @Override
                public Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
                    if (actionType != ActionType.RequestHandling
                        || (restStatus == RestStatus.UNAUTHORIZED || restStatus == RestStatus.FORBIDDEN)) {
                        if (headers.containsKey("Warning")) {
                            headers = Maps.copyMapWithRemovedEntry(headers, "Warning");
                        }
                        if (headers.containsKey("X-elastic-product")) {
                            headers = Maps.copyMapWithRemovedEntry(headers, "X-elastic-product");
                        }
                    }
                    return headers;
                }

            });
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error((Supplier<?>) () ->
                new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
        }
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return restHandler.canTripCircuitBreaker();
    }

    @Override
    public boolean supportsContentStream() {
        return restHandler.supportsContentStream();
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return restHandler.allowsUnsafeBuffers();
    }

    @Override
    public List<Route> routes() {
        return restHandler.routes();
    }

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter)restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }

    @Override
    public MediaTypeRegistry<? extends MediaType> validAcceptMediaTypes() {
        return restHandler.validAcceptMediaTypes();
    }
}
