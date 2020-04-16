/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest.GrantType;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken;

import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Transport action responsible for creating a token based on a request. Requests provide user
 * credentials that can be different than those of the user that is currently authenticated so we
 * always re-authenticate within this action. This authenticated user will be the user that the
 * token represents
 */
public final class TransportCreateTokenAction extends HandledTransportAction<CreateTokenRequest, CreateTokenResponse> {

    private static final String DEFAULT_SCOPE = "full";
    private final ThreadPool threadPool;
    private final TokenService tokenService;
    private final AuthenticationService authenticationService;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateTokenAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                      TokenService tokenService, AuthenticationService authenticationService,
                                      SecurityContext securityContext) {
        super(CreateTokenAction.NAME, transportService, actionFilters, CreateTokenRequest::new);
        this.threadPool = threadPool;
        this.tokenService = tokenService;
        this.authenticationService = authenticationService;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(Task task, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        CreateTokenRequest.GrantType type = CreateTokenRequest.GrantType.fromString(request.getGrantType());
        assert type != null : "type should have been validated in the action";
        switch (type) {
            case PASSWORD:
            case KERBEROS:
                authenticateAndCreateToken(type, request, listener);
                break;
            case CLIENT_CREDENTIALS:
                Authentication authentication = securityContext.getAuthentication();
                createToken(type, request, authentication, authentication, false, listener);
                break;
            default:
                listener.onFailure(new IllegalStateException("grant_type [" + request.getGrantType() +
                    "] is not supported by the create token action"));
                break;
        }
    }

    private void authenticateAndCreateToken(GrantType grantType, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        Authentication originatingAuthentication = securityContext.getAuthentication();
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            final AuthenticationToken authToken = extractAuthenticationToken(grantType, request, listener);
            if (authToken == null) {
                listener.onFailure(new IllegalStateException(
                        "grant_type [" + request.getGrantType() + "] is not supported by the create token action"));
                return;
            }

            authenticationService.authenticate(CreateTokenAction.NAME, request, authToken,
                ActionListener.wrap(authentication -> {
                    clearCredentialsFromRequest(grantType, request);

                    if (authentication != null) {
                        createToken(grantType, request, authentication, originatingAuthentication, true, listener);
                    } else {
                        listener.onFailure(new UnsupportedOperationException("cannot create token if authentication is not allowed"));
                    }
                }, e -> {
                    clearCredentialsFromRequest(grantType, request);
                    listener.onFailure(e);
                }));
        }
    }

    private AuthenticationToken extractAuthenticationToken(GrantType grantType, CreateTokenRequest request,
            ActionListener<CreateTokenResponse> listener) {
        AuthenticationToken authToken = null;
        if (grantType == GrantType.PASSWORD) {
            authToken = new UsernamePasswordToken(request.getUsername(), request.getPassword());
        } else if (grantType == GrantType.KERBEROS) {
            SecureString kerberosTicket = request.getKerberosTicket();
            String base64EncodedToken = kerberosTicket.toString();
            byte[] decodedKerberosTicket = null;
            try {
                decodedKerberosTicket = Base64.getDecoder().decode(base64EncodedToken);
            } catch (IllegalArgumentException iae) {
                listener.onFailure(new UnsupportedOperationException("could not decode base64 kerberos ticket " + base64EncodedToken));
            }
            authToken = new KerberosAuthenticationToken(decodedKerberosTicket);
        }
        return authToken;
    }

    private void clearCredentialsFromRequest(GrantType grantType, CreateTokenRequest request) {
        if (grantType == GrantType.PASSWORD) {
            request.getPassword().close();
        } else if (grantType == GrantType.KERBEROS) {
            request.getKerberosTicket().close();
        }
    }

    private void createToken(GrantType grantType, CreateTokenRequest request, Authentication authentication, Authentication originatingAuth,
            boolean includeRefreshToken, ActionListener<CreateTokenResponse> listener) {
        tokenService.createOAuth2Tokens(authentication, originatingAuth, Collections.emptyMap(), includeRefreshToken,
                ActionListener.wrap(tuple -> {
                    final String scope = getResponseScopeValue(request.getScope());
                    final String base64AuthenticateResponse = (grantType == GrantType.KERBEROS) ? extractOutToken() : null;
                    final CreateTokenResponse response = new CreateTokenResponse(tuple.v1(), tokenService.getExpirationDelay(), scope,
                            tuple.v2(), base64AuthenticateResponse);
                    listener.onResponse(response);
                }, listener::onFailure));
    }

    private String extractOutToken() {
        List<String> values = threadPool.getThreadContext().getResponseHeaders().get(KerberosAuthenticationToken.WWW_AUTHENTICATE);
        if (values != null && values.size() == 1) {
            final String wwwAuthenticateHeaderValue = values.get(0);
            // it may contain base64 encoded token that needs to be sent to client if mutual auth was requested
            if (wwwAuthenticateHeaderValue.startsWith(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX)) {
                final String base64EncodedToken = wwwAuthenticateHeaderValue
                        .substring(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX.length()).trim();
                return base64EncodedToken;
            }
        }
        threadPool.getThreadContext().getResponseHeaders().remove(KerberosAuthenticationToken.WWW_AUTHENTICATE);
        return null;
    }

    static String getResponseScopeValue(String requestScope) {
        final String scope;
        // the OAuth2.0 RFC requires the scope to be provided in the
        // response if it differs from the user provided scope. If the
        // scope was not provided then it does not need to be returned.
        // if the scope is not supported, the value of the scope that the
        // token is for must be returned
        if (requestScope != null) {
            scope = DEFAULT_SCOPE; // this is the only non-null value that is currently supported
        } else {
            scope = null;
        }
        return scope;
    }
}
