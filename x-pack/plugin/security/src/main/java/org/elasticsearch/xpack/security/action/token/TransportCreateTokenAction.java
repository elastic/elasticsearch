/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
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
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
    public TransportCreateTokenAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        TokenService tokenService,
        AuthenticationService authenticationService,
        SecurityContext securityContext
    ) {
        super(CreateTokenAction.NAME, transportService, actionFilters, CreateTokenRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
            case PASSWORD, KERBEROS, USER_MANAGED_SERVICE_ACCOUNT -> authenticateAndCreateToken(type, request, listener);
            case CLIENT_CREDENTIALS -> {
                Authentication authentication = securityContext.getAuthentication();
                if (authentication.isServiceAccount()) {
                    // Service account itself cannot create OAuth2 tokens.
                    listener.onFailure(
                        new ElasticsearchSecurityException(
                            "OAuth2 token creation is not supported for service accounts",
                            RestStatus.BAD_REQUEST
                        )
                    );
                    return;
                }
                createToken(type, request, authentication, authentication, false, listener);
            }
            default -> listener.onFailure(
                new IllegalStateException("grant_type [" + request.getGrantType() + "] is not supported by the create token action")
            );
        }
    }

    private void authenticateAndCreateToken(GrantType grantType, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        Authentication originatingAuthentication = securityContext.getAuthentication();
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            final Tuple<AuthenticationToken, Optional<Exception>> tokenAndException = extractAuthenticationToken(grantType, request);
            if (tokenAndException.v2().isPresent()) {
                listener.onFailure(tokenAndException.v2().get());
                return;
            }
            final AuthenticationToken authToken = tokenAndException.v1();
            if (authToken == null) {
                listener.onFailure(
                    new IllegalStateException("grant_type [" + request.getGrantType() + "] is not supported by the create token action")
                );
                return;
            }

            authenticationService.authenticate(CreateTokenAction.NAME, request, authToken, ActionListener.wrap(authentication -> {
                clearCredentials(grantType, request, authToken);

                if (authentication != null) {
                    if (grantType == GrantType.USER_MANAGED_SERVICE_ACCOUNT && false == authentication.isUserManagedServiceAccount()) {
                        // The credential authenticated, but not as a user-managed service account. In particular, built-in service
                        // accounts (e.g. elastic/kibana) must not be able to derive OAuth2 tokens through this grant.
                        listener.onFailure(invalidGrantException("service_account_token must belong to a user-managed service account"));
                        return;
                    }
                    // The user-managed service account grant deliberately does not issue a refresh token: each new access token
                    // requires re-presenting the service account credential, so that account existence, enabled status and token
                    // validity are re-verified on every exchange.
                    final boolean includeRefreshToken = grantType != GrantType.USER_MANAGED_SERVICE_ACCOUNT;
                    createToken(grantType, request, authentication, originatingAuthentication, includeRefreshToken, listener);
                } else {
                    listener.onFailure(new UnsupportedOperationException("cannot create token if authentication is not allowed"));
                }
            }, e -> {
                clearCredentials(grantType, request, authToken);
                listener.onFailure(e);
            }));
        }
    }

    private static Tuple<AuthenticationToken, Optional<Exception>> extractAuthenticationToken(
        GrantType grantType,
        CreateTokenRequest request
    ) {
        AuthenticationToken authToken = null;
        if (grantType == GrantType.PASSWORD) {
            authToken = new UsernamePasswordToken(request.getUsername(), request.getPassword());
        } else if (grantType == GrantType.KERBEROS) {
            SecureString kerberosTicket = request.getKerberosTicket();
            String base64EncodedToken = kerberosTicket.toString();
            byte[] decodedKerberosTicket;
            try {
                decodedKerberosTicket = Base64.getDecoder().decode(base64EncodedToken);
            } catch (IllegalArgumentException iae) {
                return new Tuple<>(
                    null,
                    Optional.of(new UnsupportedOperationException("could not decode base64 kerberos ticket " + base64EncodedToken, iae))
                );
            }
            authToken = new KerberosAuthenticationToken(decodedKerberosTicket);
        } else if (grantType == GrantType.USER_MANAGED_SERVICE_ACCOUNT) {
            // Parsing does not validate the credential; the token is authenticated through the regular service account
            // authentication path below, so account existence, enabled status and secret validity are all re-checked.
            authToken = ServiceAccountService.tryParseToken(request.getServiceAccountToken());
            if (authToken == null) {
                request.getServiceAccountToken().close();
                return new Tuple<>(null, Optional.of(invalidGrantException("service_account_token is not a valid service account token")));
            }
        }
        return new Tuple<>(authToken, Optional.empty());
    }

    private static void clearCredentials(GrantType grantType, CreateTokenRequest request, AuthenticationToken authToken) {
        // Kerberos and service account tokens hold a decoded copy of the credential, separate from the request field
        authToken.clearCredentials();
        if (grantType == GrantType.PASSWORD) {
            request.getPassword().close();
        } else if (grantType == GrantType.KERBEROS) {
            request.getKerberosTicket().close();
        } else if (grantType == GrantType.USER_MANAGED_SERVICE_ACCOUNT) {
            request.getServiceAccountToken().close();
        }
    }

    /**
     * Creates an {@link ElasticsearchSecurityException} in the shape that {@code RestGetTokenAction} translates into
     * an RFC 6749 {@code invalid_grant} error response.
     */
    private static ElasticsearchSecurityException invalidGrantException(String detail) {
        ElasticsearchSecurityException e = new ElasticsearchSecurityException("invalid_grant", RestStatus.BAD_REQUEST);
        e.addBodyHeader("error_description", detail);
        return e;
    }

    private void createToken(
        GrantType grantType,
        CreateTokenRequest request,
        Authentication authentication,
        Authentication originatingAuth,
        boolean includeRefreshToken,
        ActionListener<CreateTokenResponse> listener
    ) {
        tokenService.createOAuth2Tokens(
            authentication,
            originatingAuth,
            Collections.emptyMap(),
            includeRefreshToken,
            ActionListener.wrap(tokenResult -> {
                final String scope = getResponseScopeValue(request.getScope());
                final String base64AuthenticateResponse = (grantType == GrantType.KERBEROS) ? extractOutToken() : null;
                final CreateTokenResponse response = new CreateTokenResponse(
                    tokenResult.getAccessToken(),
                    tokenService.getExpirationDelay(),
                    scope,
                    tokenResult.getRefreshToken(),
                    base64AuthenticateResponse,
                    authentication
                );
                listener.onResponse(response);
            }, listener::onFailure)
        );
    }

    private String extractOutToken() {
        List<String> values = threadPool.getThreadContext().getResponseHeaders().get(KerberosAuthenticationToken.WWW_AUTHENTICATE);
        if (values != null && values.size() == 1) {
            final String wwwAuthenticateHeaderValue = values.get(0);
            // it may contain base64 encoded token that needs to be sent to client if mutual auth was requested
            if (wwwAuthenticateHeaderValue.startsWith(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX)) {
                final String base64EncodedToken = wwwAuthenticateHeaderValue.substring(
                    KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX.length()
                ).trim();
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
