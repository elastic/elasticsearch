/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;

import java.util.Map;

class OAuth2TokenAuthenticator extends MetricsRecordingAuthenticator implements Authenticator {

    public static final String METRIC_SUCCESS_COUNT = "es.security.authc.token.success.count";
    public static final String METRIC_FAILURES_COUNT = "es.security.authc.token.failures.count";

    public static final String ATTRIBUTE_TOKEN_ID = "es.security.token_id";
    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = "es.security.token_authc_failure_reason";

    private static final Logger logger = LogManager.getLogger(OAuth2TokenAuthenticator.class);
    private final TokenService tokenService;

    OAuth2TokenAuthenticator(TokenService tokenService, MeterRegistry meterRegistry) {
        super(
            meterRegistry.registerLongCounter(METRIC_SUCCESS_COUNT, "Number of successful OAuth2 token authentications.", "count"),
            meterRegistry.registerLongCounter(METRIC_FAILURES_COUNT, "Number of failed OAuth2 token authentications.", "count")
        );
        this.tokenService = tokenService;
    }

    @Override
    public String name() {
        return "oauth2 token";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final SecureString bearerString = context.getBearerString();
        return bearerString == null ? null : new BearerToken(bearerString);
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof BearerToken) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final BearerToken bearerToken = (BearerToken) authenticationToken;
        tokenService.tryAuthenticateToken(bearerToken.credentials(), ActionListener.wrap(userToken -> {
            if (userToken != null) {
                recordSuccessfulAuthentication();
                listener.onResponse(AuthenticationResult.success(userToken.getAuthentication()));
            } else {
                recordFailedAuthentication(Map.of(ATTRIBUTE_AUTHC_FAILURE_REASON, "invalid token"));
                listener.onResponse(AuthenticationResult.unsuccessful("invalid token", null));
            }
        }, e -> {
            logger.debug(() -> "Failed to validate token authentication for request [" + context.getRequest() + "]", e);
            if (e instanceof ElasticsearchSecurityException
                && false == TokenService.isExpiredTokenException((ElasticsearchSecurityException) e)) {
                // intentionally ignore the returned exception; we call this primarily
                // for the auditing as we already have a purpose built exception
                context.getRequest().tamperedRequest();
            }
            recordFailedAuthentication(Map.of(ATTRIBUTE_AUTHC_FAILURE_REASON, e.getMessage()));
            listener.onFailure(e);
        }));
    }
}
