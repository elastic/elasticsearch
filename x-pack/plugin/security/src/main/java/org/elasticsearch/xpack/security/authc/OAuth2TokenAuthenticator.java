/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.security.metric.InstrumentedSecurityActionListener;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;
import org.elasticsearch.xpack.security.metric.SecurityMetrics;

import java.util.Map;
import java.util.function.LongSupplier;

class OAuth2TokenAuthenticator implements Authenticator {

    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = "es.security.token_authc_failure_reason";

    private static final Logger logger = LogManager.getLogger(OAuth2TokenAuthenticator.class);

    private final SecurityMetrics<BearerToken> authenticationMetrics;
    private final TokenService tokenService;

    OAuth2TokenAuthenticator(TokenService tokenService, MeterRegistry meterRegistry) {
        this(tokenService, meterRegistry, System::nanoTime);
    }

    OAuth2TokenAuthenticator(TokenService tokenService, MeterRegistry meterRegistry, LongSupplier nanoTimeSupplier) {
        this.authenticationMetrics = new SecurityMetrics<>(
            SecurityMetricType.AUTHC_OAUTH2_TOKEN,
            meterRegistry,
            this::buildMetricAttributes,
            nanoTimeSupplier
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
        doAuthenticate(context, bearerToken, InstrumentedSecurityActionListener.wrapForAuthc(authenticationMetrics, bearerToken, listener));
    }

    private void doAuthenticate(Context context, BearerToken bearerToken, ActionListener<AuthenticationResult<Authentication>> listener) {
        tokenService.tryAuthenticateToken(bearerToken.credentials(), ActionListener.wrap(userToken -> {
            if (userToken != null) {
                listener.onResponse(AuthenticationResult.success(userToken.getAuthentication()));
            } else {
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
            listener.onFailure(e);
        }));
    }

    private Map<String, Object> buildMetricAttributes(BearerToken token, String failureReason) {
        if (failureReason != null) {
            return Map.of(ATTRIBUTE_AUTHC_FAILURE_REASON, failureReason);
        }
        return Map.of();
    }
}
