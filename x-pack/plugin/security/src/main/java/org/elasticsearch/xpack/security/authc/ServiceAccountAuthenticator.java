/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;

import java.util.HashMap;
import java.util.Map;

class ServiceAccountAuthenticator extends MetricsRecordingAuthenticator implements Authenticator {

    public static final String METRIC_SUCCESS_COUNT = "es.security.authc.service_account.success.count";
    public static final String METRIC_FAILURES_COUNT = "es.security.authc.service_account.failures.count";

    public static final String ATTRIBUTE_SERVICE_ACCOUNT_ID = "es.security.service_account_id";
    public static final String ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME = "es.security.service_account_token_name";
    public static final String ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_SOURCE = "es.security.service_account_token_source";
    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = "es.security.service_account_authc_failure_reason";

    private static final Logger logger = LogManager.getLogger(ServiceAccountAuthenticator.class);
    private final ServiceAccountService serviceAccountService;
    private final String nodeName;

    ServiceAccountAuthenticator(ServiceAccountService serviceAccountService, String nodeName, MeterRegistry meterRegistry) {
        super(
            meterRegistry.registerLongCounter(METRIC_SUCCESS_COUNT, "Number of successful service account authentications.", "count"),
            meterRegistry.registerLongCounter(METRIC_FAILURES_COUNT, "Number of failed service account authentications.", "count")
        );
        this.serviceAccountService = serviceAccountService;
        this.nodeName = nodeName;
    }

    @Override
    public String name() {
        return "service account";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final SecureString bearerString = context.getBearerString();
        if (bearerString == null) {
            return null;
        }
        return ServiceAccountService.tryParseToken(bearerString);
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof ServiceAccountToken) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final ServiceAccountToken serviceAccountToken = (ServiceAccountToken) authenticationToken;
        serviceAccountService.authenticateToken(serviceAccountToken, nodeName, ActionListener.wrap(authentication -> {
            assert authentication != null : "service account authenticate should return either authentication or call onFailure";
            recordSuccessfulAuthentication(buildSuccessAuthMetricAttributes(serviceAccountToken, authentication));
            listener.onResponse(AuthenticationResult.success(authentication));
        }, e -> {
            logger.debug(() -> "Failed to validate service account token for request [" + context.getRequest() + "]", e);
            recordFailedAuthentication(buildFailedAuthMetricAttributes(serviceAccountToken, e.getMessage()));
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, serviceAccountToken));
        }));
    }

    private Map<String, Object> buildSuccessAuthMetricAttributes(ServiceAccountToken serviceAccountToken, Authentication authentication) {
        return Map.ofEntries(
            Map.entry(ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal()),
            Map.entry(ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName()),
            Map.entry(
                ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_SOURCE,
                authentication.getEffectiveSubject().getMetadata().get(ServiceAccountSettings.TOKEN_SOURCE_FIELD)
            )
        );
    }

    private Map<String, Object> buildFailedAuthMetricAttributes(ServiceAccountToken serviceAccountToken, String failureReason) {
        final Map<String, Object> attributes = new HashMap<>(3);
        attributes.put(ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal());
        attributes.put(ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName());
        if (failureReason != null) {
            attributes.put(ATTRIBUTE_AUTHC_FAILURE_REASON, failureReason);
        }
        return attributes;
    }
}
