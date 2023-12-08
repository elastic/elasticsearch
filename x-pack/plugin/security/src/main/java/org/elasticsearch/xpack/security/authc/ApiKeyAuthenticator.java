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
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

class ApiKeyAuthenticator extends MetricsRecordingAuthenticator implements Authenticator {

    public static final String METRIC_SUCCESS_COUNT = "es.security.authc.api_key.success.count";
    public static final String METRIC_FAILURES_COUNT = "es.security.authc.api_key.failures.count";

    public static final String ATTRIBUTE_API_KEY_ID = "es.security.api_key_id";
    public static final String ATTRIBUTE_API_KEY_TYPE = "es.security.api_key_type";
    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = "es.security.api_key_authc_failure_reason";

    private static final Logger logger = LogManager.getLogger(ApiKeyAuthenticator.class);

    private final ApiKeyService apiKeyService;
    private final String nodeName;

    ApiKeyAuthenticator(ApiKeyService apiKeyService, String nodeName, MeterRegistry meterRegistry) {
        super(
            meterRegistry.registerLongCounter(METRIC_SUCCESS_COUNT, "Number of successful API Key authentications.", "count"),
            meterRegistry.registerLongCounter(METRIC_FAILURES_COUNT, "Number of failed API Key authentications.", "count")
        );
        this.apiKeyService = apiKeyService;
        this.nodeName = nodeName;
    }

    @Override
    public String name() {
        return "API key";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final ApiKeyCredentials apiKeyCredentials = apiKeyService.parseCredentialsFromApiKeyString(context.getApiKeyString());
        assert apiKeyCredentials == null || apiKeyCredentials.getExpectedType() == ApiKey.Type.REST;
        return apiKeyCredentials;
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof ApiKeyCredentials) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        ApiKeyCredentials apiKeyCredentials = (ApiKeyCredentials) authenticationToken;
        apiKeyService.tryAuthenticate(context.getThreadContext(), apiKeyCredentials, ActionListener.wrap(authResult -> {
            if (authResult.isAuthenticated()) {
                final Authentication authentication = Authentication.newApiKeyAuthentication(authResult, nodeName);
                recordSuccessfulAuthentication(buildMetricAttributes(apiKeyCredentials));
                listener.onResponse(AuthenticationResult.success(authentication));
            } else if (authResult.getStatus() == AuthenticationResult.Status.TERMINATE) {
                Exception e = (authResult.getException() != null)
                    ? authResult.getException()
                    : Exceptions.authenticationError(authResult.getMessage());
                logger.debug(() -> "API key service terminated authentication for request [" + context.getRequest() + "]", e);
                context.getRequest().exceptionProcessingRequest(e, authenticationToken);
                recordFailedAuthentication(buildMetricAttributes(apiKeyCredentials, authResult.getMessage()));
                listener.onFailure(e);
            } else {
                if (authResult.getMessage() != null) {
                    if (authResult.getException() != null) {
                        logger.warn(
                            () -> format("Authentication using apikey failed - %s", authResult.getMessage()),
                            authResult.getException()
                        );
                    } else {
                        logger.warn("Authentication using apikey failed - {}", authResult.getMessage());
                    }
                }
                recordFailedAuthentication(buildMetricAttributes(apiKeyCredentials, authResult.getMessage()));
                listener.onResponse(AuthenticationResult.unsuccessful(authResult.getMessage(), authResult.getException()));
            }
        }, e -> {
            recordFailedAuthentication(buildMetricAttributes(apiKeyCredentials, e.getMessage()));
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null));
        }));
    }

    private Map<String, Object> buildMetricAttributes(ApiKeyCredentials credentials) {
        return buildMetricAttributes(credentials, null);
    }

    private Map<String, Object> buildMetricAttributes(ApiKeyCredentials credentials, String failureReason) {
        final Map<String, Object> attributes = new HashMap<>(3);
        attributes.put(ATTRIBUTE_API_KEY_ID, credentials.getId());
        attributes.put(ATTRIBUTE_API_KEY_TYPE, credentials.getExpectedType().value());
        if (failureReason != null) {
            attributes.put(ATTRIBUTE_AUTHC_FAILURE_REASON, failureReason);
        }
        return attributes;
    }
}
