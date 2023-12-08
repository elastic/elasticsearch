/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;

import java.util.Map;
import java.util.Objects;

/**
 * Responsible for registering and recoding API key authentication metrics which track successful and failed authentication results.
 *
 * <p>
 * Note: The registration of {@link #METRIC_SUCCESS_COUNT} and {@link #METRIC_FAILURES_COUNT} is done during class instantiation.
 */
public final class ApiKeyAuthenticationMetrics {

    public static final String METRIC_SUCCESS_COUNT = "es.security.authc.api_keys.success.count";
    public static final String METRIC_FAILURES_COUNT = "es.security.authc.api_keys.failures.count";

    public static final String ATTRIBUTE_API_KEY_ID = "es.security.api_key_id";
    public static final String ATTRIBUTE_API_KEY_TYPE = "es.security.api_key_type";
    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = "es.security.authc_failure_reason";

    private final LongCounter successCounter;
    private final LongCounter failuresCounter;

    private ApiKeyAuthenticationMetrics(final LongCounter successCounter, final LongCounter failuresCounter) {
        this.successCounter = Objects.requireNonNull(successCounter);
        this.failuresCounter = Objects.requireNonNull(failuresCounter);
    }

    public ApiKeyAuthenticationMetrics(final MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(METRIC_SUCCESS_COUNT, "Number of successful API Key authentications.", "count"),
            meterRegistry.registerLongCounter(METRIC_FAILURES_COUNT, "Number of failed API Key authentications.", "count")
        );
    }

    /**
     * Records successful API key authentication.
     *
     * @param credentials The API key credentials used to store additional infos as attributes.
     */
    public void recordSuccessfulAuthentication(final ApiKeyCredentials credentials) {
        this.successCounter.incrementBy(
            1L,
            Map.ofEntries(
                Map.entry(ATTRIBUTE_API_KEY_ID, credentials.getId()),
                Map.entry(ATTRIBUTE_API_KEY_TYPE, credentials.getExpectedType().value())
            )
        );
    }

    /**
     * Records failed API key authentication.
     *
     * @param credentials  The API key credentials used to store additional infos as attributes.
     * @param failureReason The messages with the reason explaining why an API key authentication failed.
     */
    public void recordFailedAuthentication(final ApiKeyCredentials credentials, final String failureReason) {
        this.failuresCounter.incrementBy(
            1L,
            Map.ofEntries(
                Map.entry(ATTRIBUTE_API_KEY_ID, credentials.getId()),
                Map.entry(ATTRIBUTE_API_KEY_TYPE, credentials.getExpectedType().value()),
                Map.entry(ATTRIBUTE_AUTHC_FAILURE_REASON, failureReason != null ? failureReason : "")
            )
        );
    }

    @Override
    public String toString() {
        return "ApiKeyAuthenticationMetrics{" + "successes=" + successCounter + ", failures=" + failuresCounter + '}';
    }

}
