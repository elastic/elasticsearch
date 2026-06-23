/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.export.RetryPolicy;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;

/** Shared helpers for building OTLP exporter configuration from node settings. */
final class OtlpExporterUtils {

    private OtlpExporterUtils() {}

    /**
     * Builds a {@link RetryPolicy} from the shared OTLP retry settings, or returns {@code null}
     * when {@link OtelSdkSettings#TELEMETRY_OTEL_OTLP_RETRY_MAX_ATTEMPTS} is {@code 1} (no retry).
     */
    static RetryPolicy buildRetryPolicy(Settings settings) {
        int maxAttempts = OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_MAX_ATTEMPTS.get(settings);
        if (maxAttempts <= 1) {
            return null;
        }
        return RetryPolicy.builder()
            .setMaxAttempts(maxAttempts)
            .setInitialBackoff(OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.get(settings).toDuration())
            .setBackoffMultiplier(OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_BACKOFF_MULTIPLIER.get(settings))
            .build();
    }

    /**
     * Returns an {@code Authorization} header value derived from the configured APM API key or
     * secret token, or {@code null} if neither is set.
     */
    static String buildOtlpAuthorizationHeader(Settings settings) {
        try (SecureString apiKey = APMAgentSettings.TELEMETRY_API_KEY_SETTING.get(settings)) {
            if (apiKey.isEmpty() == false) {
                return "ApiKey " + apiKey;
            }
        }
        try (SecureString secretToken = APMAgentSettings.TELEMETRY_SECRET_TOKEN_SETTING.get(settings)) {
            if (secretToken.isEmpty() == false) {
                return "Bearer " + secretToken;
            }
        }
        return null;
    }
}
