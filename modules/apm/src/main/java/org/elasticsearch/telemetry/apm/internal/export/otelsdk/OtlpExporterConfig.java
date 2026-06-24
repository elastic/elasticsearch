/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.sdk.common.export.RetryPolicy;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;

import java.time.Duration;

/**
 * Reads the shared OTLP exporter settings (timeouts, retry policy, authorization) once and applies
 * them to any of the three concrete exporter builders.
 *
 * <p>{@link #applyTo} exists in three near-identical overloads because
 * {@link OtlpGrpcLogRecordExporterBuilder}, {@link OtlpHttpMetricExporterBuilder}, and
 * {@link OtlpHttpSpanExporterBuilder} share no common supertype despite exposing the same methods.
 */
final class OtlpExporterConfig {

    private final Duration sendTimeout;
    private final Duration connectTimeout;
    private final RetryPolicy retryPolicy;
    private final String authorizationHeader;

    private OtlpExporterConfig(Duration sendTimeout, Duration connectTimeout, RetryPolicy retryPolicy, String authorizationHeader) {
        this.sendTimeout = sendTimeout;
        this.connectTimeout = connectTimeout;
        this.retryPolicy = retryPolicy;
        this.authorizationHeader = authorizationHeader;
    }

    static OtlpExporterConfig from(Settings settings) {
        return new OtlpExporterConfig(
            OtelSdkSettings.TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings).toDuration(),
            OtelSdkSettings.TELEMETRY_OTEL_OTLP_CONNECT_TIMEOUT.get(settings).toDuration(),
            buildRetryPolicy(settings),
            buildAuthorizationHeader(settings)
        );
    }

    RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    String authorizationHeader() {
        return authorizationHeader;
    }

    void applyTo(OtlpHttpMetricExporterBuilder b) {
        b.setTimeout(sendTimeout).setConnectTimeout(connectTimeout).setRetryPolicy(retryPolicy);
        if (authorizationHeader != null) {
            b.addHeader("Authorization", authorizationHeader);
        }
    }

    void applyTo(OtlpGrpcLogRecordExporterBuilder b) {
        b.setTimeout(sendTimeout).setConnectTimeout(connectTimeout).setRetryPolicy(retryPolicy);
        if (authorizationHeader != null) {
            b.addHeader("Authorization", authorizationHeader);
        }
    }

    void applyTo(OtlpHttpSpanExporterBuilder b) {
        b.setTimeout(sendTimeout).setConnectTimeout(connectTimeout).setRetryPolicy(retryPolicy);
        if (authorizationHeader != null) {
            b.addHeader("Authorization", authorizationHeader);
        }
    }

    private static RetryPolicy buildRetryPolicy(Settings settings) {
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

    private static String buildAuthorizationHeader(Settings settings) {
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
