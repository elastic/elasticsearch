/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_OTEL_METRICS_INTERVAL;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_OTEL_OTLP_SEND_TIMEOUT;

public class OtelSdkSettingsTests extends ESTestCase {

    public void testSendTimeoutGreaterThanInitialBackoffIsValid() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(2))
            .put(TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings);
    }

    public void testSendTimeoutEqualToInitialBackoffIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(1))
            .put(TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings));
    }

    public void testSendTimeoutLessThanInitialBackoffIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueMillis(500))
            .put(TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings));
    }

    public void testSendTimeoutDefaultIsRejectedWhenInitialBackoffIsHigher() {
        // send_timeout defaults to 5s
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_OTLP_RETRY_INITIAL_BACKOFF.getKey(), TimeValue.timeValueSeconds(10))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings));
    }

    public void testMetricsIntervalGreaterThanSendTimeoutIsValid() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_METRICS_INTERVAL.getKey(), TimeValue.timeValueMillis(500))
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueMillis(300))
            .build();
        TELEMETRY_OTEL_METRICS_INTERVAL.get(settings);
    }

    public void testMetricsIntervalEqualToSendTimeoutIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_METRICS_INTERVAL.getKey(), TimeValue.timeValueSeconds(5))
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_METRICS_INTERVAL.get(settings));
    }

    public void testMetricsIntervalLessThanSendTimeoutIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_OTEL_METRICS_INTERVAL.getKey(), TimeValue.timeValueSeconds(4))
            .put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_METRICS_INTERVAL.get(settings));
    }

    public void testMetricsIntervalDefaultIsRejectedWhenSendTimeoutIsHigher() {
        // metrics.interval defaults to 10s
        Settings settings = Settings.builder().put(TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(15)).build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_OTEL_METRICS_INTERVAL.get(settings));
    }

    public void testDefaultValuesAreValid() {
        TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(Settings.EMPTY);
        TELEMETRY_OTEL_METRICS_INTERVAL.get(Settings.EMPTY);
    }
}
