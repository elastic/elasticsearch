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

import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.OTLP_RETRY_INITIAL_BACKOFF;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_TRACING_MAX_QUEUE_SIZE;
import static org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE;
import static org.hamcrest.Matchers.equalTo;

public class OtelSdkSettingsTests extends ESTestCase {

    public void testExportIntervalGreaterThanSendTimeoutIsValid() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_EXPORT_INTERVAL.getKey(), TimeValue.timeValueMillis(500))
            .put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueMillis(300))
            .build();
        TELEMETRY_EXPORT_INTERVAL.get(settings);
    }

    public void testExportIntervalEqualToSendTimeoutIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_EXPORT_INTERVAL.getKey(), TimeValue.timeValueSeconds(5))
            .put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_EXPORT_INTERVAL.get(settings));
    }

    public void testExportIntervalLessThanSendTimeoutIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_EXPORT_INTERVAL.getKey(), TimeValue.timeValueSeconds(4))
            .put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_EXPORT_INTERVAL.get(settings));
    }

    public void testExportIntervalDefaultIsRejectedWhenSendTimeoutIsHigher() {
        Settings settings = Settings.builder().put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueSeconds(75)).build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_EXPORT_INTERVAL.get(settings));
    }

    public void testSendTimeoutGreaterThanInitialBackoffIsValid() {
        Settings settings = Settings.builder().put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueMillis(200)).build();
        TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings);
    }

    public void testSendTimeoutBelowInitialBackoffIsRejected() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_EXPORT_SEND_TIMEOUT.getKey(), TimeValue.timeValueMillis(OTLP_RETRY_INITIAL_BACKOFF.millis() - 1))
            .build();
        expectThrows(IllegalArgumentException.class, () -> TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings));
    }

    public void testDefaultValuesAreValid() {
        TELEMETRY_EXPORT_SEND_TIMEOUT.get(Settings.EMPTY);
        TELEMETRY_EXPORT_INTERVAL.get(Settings.EMPTY);
    }

    public void testExportIntervalFallsBackToAgentMetricsInterval() {
        Settings settings = Settings.builder().put("telemetry.agent.metrics_interval", "30s").build();
        assertThat(TELEMETRY_EXPORT_INTERVAL.get(settings), equalTo(TimeValue.timeValueSeconds(30)));
    }

    public void testExportIntervalPrefersOwnKeyOverAgentFallback() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_EXPORT_INTERVAL.getKey(), "20s")
            .put("telemetry.agent.metrics_interval", "30s")
            .build();
        assertThat(TELEMETRY_EXPORT_INTERVAL.get(settings), equalTo(TimeValue.timeValueSeconds(20)));
    }

    public void testSampleRateFallsBackToAgentTransactionSampleRate() {
        Settings settings = Settings.builder().put("telemetry.agent.transaction_sample_rate", "0.5").build();
        assertThat(TELEMETRY_TRACING_SAMPLE_RATE.get(settings), equalTo(0.5));
    }

    public void testSampleRateDefault() {
        assertThat(TELEMETRY_TRACING_SAMPLE_RATE.get(Settings.EMPTY), equalTo(0.001));
    }

    public void testMaxQueueSizeFallsBackToAgentMaxQueueSize() {
        Settings settings = Settings.builder().put("telemetry.agent.max_queue_size", "2048").build();
        assertThat(TELEMETRY_TRACING_MAX_QUEUE_SIZE.get(settings), equalTo(2048));
    }

    public void testMaxQueueSizeDefault() {
        assertThat(TELEMETRY_TRACING_MAX_QUEUE_SIZE.get(Settings.EMPTY), equalTo(1024));
    }
}
