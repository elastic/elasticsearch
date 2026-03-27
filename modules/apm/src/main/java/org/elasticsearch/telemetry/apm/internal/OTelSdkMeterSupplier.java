/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.time.Duration;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

public class OTelSdkMeterSupplier implements MeterSupplier {
    private final Settings settings;
    private volatile SdkMeterProvider meterProvider;
    private volatile RuntimeMetrics runtimeMetrics;
    private static final Object mutex = new Object();

    OTelSdkMeterSupplier(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Meter get() {
        synchronized (mutex) {
            if (meterProvider == null) {
                var exporter = createOTLPExporter();
                TimeValue intervalTimeValue = OTelSdkSettings.TELEMETRY_OTEL_METRICS_INTERVAL.get(settings);
                var reader = PeriodicMetricReader.builder(exporter).setInterval(Duration.ofMillis(intervalTimeValue.millis())).build();
                meterProvider = SdkMeterProvider.builder()
                    .setResource(Resource.builder().put("service.name", "elasticsearch").build())
                    .registerMetricReader(reader)
                    .build();
                if (OTelSdkSettings.TELEMETRY_OTEL_METRICS_ENABLED.get(settings)) {
                    var otelSdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
                    // RuntimeMetrics uses two underlying implementations to gather the full set of metric data, JFR and JMX.
                    // The metrics gathered by the two implementations are mutually exclusive and the union of them produces the full
                    // set of available metrics. See more at: https://ela.st/otel-runtime-telemetry
                    runtimeMetrics = RuntimeMetrics.builder(otelSdk).build();
                }
            }
            return meterProvider.get("elasticsearch");
        }
    }

    private OtlpHttpMetricExporter createOTLPExporter() {
        String endpoint = OTelSdkSettings.TELEMETRY_OTEL_METRICS_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(
                OTEL_METRICS_ENABLED_SYSTEM_PROPERTY + "=true requires telemetry.otel.metrics.endpoint to be configured"
            );
        }
        OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder()
            .setEndpoint(endpoint)
            .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred());
        String authHeader = getAuthorizationHeader();
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        return builder.build();
    }

    private String getAuthorizationHeader() {
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

    @Override
    public void close() {
        synchronized (mutex) {
            if (runtimeMetrics != null) {
                runtimeMetrics.close();
                runtimeMetrics = null;
            }
            if (meterProvider != null) {
                meterProvider.close();
                meterProvider = null;
            }
        }
    }
}
