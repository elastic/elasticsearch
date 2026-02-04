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
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.time.Duration;
import java.util.function.Supplier;

public class OtelSdkMeterSupplier implements Supplier<Meter> {
    private final Settings settings;
    private SdkMeterProvider meterProvider;
    private RuntimeMetrics runtimeMetrics;

    OtelSdkMeterSupplier(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Meter get() {
        if (meterProvider == null) {
            var exporter = createOTLPExporter();
            TimeValue intervalTimeValue = settings.getAsTime("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(10));
            var reader = PeriodicMetricReader.builder(exporter).setInterval(Duration.ofMillis(intervalTimeValue.millis())).build();
            meterProvider = SdkMeterProvider.builder()
                .setResource(Resource.builder().put("service.name", "elasticsearch").build())
                .registerMetricReader(reader)
                .build();
            var otelSdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
            runtimeMetrics = RuntimeMetrics.builder(otelSdk).enableAllFeatures().emitExperimentalTelemetry().build();
        }
        return meterProvider.get("elasticsearch");
    }

    private OtlpHttpMetricExporter createOTLPExporter() {
        String serverUrl = APMAgentSettings.APM_AGENT_SETTINGS.getConcreteSetting("telemetry.agent.server_url").get(settings);
        if (serverUrl == null || serverUrl.isEmpty()) {
            throw new IllegalStateException("telemetry.otel.metrics.enabled=true requires telemetry.agent.server_url to be configured");
        }
        String endpoint = serverUrl + (serverUrl.endsWith("/") ? "" : "/") + "v1/metrics";
        OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder().setEndpoint(endpoint);
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

    void close() {
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
