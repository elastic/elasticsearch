/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportTracerSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Owns the lifecycle of the {@link APMTracer}, and applies the changes of the tracing settings to it. Analogous to
 * {@link APMMeterService} (metrics) and {@link APMLoggingService} (logs).
 */
public class APMTracingService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(APMTracingService.class);

    private final TraceSupplier traceSupplier;

    private final AtomicBoolean enabled = new AtomicBoolean();
    private final AtomicBoolean recordExceptionStacks = new AtomicBoolean();
    private final APMTracer tracer;

    public APMTracingService(ThreadContext threadContext, Settings settings, Supplier<MeterProvider> meterProvider) {
        this(threadContext, settings, new OtelSdkExportTracerSupplier(settings, meterProvider));
    }

    // visible for testing
    public APMTracingService(ThreadContext threadContext, Settings settings, TraceSupplier traceSupplier) {
        this.traceSupplier = traceSupplier;
        this.enabled.set(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.get(settings));
        this.recordExceptionStacks.set(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.get(settings));
        this.tracer = new APMTracer(threadContext, traceSupplier, enabled, recordExceptionStacks);
        this.tracer.setIncludeNames(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.get(settings));
        this.tracer.setExcludeNames(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.get(settings));
        this.tracer.setLabelFilters(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(settings));
        this.tracer.setMaxTraceDepth(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.get(settings));
        this.tracer.setMaxSpans(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.get(settings));
    }

    public APMTracer tracer() {
        return tracer;
    }

    public CompletableResultCode attemptFlushTraces() {
        if (enabled.get()) {
            return traceSupplier.attemptFlushTraces();
        }
        return CompletableResultCode.ofSuccess();
    }

    public void addClusterSettingsListeners(ClusterService clusterService) {
        tracer.setClusterName(clusterService.getClusterName().value());
        tracer.setNodeName(clusterService.getNodeName());

        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING, enabled::set);
        clusterSettings.addSettingsUpdateConsumer(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING, tracer::setIncludeNames);
        clusterSettings.addSettingsUpdateConsumer(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING, tracer::setExcludeNames);
        clusterSettings.addSettingsUpdateConsumer(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH, tracer::setMaxTraceDepth);
        clusterSettings.addSettingsUpdateConsumer(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS, tracer::setMaxSpans);
        clusterSettings.addSettingsUpdateConsumer(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES, tracer::setLabelFilters);
        clusterSettings.addSettingsUpdateConsumer(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS, recordExceptionStacks::set);
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        if (enabled.get()) {
            try {
                traceSupplier.attemptFlushTraces().join(OtelSdkSettings.OTEL_EXPORT_FLUSH_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.warn("Exception flushing trace supplier", e);
            }
        }
    }

    @Override
    protected void doClose() {
        enabled.set(false);
        try {
            traceSupplier.close();
        } catch (Exception e) {
            logger.warn("Exception closing trace supplier", e);
        }
    }
}
