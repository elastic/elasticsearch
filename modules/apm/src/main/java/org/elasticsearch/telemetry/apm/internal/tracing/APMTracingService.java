/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportTracerSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Owns the node's tracing runtime; instrumentation uses OpenTelemetry, not this lifecycle service. */
public final class APMTracingService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(APMTracingService.class);
    private static final Map<String, String> LEGACY_HTTP_NAMES = Map.of(
        "http.request.method",
        "http.method",
        "http.response.status_code",
        "http.status_code",
        "url.full",
        "http.url",
        "url.path",
        "http.url",
        "url.query",
        "http.url",
        "network.protocol.version",
        "http.flavour"
    );
    private final TraceSupplier supplier;
    private final OpenTelemetry openTelemetry;
    private volatile boolean enabled;
    private volatile boolean closed;
    private volatile int maxDepth;
    private volatile boolean recordExceptionStacks;
    private volatile CharacterRunAutomaton includeFilter;
    private volatile CharacterRunAutomaton excludeFilter;
    private volatile Redaction redaction;
    private volatile String nodeName;
    private volatile String clusterName;

    public APMTracingService(Settings settings, Supplier<MeterProvider> meterProvider) {
        this(settings, new OtelSdkExportTracerSupplier(settings, meterProvider));
    }

    APMTracingService(Settings settings, TraceSupplier supplier) {
        this.supplier = supplier;
        enabled = APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.get(settings);
        maxDepth = OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.get(settings);
        recordExceptionStacks = OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.get(settings);
        setIncludeNames(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.get(settings));
        setExcludeNames(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.get(settings));
        setLabelFilters(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(settings));
        nodeName = settings.get("node.name", "");
        clusterName = settings.get("cluster.name", "elasticsearch");
        if (supplier instanceof OtelSdkExportTracerSupplier sdkSupplier) {
            sdkSupplier.setAttributeSanitizer(this::sanitize);
        }
        openTelemetry = new PolicyOpenTelemetry(this);
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    public void setEnabled(boolean enabled) {
        if (closed) {
            return;
        }
        if (enabled) {
            supplier.get();
        }
        this.enabled = enabled;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setMaxTraceDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public void setRecordExceptionStacks(boolean enabled) {
        recordExceptionStacks = enabled;
    }

    public void setIncludeNames(List<String> names) {
        includeFilter = compile(names);
    }

    public void setExcludeNames(List<String> names) {
        excludeFilter = compile(names);
    }

    public void setLabelFilters(List<String> names) {
        redaction = new Redaction(compile(names), compile(names.stream().map(name -> name.toLowerCase(Locale.ROOT)).toList()));
    }

    boolean isEnabled() {
        if (enabled == false || closed) {
            return false;
        }
        return supplier instanceof OtelSdkExportTracerSupplier sdkSupplier ? sdkSupplier.hasEndpoint() : true;
    }

    OpenTelemetry delegate() {
        return isEnabled() ? supplier.get() : OpenTelemetry.noop();
    }

    boolean recordExceptionStacks() {
        return recordExceptionStacks;
    }

    String nodeName() {
        return nodeName;
    }

    String clusterName() {
        return clusterName;
    }

    boolean shouldRecord(String name, int depth) {
        // TODO: Preserve depth for compatibility; revisit instrumentation selection and per-trace volume safeguards.
        var included = includeFilter;
        var excluded = excludeFilter;
        return isEnabled()
            && depth <= maxDepth
            && (included == null || included.run(name))
            && (excluded == null || excluded.run(name) == false);
    }

    /** Enforces privacy at export, including late attributes and SDK-generated events. */
    public Attributes sanitize(Attributes attributes) {
        // TODO: Preserve field-name policy for compatibility; revisit its ergonomics independently of instrumentation APIs.
        var filter = redaction;
        var result = attributes.toBuilder();
        attributes.forEach((key, value) -> {
            String name = key.getKey();
            if (filter.matches(name) || filter.matches(legacyHttpName(name))) {
                result.remove(key);
                result.put(AttributeKey.stringKey(name), "[REDACTED]");
            }
        });
        return result.build();
    }

    private static String legacyHttpName(String name) {
        if (name.startsWith("http.request.header.")) {
            return "http.request.headers." + name.substring("http.request.header.".length()).replace('-', '_');
        }
        if (name.startsWith("http.response.header.")) {
            return "http.response.headers." + name.substring("http.response.header.".length());
        }
        return LEGACY_HTTP_NAMES.getOrDefault(name, name);
    }

    /** Canonical HTTP header names must retain protection from selectors written against their original casing. */
    private record Redaction(CharacterRunAutomaton fields, CharacterRunAutomaton headers) {
        boolean matches(String name) {
            if (fields != null && fields.run(name)) {
                return true;
            }
            boolean header = name.startsWith("http.request.header.")
                || name.startsWith("http.request.headers.")
                || name.startsWith("http.response.header.")
                || name.startsWith("http.response.headers.");
            return header && headers != null && headers.run(name.toLowerCase(Locale.ROOT));
        }
    }

    private static CharacterRunAutomaton compile(List<String> names) {
        if (names.isEmpty()) {
            return null;
        }
        var automata = names.stream()
            .map(name -> new RegExp(name.replace(".", "\\.").replace("*", ".*"), RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT).toAutomaton())
            .toList();
        return new CharacterRunAutomaton(Operations.determinize(Operations.union(automata), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }

    /** Flushes without taking ownership away from operations still completing during node shutdown. */
    public CompletableResultCode attemptFlushTraces() {
        return enabled ? supplier.attemptFlushTraces() : CompletableResultCode.ofSuccess();
    }

    @Override
    protected void doStart() {
        if (enabled) {
            supplier.get();
        }
    }

    @Override
    protected void doStop() {
        try {
            attemptFlushTraces().join(OtelSdkSettings.OTEL_EXPORT_FLUSH_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            logger.warn("Failed to flush tracing during shutdown", failure);
        }
    }

    @Override
    protected void doClose() {
        closed = true;
        enabled = false;
        try {
            supplier.close();
        } catch (Exception failure) {
            logger.warn("Failed to close tracing during shutdown", failure);
        }
    }
}
