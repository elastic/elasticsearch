/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.test.ESTestCase;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class APMTracingServiceTests extends ESTestCase {

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    public void testSensitiveAttributesAreRedactedByDefault() {
        try (var service = service(true)) {
            service.start();

            service.tracer()
                .spanBuilder("name1")
                .setAttribute("password", "hunter2")
                .setAttribute("not.sensitive", "visible")
                .startSpan()
                .end();

            assertThat(exportedAttribute("name1", "password"), equalTo("[REDACTED]"));
            assertThat(exportedAttribute("name1", "not.sensitive"), equalTo("visible"));
        }
    }

    public void testLabelFiltersCanBeUpdatedWhileTracing() {
        try (var service = service(true)) {
            service.start();

            updater(service, true).update(labelFilters("custom*"));
            service.tracer().spanBuilder("name1").setAttribute("custom-field", "a").setAttribute("password", "hunter2").startSpan().end();

            assertThat(exportedAttribute("name1", "custom-field"), equalTo("[REDACTED]"));
            assertThat(exportedAttribute("name1", "password"), equalTo("hunter2"));
        }
    }

    public void testLabelFiltersSetWhileTracingIsDisabledApplyOnceItIsEnabled() {
        try (var service = service(false)) {
            service.start();

            final var updater = updater(service, false);
            updater.update(labelFilters("custom*"));
            updater.update(enabled(true));
            service.tracer().spanBuilder("name1").setAttribute("custom-field", "a").setAttribute("password", "hunter2").startSpan().end();

            assertThat(exportedAttribute("name1", "custom-field"), equalTo("[REDACTED]"));
            assertThat(exportedAttribute("name1", "password"), equalTo("hunter2"));
        }
    }

    public void testSpansCarryTheNamesOfTheClusterAndTheNode() {
        try (var service = service(true)) {
            service.start();
            updater(service, false).update(enabled(true));

            service.tracer().spanBuilder("name1").startSpan().end();

            assertThat(exportedAttribute("name1", "es.cluster.name"), equalTo(TracingSettingsUpdater.CLUSTER_NAME));
            assertThat(exportedAttribute("name1", "es.node.name"), equalTo(TracingSettingsUpdater.NODE_NAME));
        }
    }

    public void testDisabledServiceDoesNotTrace() {
        try (var service = service(false)) {
            service.start();

            service.tracer().spanBuilder("name1").startSpan().end();
            assertThat(service.tracer().currentSpan(), sameInstance(Span.noop()));
            assertThat(exporter.getFinishedSpanItems().size(), equalTo(0));
        }
    }

    /** A service whose spans are exported to {@link #exporter} through the sampler of the service. */
    private APMTracingService service(boolean enabled) {
        final Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), enabled)
            .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
            .build();
        return new APMTracingService(
            new ThreadContext(settings),
            settings,
            sampler -> () -> OpenTelemetrySdk.builder()
                .setTracerProvider(
                    SdkTracerProvider.builder().setSampler(sampler).addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
                )
                .build()
        );
    }

    private static TracingSettingsUpdater updater(APMTracingService service, boolean enabled) {
        return new TracingSettingsUpdater(
            service,
            Settings.builder()
                .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), enabled)
                .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
                .build()
        );
    }

    private static Settings enabled(boolean enabled) {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), enabled).build();
    }

    private static Settings labelFilters(String... patterns) {
        return Settings.builder().putList(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.getKey(), patterns).build();
    }

    private String exportedAttribute(String spanName, String key) {
        return exporter.getFinishedSpanItems()
            .stream()
            .filter(span -> span.getName().equals(spanName))
            .findFirst()
            .orElseThrow()
            .getAttributes()
            .get(stringKey(key));
    }
}
