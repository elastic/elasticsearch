/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class APMTracingServiceTests extends ESTestCase {

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    public void testSensitiveAttributesAreRedactedByDefault() {
        try (var service = service(enabled(true))) {
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
        try (var service = service(enabled(false))) {
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
        try (var service = service(enabled(true))) {
            service.start();
            updater(service, false).update(enabled(true));

            service.tracer().spanBuilder("name1").startSpan().end();

            assertThat(exportedAttribute("name1", "es.cluster.name"), equalTo(TracingSettingsUpdater.CLUSTER_NAME));
            assertThat(exportedAttribute("name1", "es.node.name"), equalTo(TracingSettingsUpdater.NODE_NAME));
        }
    }

    public void testDisabledServiceDoesNotCreateTheSdk() {
        final AtomicInteger created = new AtomicInteger();
        final Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        try (var service = new APMTracingService(new ThreadContext(settings), settings, () -> {
            created.incrementAndGet();
            return SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        })) {
            service.start();

            service.tracer().spanBuilder("name1").startSpan().end();

            assertThat(service.tracer().currentSpan(), sameInstance(Span.noop()));
            assertThat(exporter.getFinishedSpanItems().size(), equalTo(0));
            assertThat(created.get(), equalTo(0));
        }
    }

    // the limits and the filters of the spans are applied by the tracer, so that the spans that they drop do not drop the trace elsewhere

    public void testSpanLimitsAreAppliedToTheTracerFromTheSettingsTheServiceIsCreatedWith() {
        final Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), 1)
            .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), "excluded*")
            .build();
        try (var service = service(settings)) {
            service.start();
            final var tracer = service.tracer();

            final Span root = tracer.spanBuilder("root").startSpan();
            // the first child is recorded, the grandchild is too deep for the depth, and then the root and the child fill the count
            final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
            assertThat(tracer.spanBuilder("grandchild").setParent(child).startSpan().isRecording(), is(false));
            assertThat(tracer.spanBuilder("child-2").setParent(root).startSpan().isRecording(), is(false));
            assertThat(tracer.spanBuilder("excluded-child").setParent(root).startSpan().isRecording(), is(false));
            child.end();
            root.end();

            assertThat(exportedNames(), containsInAnyOrder("root", "child"));
        }
    }

    public void testSpanLimitsAndFiltersCanBeUpdatedWhileTracing() {
        try (var service = service(enabled(true))) {
            service.start();
            final var tracer = service.tracer();
            final var updater = updater(service, true);

            // by default only the local root is traced
            final Span root = tracer.spanBuilder("root").startSpan();
            assertThat(tracer.spanBuilder("child-1").setParent(root).startSpan().isRecording(), is(false));

            updater.update(
                Settings.builder()
                    .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), 1)
                    .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2)
                    .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), "excluded*")
                    .build()
            );
            assertThat(tracer.spanBuilder("excluded-child").setParent(root).startSpan().isRecording(), is(false));
            tracer.spanBuilder("child-2").setParent(root).startSpan().end();
            assertThat(tracer.spanBuilder("child-3").setParent(root).startSpan().isRecording(), is(false));
            root.end();

            assertThat(exportedNames(), containsInAnyOrder("root", "child-2"));
        }
    }

    public void testSpanIncludeFilterCanBeUpdatedWhileTracing() {
        try (var service = service(enabled(true))) {
            service.start();
            updater(service, true).update(
                Settings.builder().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), "included*").build()
            );

            service.tracer().spanBuilder("included-1").startSpan().end();
            service.tracer().spanBuilder("other").startSpan().end();

            assertThat(exportedNames(), containsInAnyOrder("included-1"));
        }
    }

    private APMTracingService service(Settings settings) {
        return new APMTracingService(
            new ThreadContext(settings),
            settings,
            () -> SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
        );
    }

    private static Settings enabled(boolean enabled) {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), enabled).build();
    }

    private static Settings labelFilters(String... patterns) {
        return Settings.builder().putList(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.getKey(), patterns).build();
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

    private List<String> exportedNames() {
        return exporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
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
