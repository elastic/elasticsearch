/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.doubleKey;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RedactingSpanExporterTests extends ESTestCase {

    private static final List<String> DEFAULT_PATTERNS = APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(Settings.EMPTY);

    private final InMemorySpanExporter redactedSpans = InMemorySpanExporter.create();

    private final InMemorySpanExporter unmodifiedSpans = InMemorySpanExporter.create();
    private final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(unmodifiedSpans))
        .build();

    @After
    public void closeTracerProvider() {
        tracerProvider.close();
    }

    public void testRedactsSensitiveValuesOfEveryType() {
        final var exporter = factory().create(redactedSpans);
        final SpanData span = span(
            "name1",
            Attributes.builder()
                .put("password", "hunter2")
                .put("http.request.headers.authorization", "Basic abc")
                .put("session_id", 1234L)
                .put("credit-card", 1.5d)
                .put("my-token-flag", true)
                .put(stringArrayKey("auth-values"), List.of("a", "b"))
                .build()
        );

        exporter.export(List.of(span));

        final Attributes attributes = exported().getFirst().getAttributes();
        for (String key : List.of(
            "password",
            "http.request.headers.authorization",
            "session_id",
            "credit-card",
            "my-token-flag",
            "auth-values"
        )) {
            assertThat(key, attributes.get(stringKey(key)), equalTo("[REDACTED]"));
        }
        assertThat(attributes.size(), equalTo(6));
    }

    public void testKeepsTheOtherAttributesAsTheyAre() {
        final var exporter = factory().create(redactedSpans);
        final SpanData span = span(
            "name1",
            Attributes.builder()
                .put("password", "hunter2")
                .put("not.sensitive", "visible")
                .put("a.long", 42L)
                .put("a.double", 1.5d)
                .put("a.boolean", true)
                .put(stringArrayKey("a.array"), List.of("x", "y"))
                .build()
        );

        exporter.export(List.of(span));

        final Attributes attributes = exported().getFirst().getAttributes();
        assertThat(attributes.get(stringKey("not.sensitive")), equalTo("visible"));
        assertThat(attributes.get(longKey("a.long")), equalTo(42L));
        assertThat(attributes.get(doubleKey("a.double")), equalTo(1.5d));
        assertThat(attributes.get(booleanKey("a.boolean")), is(true));
        assertThat(attributes.get(stringArrayKey("a.array")), contains("x", "y"));
        assertThat(attributes.size(), equalTo(6));
    }

    public void testKeepsTheRestOfTheSpanAsItIs() {
        final var exporter = factory().create(redactedSpans);
        final SpanData span = span("name1", Attributes.of(stringKey("password"), "hunter2"));

        exporter.export(List.of(span));

        final SpanData redacted = exported().getFirst();
        assertThat(redacted.getName(), equalTo("name1"));
        assertThat(redacted.getKind(), equalTo(span.getKind()));
        assertThat(redacted.getSpanContext(), equalTo(span.getSpanContext()));
        assertThat(redacted.getStartEpochNanos(), equalTo(span.getStartEpochNanos()));
        assertThat(redacted.getEndEpochNanos(), equalTo(span.getEndEpochNanos()));
        assertThat(redacted.getTotalAttributeCount(), equalTo(span.getTotalAttributeCount()));
    }

    public void testPatternsCanBeUpdated() {
        final var factory = factory();
        final var exporter = factory.create(redactedSpans);
        factory.setSensitiveKeyPatterns(List.of("custom*"));
        final SpanData span = span("name1", Attributes.builder().put("custom-field", "a").put("password", "hunter2").build());

        exporter.export(List.of(span));

        final Attributes attributes = exported().getFirst().getAttributes();
        assertThat(attributes.get(stringKey("custom-field")), equalTo("[REDACTED]"));
        assertThat(attributes.get(stringKey("password")), equalTo("hunter2"));
    }

    public void testFactoryTakesTheInitialPatternsFromTheSettings() {
        final var settings = Settings.builder()
            .putList(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.getKey(), "custom*")
            .build();
        final var exporter = new RedactingSpanExporter.Factory(settings).create(redactedSpans);

        exporter.export(List.of(span("name1", Attributes.builder().put("custom-field", "a").put("password", "hunter2").build())));

        final Attributes attributes = exported().getFirst().getAttributes();
        assertThat(attributes.get(stringKey("custom-field")), equalTo("[REDACTED]"));
        assertThat(attributes.get(stringKey("password")), equalTo("hunter2"));
    }

    public void testDefaultSensitiveKeysAreRedacted() {
        final var exporter = factory().create(redactedSpans);
        final List<String> keys = List.of(
            "auth",
            "auth-header",
            "authValue",
            "card",
            "card-details",
            "card-number",
            "credit",
            "credit-card",
            "key",
            "my-credit-number",
            "my_session_id",
            "passwd",
            "password",
            "principal",
            "principal-value",
            "pwd",
            "secret",
            "secure-key",
            "sensitive-token*",
            "session",
            "session_id",
            "set-cookie",
            "some-auth",
            "some-principal",
            "token-for login"
        );
        final var attributes = Attributes.builder();
        keys.forEach(key -> attributes.put(key, "value"));

        exporter.export(List.of(span("name1", attributes.build())));

        final Attributes redacted = exported().getFirst().getAttributes();
        for (String key : keys) {
            assertThat("Expected [" + key + "] to be redacted", redacted.get(stringKey(key)), equalTo("[REDACTED]"));
        }
    }

    public void testDelegatesFlushShutdownAndClose() {
        final List<String> calls = new ArrayList<>();
        final var exporter = factory().create(new SpanExporter() {
            @Override
            public CompletableResultCode export(Collection<SpanData> spans) {
                calls.add("export");
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                calls.add("flush");
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                calls.add("shutdown");
                return CompletableResultCode.ofSuccess();
            }
        });

        assertThat(exporter.export(List.of()).isSuccess(), is(true));
        assertThat(exporter.flush().isSuccess(), is(true));
        assertThat(exporter.shutdown().isSuccess(), is(true));
        exporter.close();

        // close() shuts the exporter down
        assertThat(calls, contains("export", "flush", "shutdown", "shutdown"));
    }

    private static RedactingSpanExporter.Factory factory() {
        final var factory = new RedactingSpanExporter.Factory(Settings.EMPTY);
        factory.setSensitiveKeyPatterns(RedactingSpanExporterTests.DEFAULT_PATTERNS);
        return factory;
    }

    private SpanData span(String name, Attributes attributes) {
        tracerProvider.get("test").spanBuilder(name).setAllAttributes(attributes).startSpan().end();
        return unmodifiedSpans.getFinishedSpanItems().getLast();
    }

    private List<SpanData> exported() {
        return redactedSpans.getFinishedSpanItems();
    }
}
