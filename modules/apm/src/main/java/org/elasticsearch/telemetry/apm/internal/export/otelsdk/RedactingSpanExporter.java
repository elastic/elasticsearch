/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link SpanExporter} that replaces the values of the sensitive span attributes with {@value #REDACTED} before the spans are handed
 * over to the exporter.
 */
public class RedactingSpanExporter implements SpanExporter {

    static final String REDACTED = "[REDACTED]";

    private final SpanExporter delegate;
    // reference shared with the Factory
    private final AtomicReference<CharacterRunAutomaton> sensitiveKeys;

    private RedactingSpanExporter(SpanExporter delegate, AtomicReference<CharacterRunAutomaton> sensitiveKeys) {
        this.delegate = delegate;
        this.sensitiveKeys = sensitiveKeys;
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        final CharacterRunAutomaton sensitiveKeys = this.sensitiveKeys.get();
        final List<SpanData> redacted = new ArrayList<>(spans.size());
        for (SpanData span : spans) {
            redacted.add(hasAnySensitiveAttributes(sensitiveKeys, span) ? new RedactedSpanData(span, sensitiveKeys) : span);
        }
        return delegate.export(redacted);
    }

    private boolean hasAnySensitiveAttributes(CharacterRunAutomaton sensitiveKeys, SpanData span) {
        if (sensitiveKeys == Factory.NOTHING) {
            return false;
        }

        // asMap() returns a view, not a copy
        for (var attributeKey : span.getAttributes().asMap().keySet()) {
            if (sensitiveKeys.run(attributeKey.getKey())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static final class RedactedSpanData extends DelegatingSpanData {

        private final Attributes attributes;

        RedactedSpanData(SpanData delegate, CharacterRunAutomaton sensitiveKeys) {
            super(delegate);
            final AttributesBuilder builder = Attributes.builder();
            delegate.getAttributes().forEach((key, value) -> {
                if (sensitiveKeys.run(key.getKey())) {
                    builder.put(key.getKey(), REDACTED);
                } else {
                    putUnchecked(builder, key, value);
                }
            });
            this.attributes = builder.build();
        }

        @SuppressWarnings("unchecked")
        private static <T> void putUnchecked(AttributesBuilder builder, AttributeKey<T> key, Object value) {
            builder.put(key, (T) value);
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }
    }

    public static final class Factory {

        static final CharacterRunAutomaton NOTHING = new CharacterRunAutomaton(Automata.makeEmpty());

        private final AtomicReference<CharacterRunAutomaton> sensitiveKeys = new AtomicReference<>(NOTHING);

        public Factory(Settings settings) {
            setSensitiveKeyPatterns(APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(settings));
        }

        public void setSensitiveKeyPatterns(List<String> sensitiveKeyPatterns) {
            // without patterns nothing is sensitive
            this.sensitiveKeys.set(sensitiveKeyPatterns.isEmpty() ? NOTHING : PatternAutomata.compile(sensitiveKeyPatterns, List.of()));
        }

        public SpanExporter create(SpanExporter original) {
            return new RedactingSpanExporter(original, sensitiveKeys);
        }
    }
}
