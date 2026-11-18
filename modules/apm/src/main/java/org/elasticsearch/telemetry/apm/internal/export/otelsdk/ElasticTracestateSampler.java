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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Parent-based ratio sampler that sets {@code ot=p:<n>} so APM Server can derive
 * {@code representative_count} (= 2^n), translating classic Elastic {@code es=s:<rate>} when needed.
 * {@code es} parsing follows the APM Java agent
 * <a href="https://github.com/elastic/apm-agent-java/blob/0b0082d92686c5ba4fedbc4a251bcefbdbf6b8f8/apm-agent-core/src/main/java/co/elastic/apm/agent/impl/transaction/TraceState.java#L83-L155">TraceState#addTextHeader</a>.
 */
public final class ElasticTracestateSampler implements Sampler {

    // TODO: emit the modern th: tracestate instead of ot=p: once exporting to EDOT gateway
    private final Sampler root;
    private final String localRootOt;

    public ElasticTracestateSampler(Settings settings) {
        this(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.get(settings));
    }

    // visible for tests
    ElasticTracestateSampler(double sampleRate) {
        this.root = Sampler.parentBased(Sampler.traceIdRatioBased(sampleRate));
        this.localRootOt = sampleRate > 0 ? pValueForRate(sampleRate) : null;
    }

    @Override
    public SamplingResult shouldSample(
        Context parentContext,
        String traceId,
        String name,
        SpanKind spanKind,
        Attributes attributes,
        List<LinkData> parentLinks
    ) {
        SamplingResult result = root.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        if (result.getDecision() == SamplingDecision.DROP) {
            return result;
        }
        String ot = resolveOt(Span.fromContext(parentContext).getSpanContext());
        return ot == null ? result : new SamplingResultWithOt(result, ot);
    }

    private String resolveOt(SpanContext parent) {
        String existing = Objects.requireNonNullElse(parent.getTraceState().get("ot"), "");
        if (existing.contains("p:")) {
            return null;
        }
        String p = parent.isValid() ? pValueFromESString(parent.getTraceState().get("es")) : localRootOt;
        if (p == null) {
            return null;
        }
        return existing.isEmpty() ? p : p + ";" + existing;
    }

    @Nullable
    private static String pValueFromESString(String es) {
        if (es == null || es.startsWith("s:") == false) {
            return null;
        }
        int end = es.indexOf(';');
        try {
            double rate = Double.parseDouble(end < 0 ? es.substring(2) : es.substring(2, end));
            if (rate <= 0 || rate > 1) {
                return null;
            }
            return pValueForRate(rate);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String pValueForRate(double rate) {
        return "p:" + Math.min(62, Math.round(-Math.log(rate) / Math.log(2)));
    }

    @Override
    public String getDescription() {
        return "ElasticTracestateSampler{" + root.getDescription() + "}";
    }

    private record SamplingResultWithOt(SamplingResult original, String ot) implements SamplingResult {
        @Override
        public SamplingDecision getDecision() {
            return original.getDecision();
        }

        @Override
        public Attributes getAttributes() {
            return original.getAttributes();
        }

        @Override
        public TraceState getUpdatedTraceState(TraceState traceState) {
            return original.getUpdatedTraceState(traceState).toBuilder().put("ot", ot).build();
        }
    }
}
