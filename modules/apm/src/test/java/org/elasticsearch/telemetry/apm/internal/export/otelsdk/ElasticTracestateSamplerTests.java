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
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.tracing.LocalSpanCount;
import org.elasticsearch.telemetry.apm.internal.tracing.LocalTraceDepth;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ElasticTracestateSamplerTests extends ESTestCase {

    private final ElasticTracestateSampler sampler = new ElasticTracestateSampler(0.001);

    public void testElasticSampleRateTranslatedToOt() {
        assertThat(sample(TraceState.builder().put("es", "s:0.125").build()).get("ot"), equalTo("p:3"));
        assertThat(sample(TraceState.builder().put("es", "s:0.125;k:v").build()).get("ot"), equalTo("p:3"));
        assertThat(sample(TraceState.builder().put("es", "s:0.01").build()).get("ot"), equalTo("p:7"));
    }

    public void testExistingOtWithPIsPreserved() {
        TraceState parent = TraceState.builder().put("ot", "p:3;r:5").build();
        assertThat(sample(parent).get("ot"), equalTo("p:3;r:5"));
    }

    public void testNoSamplingInfoLeavesOtUnchanged() {
        assertThat(sample(TraceState.getDefault()).get("ot"), nullValue());
    }

    public void testMalformedOrOutOfRangeElasticRateIsIgnored() {
        assertThat(sample(TraceState.builder().put("es", "s:not-a-number").build()).get("ot"), nullValue());
        assertThat(sample(TraceState.builder().put("es", "s:1.5").build()).get("ot"), nullValue());
    }

    public void testSpanWithAFilteredOutNameIsDropped() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of("included*"), List.of(), Integer.MAX_VALUE, Integer.MAX_VALUE);

        assertThat(decision(sampler, "included-1", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "other", localContext(0)), equalTo(SamplingDecision.DROP));
    }

    public void testExcludedNameIsDroppedEvenIfIncluded() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of("name-*"), List.of("name-b*"), Integer.MAX_VALUE, Integer.MAX_VALUE);

        assertThat(decision(sampler, "name-a", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "name-b", localContext(0)), equalTo(SamplingDecision.DROP));
    }

    public void testNamesCanBeUpdated() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertThat(decision(sampler, "name-a", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));

        sampler.setExcludeNames(List.of("name-a"));
        assertThat(decision(sampler, "name-a", localContext(0)), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "name-b", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));

        sampler.setExcludeNames(List.of());
        sampler.setIncludeNames(List.of("name-b"));
        assertThat(decision(sampler, "name-a", localContext(0)), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "name-b", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
    }

    public void testSpansDeeperThanTheMaximumDepthAreDropped() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), 1, Integer.MAX_VALUE);

        assertThat(decision(sampler, "root", localContext(0)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child", localContext(1)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "grandchild", localContext(2)), equalTo(SamplingDecision.DROP));
    }

    public void testMaximumDepthCanBeUpdated() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), 0, Integer.MAX_VALUE);
        assertThat(decision(sampler, "child", localContext(1)), equalTo(SamplingDecision.DROP));

        sampler.setMaxTraceDepth(1);

        assertThat(decision(sampler, "child", localContext(1)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
    }

    public void testSpansBeyondTheMaximumCountAreDropped() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), Integer.MAX_VALUE, 2);
        final var count = new LocalSpanCount();

        assertThat(decision(sampler, "child-1", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child-2", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child-3", localContext(1, count)), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "child-4", localContext(1, count)), equalTo(SamplingDecision.DROP));
    }

    public void testFilteredOutSpansAreGivenBackToTheCount() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of("excluded"), Integer.MAX_VALUE, 1);
        final var count = new LocalSpanCount();

        assertThat(decision(sampler, "excluded", localContext(1, count)), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "child", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
    }

    /** The rate is what is checked first, so the spans that it drops are never counted. */
    public void testSpansThatAreDroppedByTheRateAreNotCounted() {
        final var sampler = new ElasticTracestateSampler(0.0, List.of(), List.of(), Integer.MAX_VALUE, 1);
        final var count = new LocalSpanCount();

        // a span whose local parent is not sampled is not sampled either
        final Context unsampledParent = Context.root()
            .with(Span.wrap(SpanContext.create(TRACE_ID, "0000000000000001", TraceFlags.getDefault(), TraceState.getDefault())))
            .with(count);
        assertThat(decision(sampler, "child-1", unsampledParent), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "child-2", unsampledParent), equalTo(SamplingDecision.DROP));

        assertThat(count.incrementAndGet() - 1, equalTo(0));
    }

    /** The depth is checked before the number of the spans, so the spans that are too deep are never counted. */
    public void testSpansThatAreTooDeepAreNotCounted() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), 1, 1);
        final var count = new LocalSpanCount();

        assertThat(decision(sampler, "grandchild-1", localContext(2, count)), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "grandchild-2", localContext(2, count)), equalTo(SamplingDecision.DROP));

        assertThat(count.incrementAndGet() - 1, equalTo(0));
    }

    public void testLocalRootCountsAgainstTheMaximum() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), Integer.MAX_VALUE, 2);
        final var count = new LocalSpanCount();

        assertThat(decision(sampler, "root", localContext(0, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child-1", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child-2", localContext(1, count)), equalTo(SamplingDecision.DROP));
    }

    public void testNothingIsRecordedWithoutRoomForTheLocalRoot() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), Integer.MAX_VALUE, 0);

        assertThat(decision(sampler, "root", localContext(0, new LocalSpanCount())), equalTo(SamplingDecision.DROP));
    }

    public void testMaximumCanBeUpdated() {
        final var sampler = new ElasticTracestateSampler(1.0, List.of(), List.of(), Integer.MAX_VALUE, 1);
        final var count = new LocalSpanCount();
        assertThat(decision(sampler, "child-1", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "child-2", localContext(1, count)), equalTo(SamplingDecision.DROP));

        sampler.setMaxSpansInTrace(2);

        assertThat(decision(sampler, "child-2", localContext(1, count)), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
    }

    public void testSettingsDefineTheLimits() {
        final var sampler = new ElasticTracestateSampler(
            Settings.builder()
                .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
                .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), "excluded")
                .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), 1)
                .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 10)
                .build()
        );

        assertThat(decision(sampler, "excluded", localContext(0, new LocalSpanCount())), equalTo(SamplingDecision.DROP));
        assertThat(decision(sampler, "child", localContext(1, new LocalSpanCount())), equalTo(SamplingDecision.RECORD_AND_SAMPLE));
        assertThat(decision(sampler, "grandchild", localContext(2, new LocalSpanCount())), equalTo(SamplingDecision.DROP));
    }

    private static final String TRACE_ID = "00000000000000000000000000000001";

    /** A context of a sampled local span at the given depth. */
    private static Context localContext(int depth) {
        return localContext(depth, new LocalSpanCount());
    }

    private static Context localContext(int depth, LocalSpanCount count) {
        final SpanContext parent = SpanContext.create(TRACE_ID, "0000000000000001", TraceFlags.getSampled(), TraceState.getDefault());
        return Context.root().with(Span.wrap(parent)).with(new LocalTraceDepth(depth)).with(count);
    }

    private static SamplingDecision decision(ElasticTracestateSampler sampler, String name, Context parentContext) {
        return sampler.shouldSample(parentContext, TRACE_ID, name, SpanKind.INTERNAL, Attributes.empty(), List.of()).getDecision();
    }

    private TraceState sample(TraceState parentTraceState) {
        SpanContext parent = SpanContext.createFromRemoteParent(TRACE_ID, "0000000000000001", TraceFlags.getSampled(), parentTraceState);
        var result = sampler.shouldSample(
            Context.root().with(Span.wrap(parent)).with(new LocalSpanCount()),
            TRACE_ID,
            "test",
            SpanKind.SERVER,
            Attributes.empty(),
            List.of()
        );
        return result.getUpdatedTraceState(parentTraceState);
    }
}
