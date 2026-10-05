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

    private TraceState sample(TraceState parentTraceState) {
        SpanContext parent = SpanContext.createFromRemoteParent(
            "00000000000000000000000000000001",
            "0000000000000001",
            TraceFlags.getSampled(),
            parentTraceState
        );
        var result = sampler.shouldSample(
            Context.root().with(Span.wrap(parent)),
            "00000000000000000000000000000001",
            "test",
            SpanKind.SERVER,
            Attributes.empty(),
            List.of()
        );
        return result.getUpdatedTraceState(parentTraceState);
    }
}
