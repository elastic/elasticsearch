/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.GaugeAggregation;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.Metric;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricType;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics.MetricValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class CompiledDerivedMetricsTests extends ESTestCase {

    public void testBuiltinWildcardExpandsToEveryIngestMetric() {
        CompiledDerivedMetrics compiled = compile(new DataStreamDerivedMetrics(true, List.of("ingest.*"), null, null, null));
        assertThat(
            compiled.metrics().stream().map(CompiledMetric::name).toList(),
            contains(
                "ingest.docs.count",
                "ingest.docs.rate",
                "ingest.bytes.count",
                "ingest.bytes.rate",
                "ingest.failures.count",
                "ingest.failures.rate"
            )
        );
    }

    /**
     * A stream that only wants the built-in ingest metrics must not force the write path to read {@code _source} at all.
     */
    public void testBuiltinOnlyConfigurationNeedsNoSource() {
        CompiledDerivedMetrics compiled = compile(new DataStreamDerivedMetrics(true, List.of("ingest.*"), null, null, null));
        assertThat(compiled.requiredPaths(), empty());
        assertFalse(compiled.needsSource());
    }

    public void testGlobalDimensionsApplyToBuiltins() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(true, List.of("ingest.docs.count"), null, List.of("service.name"), null)
        );
        assertThat(compiled.requiredPaths(), contains("service.name"));
        assertTrue(compiled.needsSource());
        assertThat(compiled.metrics().get(0).dimensions(), contains("service.name"));
    }

    public void testRequiredPathsCoverDimensionsPredicatesAndValues() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                null,
                List.of("service.name"),
                List.of(
                    new Metric(
                        "http.requests",
                        MetricType.COUNTER,
                        Map.of("exists", Map.of("field", "http.request.method")),
                        null,
                        null,
                        List.of("http.response.status_code")
                    ),
                    new Metric("queue.depth", MetricType.GAUGE, null, MetricValue.field("queue.depth"), GaugeAggregation.MAX, null)
                )
            )
        );
        assertThat(
            compiled.requiredPaths(),
            containsInAnyOrder("service.name", "http.response.status_code", "http.request.method", "queue.depth")
        );
    }

    public void testMetricDimensionsAreAddedToGlobalDimensions() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                null,
                List.of("service.name"),
                List.of(new Metric("http.requests", MetricType.COUNTER, null, null, null, List.of("http.request.method")))
            )
        );
        assertThat(compiled.metrics().get(0).dimensions(), contains("service.name", "http.request.method"));
    }

    public void testGaugeAggregationsMapToReductions() {
        assertEquals(Reduction.FIRST, reductionOfGauge(GaugeAggregation.FIRST_VALUE));
        assertEquals(Reduction.LAST, reductionOfGauge(GaugeAggregation.LAST_VALUE));
        assertEquals(Reduction.MIN, reductionOfGauge(GaugeAggregation.MIN));
        assertEquals(Reduction.MAX, reductionOfGauge(GaugeAggregation.MAX));
        assertEquals(Reduction.AVG, reductionOfGauge(GaugeAggregation.AVG));
        assertEquals(Reduction.SUM, reductionOfGauge(GaugeAggregation.SUM));
    }

    public void testCounterReducesBySum() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                null,
                null,
                List.of(new Metric("http.requests", MetricType.COUNTER, null, null, null, null))
            )
        );
        CompiledMetric metric = compiled.metrics().get(0);
        assertEquals(Reduction.SUM, metric.reduction());
        assertEquals(Trigger.SUCCESS, metric.trigger());
        assertEquals(new CompiledDerivedMetrics.Source.Constant(1.0), metric.source());
    }

    public void testFailureBuiltinsAreTriggeredByFailures() {
        CompiledDerivedMetrics compiled = compile(new DataStreamDerivedMetrics(true, List.of("ingest.failures.count"), null, null, null));
        assertEquals(Trigger.FAILURE, compiled.metrics().get(0).trigger());
        assertEquals(Set.of(Trigger.FAILURE), Set.copyOf(compiled.triggers()));
    }

    /**
     * Histograms are accepted by the configuration model but cannot be emitted yet, so compilation reports them instead of failing the
     * whole configuration.
     */
    public void testHistogramMetricsAreReportedAsUnsupported() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                null,
                null,
                List.of(
                    new Metric("http.request.duration", MetricType.HISTOGRAM, null, MetricValue.field("event.duration"), null, null),
                    new Metric("http.requests", MetricType.COUNTER, null, null, null, null)
                )
            )
        );
        assertThat(compiled.unsupportedMetrics(), contains("http.request.duration"));
        assertThat(compiled.metrics().stream().map(CompiledMetric::name).toList(), contains("http.requests"));
    }

    public void testIntervalsKeepTheirConfiguredRendering() {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                List.of(TimeValue.timeValueSeconds(10), TimeValue.timeValueMinutes(1)),
                null,
                null
            )
        );
        assertThat(compiled.intervals().stream().map(CompiledDerivedMetrics.Interval::name).toList(), contains("10s", "1m"));
        assertEquals(10_000L, compiled.intervals().get(0).millis());
        assertEquals(60_000L, compiled.intervals().get(1).millis());
    }

    private static Reduction reductionOfGauge(GaugeAggregation aggregation) {
        CompiledDerivedMetrics compiled = compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                null,
                null,
                List.of(new Metric("queue.depth", MetricType.GAUGE, null, MetricValue.field("queue.depth"), aggregation, null))
            )
        );
        return compiled.metrics().get(0).reduction();
    }

    private static CompiledDerivedMetrics compile(DataStreamDerivedMetrics config) {
        return CompiledDerivedMetrics.compile(config);
    }
}
