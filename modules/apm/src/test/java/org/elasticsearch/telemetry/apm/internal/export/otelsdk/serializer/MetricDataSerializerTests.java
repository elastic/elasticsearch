/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryPointData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MetricDataSerializerTests extends ESTestCase {

    private static final Resource RESOURCE = Resource.create(allAttributeTypes("res"));
    private static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.builder("scope")
        .setVersion("1.2.3")
        .setSchemaUrl("https://example.invalid/schema")
        .build();

    public void testLongGaugeRoundTrip() throws Exception {
        MetricData input = ImmutableMetricData.createLongGauge(
            RESOURCE,
            SCOPE,
            "ops_total",
            "operations performed",
            "ops",
            GaugeData.createLongGaugeData(
                List.of(LongPointData.create(10L, 20L, allAttributeTypes("p"), 42L), LongPointData.create(11L, 21L, Attributes.empty(), 0L))
            )
        );
        assertRoundTripEqual(input);
    }

    public void testDoubleGaugeRoundTrip() throws Exception {
        MetricData input = ImmutableMetricData.createDoubleGauge(
            RESOURCE,
            SCOPE,
            "load_avg",
            "load average",
            "1",
            GaugeData.createDoubleGaugeData(List.of(DoublePointData.create(1L, 2L, allAttributeTypes("p"), 0.5, Collections.emptyList())))
        );
        assertRoundTripEqual(input);
    }

    public void testLongSumRoundTripMonotonicCumulative() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createLongSum(
                RESOURCE,
                SCOPE,
                "counter",
                "monotonic cumulative long counter",
                "1",
                SumData.createLongSumData(
                    true,
                    AggregationTemporality.CUMULATIVE,
                    List.of(LongPointData.create(0L, 5L, allAttributeTypes("p"), 7L))
                )
            )
        );
    }

    public void testLongSumRoundTripNonMonotonicDelta() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createLongSum(
                RESOURCE,
                SCOPE,
                "gauge_as_sum",
                "non-monotonic delta long",
                "1",
                SumData.createLongSumData(
                    false,
                    AggregationTemporality.DELTA,
                    List.of(LongPointData.create(0L, 5L, Attributes.empty(), -7L))
                )
            )
        );
    }

    public void testDoubleSumRoundTrip() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createDoubleSum(
                RESOURCE,
                SCOPE,
                "cpu_seconds",
                "cpu time",
                "s",
                SumData.createDoubleSumData(
                    true,
                    AggregationTemporality.DELTA,
                    List.of(DoublePointData.create(0L, 5L, allAttributeTypes("p"), 1.25, Collections.emptyList()))
                )
            )
        );
    }

    public void testHistogramRoundTripWithMinMax() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createDoubleHistogram(
                RESOURCE,
                SCOPE,
                "latency",
                "request latency",
                "ms",
                HistogramData.create(
                    AggregationTemporality.CUMULATIVE,
                    List.of(
                        HistogramPointData.create(
                            0L,
                            10L,
                            allAttributeTypes("p"),
                            12.5,
                            true,
                            0.1,
                            true,
                            9.9,
                            List.of(1.0, 5.0, 10.0),
                            List.of(1L, 2L, 3L, 0L)
                        )
                    )
                )
            )
        );
    }

    public void testHistogramRoundTripWithoutMinMax() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createDoubleHistogram(
                RESOURCE,
                SCOPE,
                "latency_nominmax",
                "request latency, no min/max recorded",
                "ms",
                HistogramData.create(
                    AggregationTemporality.DELTA,
                    List.of(
                        HistogramPointData.create(
                            0L,
                            10L,
                            Attributes.empty(),
                            42.0,
                            false,
                            Double.NaN,
                            false,
                            Double.NaN,
                            List.of(1.0),
                            List.of(2L, 3L)
                        )
                    )
                )
            )
        );
    }

    public void testEmptyResourceAndScopeRoundTrip() throws Exception {
        // Exercises the "no resource attrs" and "no scope version/schemaUrl" branches.
        assertRoundTripEqual(
            ImmutableMetricData.createLongGauge(
                Resource.empty(),
                InstrumentationScopeInfo.create("bare_scope"),
                "bare",
                "no resource attrs, minimal scope",
                "1",
                GaugeData.createLongGaugeData(List.of(LongPointData.create(0L, 1L, Attributes.empty(), 1L)))
            )
        );
    }

    public void testResourceWithSchemaUrlRoundTrip() throws Exception {
        Resource withSchemaUrl = Resource.create(allAttributeTypes("res"), "https://example.invalid/resource-schema");
        assertRoundTripEqual(
            ImmutableMetricData.createLongGauge(
                withSchemaUrl,
                SCOPE,
                "with_schema",
                "resource carries a schemaUrl",
                "1",
                GaugeData.createLongGaugeData(List.of(LongPointData.create(0L, 1L, Attributes.empty(), 1L)))
            )
        );
    }

    public void testHistogramSingleBucketRoundTrip() throws Exception {
        assertRoundTripEqual(
            ImmutableMetricData.createDoubleHistogram(
                RESOURCE,
                SCOPE,
                "single_bucket",
                "histogram with no explicit boundaries (one bucket counts everything)",
                "ms",
                HistogramData.create(
                    AggregationTemporality.DELTA,
                    List.of(
                        HistogramPointData.create(
                            0L,
                            10L,
                            Attributes.empty(),
                            7.0,
                            false,
                            Double.NaN,
                            false,
                            Double.NaN,
                            List.of(),
                            List.of(4L)
                        )
                    )
                )
            )
        );
    }

    public void testUnsupportedTypesAreDropped() throws Exception {
        MetricData summary = ImmutableMetricData.createDoubleSummary(
            RESOURCE,
            SCOPE,
            "summary",
            "summary metric",
            "1",
            ImmutableSummaryData.create(List.of(ImmutableSummaryPointData.create(0L, 1L, Attributes.empty(), 0L, 0.0, List.of())))
        );
        MetricData supportedAlongside = ImmutableMetricData.createLongGauge(
            RESOURCE,
            SCOPE,
            "survivor",
            "supported metric in the same batch",
            "1",
            GaugeData.createLongGaugeData(List.of(LongPointData.create(0L, 1L, Attributes.empty(), 99L)))
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MetricDataSerializer.serialize(List.of(summary, supportedAlongside), out);
        List<MetricData> decoded = MetricDataSerializer.deserialize(new ByteArrayInputStream(out.toByteArray()));

        assertThat(decoded, hasSize(1));
        assertThat(decoded.get(0).getName(), equalTo("survivor"));
    }

    public void testEmptyBatchRoundTrip() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MetricDataSerializer.serialize(List.of(), out);
        List<MetricData> decoded = MetricDataSerializer.deserialize(new ByteArrayInputStream(out.toByteArray()));
        assertThat(decoded, empty());
    }

    public void testMalformedInputThrowsXContentParseException() throws Exception {
        // Write a batch with an invalid "type" value so MetricDataType.valueOf fails during parsing.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (var builder = org.elasticsearch.xcontent.XContentFactory.smileBuilder(out);) {
            builder.startArray();
            builder.startObject();
            builder.field("name", "x");
            builder.field("description", "");
            builder.field("unit", "");
            builder.field("type", "NOT_A_REAL_TYPE");
            builder.startArray("resource").endArray();
            builder.startObject("scope").field("name", "s").endObject();
            builder.startArray("points").endArray();
            builder.endObject();
            builder.endArray();
        }
        var ex = expectThrows(
            XContentParseException.class,
            () -> MetricDataSerializer.deserialize(new ByteArrayInputStream(out.toByteArray()))
        );
        assertThat(ex.getMessage(), containsString("type"));
    }

    private static Attributes allAttributeTypes(String prefix) {
        return Attributes.builder()
            .put(AttributeKey.stringKey(prefix + ".s"), "v")
            .put(AttributeKey.booleanKey(prefix + ".b"), true)
            .put(AttributeKey.longKey(prefix + ".l"), 7L)
            .put(AttributeKey.doubleKey(prefix + ".d"), 1.5)
            .put(AttributeKey.stringArrayKey(prefix + ".sa"), List.of("a", "b"))
            .put(AttributeKey.booleanArrayKey(prefix + ".ba"), List.of(true, false))
            .put(AttributeKey.longArrayKey(prefix + ".la"), List.of(1L, 2L, 3L))
            .put(AttributeKey.doubleArrayKey(prefix + ".da"), List.of(0.5, 1.5))
            .build();
    }

    private static void assertRoundTripEqual(MetricData input) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MetricDataSerializer.serialize(List.of(input), out);
        List<MetricData> decoded = MetricDataSerializer.deserialize(new ByteArrayInputStream(out.toByteArray()));
        assertThat(decoded, hasSize(1));
        assertThat(decoded.get(0), equalTo(input));
    }
}
