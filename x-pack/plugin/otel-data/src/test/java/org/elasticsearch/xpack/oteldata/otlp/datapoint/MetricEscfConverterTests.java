/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.HistogramMapping;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createExponentialHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSummaryMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValueList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link MetricEscfConverter} using an equivalence oracle: for each request, the test builds
 * the expected document via {@link MetricDocumentBuilder} (JSON), then builds via
 * {@link MetricEscfConverter} and reconstructs each ESCF row to JSON via {@link EirfRowToXContent}.
 * The two JSON maps must be equal.
 */
public class MetricEscfConverterTests extends ESTestCase {

    private final IndexVersion indexVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);
    private final long timestamp = Math.abs(randomLong() % 1_000_000_000_000_000L) + 1_000_000_000_000_000L;
    private final long startTimestamp = timestamp - 1_000_000_000L;

    /**
     * Builds the expected JSON via MetricDocumentBuilder and the ESCF via MetricEscfConverter, then
     * asserts that the reconstructed ESCF rows equal the expected JSON documents.
     */
    private void assertRoundTrip(ExportMetricsServiceRequest request) throws IOException {
        assertRoundTrip(request, MappingHints.DEFAULT_TDIGEST);
    }

    /**
     * Equivalence oracle parameterized by the effective {@link MappingHints}, so histogram tests can
     * exercise the tdigest / raw / aggregate-metric-double / exponential mappings. Asserts row content,
     * tsid, and the dynamic-template assignments all match the {@link MetricDocumentBuilder} output.
     */
    private void assertRoundTrip(ExportMetricsServiceRequest request, MappingHints hints) throws IOException {
        MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(new BufferedByteStringAccessor(), hints);
        // 1. Build expected JSON docs, tsids, and dynamic templates via MetricDocumentBuilder.
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        List<BytesRef> expectedTsids = new ArrayList<>();
        List<Map<String, String>> expectedTemplates = new ArrayList<>();
        List<Map<String, Map<String, String>>> expectedTemplateParams = new ArrayList<>();
        DataPointGroupingContext ctx = new DataPointGroupingContext(new BufferedByteStringAccessor(), hints);
        ctx.groupDataPoints(request);
        ctx.consume(group -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            Map<String, String> dt = new HashMap<>();
            Map<String, Map<String, String>> dtp = new HashMap<>();
            BytesRef tsid = documentBuilder.buildMetricDocument(builder, group, dt, dtp, indexVersion);
            expectedDocs.add(asMap(BytesReference.bytes(builder)));
            expectedTsids.add(tsid);
            expectedTemplates.add(dt);
            expectedTemplateParams.add(dtp);
        });

        // 2. Convert via MetricEscfConverter.
        try (MetricEscfConverter.Result result = MetricEscfConverter.convert(request, hints, indexVersion, t -> 0)) {
            List<MetricEscfConverter.GroupResult> groups = result.groups();
            assertThat(groups.size(), equalTo(expectedDocs.size()));

            for (int i = 0; i < groups.size(); i++) {
                MetricEscfConverter.GroupResult g = groups.get(i);
                EscfBatch batch = result.batch(g.targetIndex(), g.shardId());
                assertThat("batch for group " + i, batch, notNullValue());

                // Reconstruct the ESCF row to JSON.
                Map<String, Object> actual;
                try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                    EirfRowToXContent.writeRow(batch.row(g.rowIndex()), batch.schema(), builder);
                    actual = asMap(BytesReference.bytes(builder));
                }

                // Assert structural equality.
                assertEquals("group " + i + " row content mismatch", expectedDocs.get(i), actual);

                // Assert tsid equality.
                assertThat("group " + i + " tsid mismatch", g.tsid(), equalTo(expectedTsids.get(i)));

                // Assert dynamic-template equality (needed for the ESCF path to map fields correctly).
                assertEquals("group " + i + " dynamic templates mismatch", expectedTemplates.get(i), g.dynamicTemplates());
                assertEquals("group " + i + " dynamic template params mismatch", expectedTemplateParams.get(i), g.dynamicTemplateParams());
            }
        }
    }

    public void testGaugeDouble() throws IOException {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createGaugeMetric("cpu.usage", "By", List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of()))))
        );
        assertRoundTrip(request);
    }

    public void testGaugeLong() throws IOException {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createGaugeMetric("cpu.usage", "By", List.of(createLongDataPoint(timestamp, startTimestamp, List.of()))))
        );
        assertRoundTrip(request);
    }

    public void testSumMonotonic() throws IOException {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(
                createSumMetric(
                    "requests.total",
                    "{requests}",
                    List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of())),
                    true,
                    AGGREGATION_TEMPORALITY_DELTA
                )
            )
        );
        assertRoundTrip(request);
    }

    public void testSummary() throws IOException {
        List<KeyValue> attrs = List.of(keyValue("service.name", "svc"), keyValue("region", "us-east-1"));
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            attrs,
            List.of(createSummaryMetric("latency.summary", "s", List.of(createSummaryDataPoint(timestamp, List.of()))))
        );
        assertRoundTrip(request);
    }

    public void testHistogramTDigest() throws IOException {
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(100)
            .setSum(500.0)
            .addExplicitBounds(1.0)
            .addExplicitBounds(5.0)
            .addExplicitBounds(10.0)
            .addBucketCounts(10)
            .addBucketCounts(40)
            .addBucketCounts(30)
            .addBucketCounts(20)
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createHistogramMetric("response.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request);
    }

    public void testDataPointAttributes() throws IOException {
        List<KeyValue> dpAttrs = List.of(keyValue("status_code", "200"), keyValue("region", "us-west-2"));
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createGaugeMetric("http.requests", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, dpAttrs))))
        );
        assertRoundTrip(request);
    }

    public void testMultipleMetricsInOneGroup() throws IOException {
        // Two metrics share the same data-point attributes and timestamp → same group.
        long ts = timestamp;
        long startTs = startTimestamp;
        List<KeyValue> attrs = List.of(keyValue("status", "ok"));
        List<KeyValue> resource = List.of(keyValue("service.name", "svc"));
        // We create them in separate resource metrics so they group by their shared data-point tsid.
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            resource,
            List.of(
                createGaugeMetric("metric.a", "", List.of(createDoubleDataPoint(ts, startTs, attrs))),
                createGaugeMetric("metric.b", "", List.of(createLongDataPoint(ts, startTs, attrs)))
            )
        );
        assertRoundTrip(request);
    }

    public void testResourceAttributes() throws IOException {
        List<KeyValue> resourceAttrs = List.of(
            keyValue("service.name", "my-service"),
            keyValue("host.name", "host-1"),
            keyValue("numeric.attr", 42L),
            keyValue("double.attr", 3.14)
        );
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            resourceAttrs,
            List.of(createGaugeMetric("test.metric", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of()))))
        );
        assertRoundTrip(request);
    }

    public void testStringArrayAttribute() throws IOException {
        List<KeyValue> resourceAttrs = List.of(keyValue("service.name", "svc"), keyValue("tags", "prod", "primary", "us-east-1"));
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            resourceAttrs,
            List.of(createGaugeMetric("test.metric", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, List.of()))))
        );
        assertRoundTrip(request);
    }

    /**
     * Verifies that when a pluggable shard router is used, rows are committed to different
     * partitions and each partition's batch has the expected doc count.
     */
    public void testShardPartitioning() throws IOException {
        int numShards = 4;
        List<KeyValue> resource = List.of(keyValue("service.name", "svc"));

        // Two data points with different timestamps → different groups → (likely) different tsids
        NumberDataPoint dp1 = createDoubleDataPoint(timestamp, startTimestamp, List.of());
        NumberDataPoint dp2 = createDoubleDataPoint(timestamp + 1_000_000_000L, startTimestamp, List.of());

        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            resource,
            List.of(createGaugeMetric("test.metric", "", List.of(dp1, dp2)))
        );

        AtomicInteger totalRows = new AtomicInteger(0);
        try (
            MetricEscfConverter.Result result = MetricEscfConverter.convert(
                request,
                MappingHints.DEFAULT_TDIGEST,
                indexVersion,
                tsid -> Math.floorMod(tsid.hashCode(), numShards)
            )
        ) {
            for (MetricEscfConverter.GroupResult g : result.groups()) {
                EscfBatch batch = result.batch(g.targetIndex(), g.shardId());
                assertThat("batch for shard " + g.shardId(), batch, notNullValue());
                assertThat("shard id in range", g.shardId(), greaterThan(-1));
                totalRows.incrementAndGet();
            }
            // Both groups should have been committed.
            assertThat(totalRows.get(), equalTo(2));
        }
    }

    public void testNativeExponentialHistogram() throws IOException {
        ExponentialHistogramDataPoint dp = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setScale(2)
            .setZeroCount(5)
            .setZeroThreshold(1e-6)
            .setSum(123.4)
            .setMin(0.1)
            .setMax(50.0)
            .setPositive(
                ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addBucketCounts(2).addBucketCounts(0).addBucketCounts(7)
            )
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(1).addBucketCounts(3).addBucketCounts(1))
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createExponentialHistogramMetric("expo.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request, MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM);
    }

    public void testExponentialHistogramAsRaw() throws IOException {
        ExponentialHistogramDataPoint dp = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setScale(1)
            .setSum(88.0)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addBucketCounts(4).addBucketCounts(6))
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createExponentialHistogramMetric("expo.raw", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request, new MappingHints(HistogramMapping.HISTOGRAM_RAW, false));
    }

    public void testExponentialHistogramAsTDigest() throws IOException {
        ExponentialHistogramDataPoint dp = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setScale(0)
            .setSum(50.0)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addBucketCounts(5).addBucketCounts(5))
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createExponentialHistogramMetric("expo.tdigest", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request); // DEFAULT_TDIGEST
    }

    /** Explicit-bucket histogram stored as an exponential_histogram (delta path with min/max injection). */
    public void testExplicitHistogramAsExponential() throws IOException {
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(100)
            .setSum(500.0)
            .setMin(0.5)
            .setMax(9.5)
            .addExplicitBounds(1.0)
            .addExplicitBounds(5.0)
            .addExplicitBounds(10.0)
            .addBucketCounts(10)
            .addBucketCounts(40)
            .addBucketCounts(30)
            .addBucketCounts(20)
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createHistogramMetric("response.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request, MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM);
    }

    /** Cumulative histogram stored as exponential_histogram (no min/max single-value bucket injection). */
    public void testExplicitHistogramAsExponentialCumulative() throws IOException {
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(60)
            .setSum(300.0)
            .addExplicitBounds(2.0)
            .addExplicitBounds(8.0)
            .addBucketCounts(20)
            .addBucketCounts(30)
            .addBucketCounts(10)
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createHistogramMetric("cumulative.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_CUMULATIVE))
        );
        assertRoundTrip(request, MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM);
    }

    public void testHistogramRaw() throws IOException {
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(70)
            .setSum(210.0)
            .addExplicitBounds(1.0)
            .addExplicitBounds(4.0)
            .addBucketCounts(20)
            .addBucketCounts(30)
            .addBucketCounts(20)
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createHistogramMetric("raw.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request, new MappingHints(HistogramMapping.HISTOGRAM_RAW, false));
    }

    public void testHistogramAggregateMetricDouble() throws IOException {
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(startTimestamp)
            .setCount(70)
            .setSum(210.0)
            .addExplicitBounds(1.0)
            .addBucketCounts(30)
            .addBucketCounts(40)
            .build();
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createHistogramMetric("agg.latency", "s", List.of(dp), AGGREGATION_TEMPORALITY_DELTA))
        );
        assertRoundTrip(request, new MappingHints(HistogramMapping.AGGREGATE_METRIC_DOUBLE, false));
    }

    /** Covers every scalar {@code AnyValue} kind plus nested and empty objects and an array. */
    public void testAttributeValueTypes() throws IOException {
        List<KeyValue> dpAttrs = List.of(
            keyValue("str", "hello"),
            keyValue("int", 123L),
            keyValue("dbl", 1.5),
            keyValue("bool", true),
            keyValue("arr", "a", "b", "c"),
            keyValue("nested", keyValueList(keyValue("a", "x"), keyValue("b", 7L))),
            keyValue("emptyobj", keyValueList())
        );
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createGaugeMetric("g", "", List.of(createDoubleDataPoint(timestamp, startTimestamp, dpAttrs))))
        );
        assertRoundTrip(request);
    }

    /** Two data points at different timestamps form two separate groups → two rows. */
    public void testMultipleTimestampsRoundTrip() throws IOException {
        NumberDataPoint dp1 = createDoubleDataPoint(timestamp, startTimestamp, List.of());
        NumberDataPoint dp2 = createLongDataPoint(timestamp + 1_000_000_000L, startTimestamp, List.of());
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(
            List.of(keyValue("service.name", "svc")),
            List.of(createGaugeMetric("m", "", List.of(dp1, dp2)))
        );
        assertRoundTrip(request);
    }

    private static Map<String, Object> asMap(BytesReference bytes) {
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    private static SummaryDataPoint createSummaryDataPoint(long timestamp, List<KeyValue> attributes) {
        return SummaryDataPoint.newBuilder()
            .setTimeUnixNano(timestamp)
            .setStartTimeUnixNano(timestamp)
            .addAllAttributes(attributes)
            .setCount(42L)
            .setSum(1234.5)
            .build();
    }
}
