/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Source;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DerivedMetricsEmitterTests extends ESTestCase {

    private static final Interval TEN_SECONDS = new Interval("10s", 10_000L);

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void testEmittedDocumentCarriesInternalAndUserDimensions() {
        Map<String, Object> document = emit(Reduction.SUM, List.of("service.name", "cloud.region"), "checkout", "eu-west-1");

        assertEquals("1970-01-01T00:01:00.000Z", document.get("@timestamp"));
        assertEquals("ingest.docs.count", document.get("metric.name"));
        assertEquals("logs-my_app-default", document.get("derived_metrics.source"));
        assertEquals("10s", document.get("derived_metrics.interval"));
        assertEquals("node-1", document.get("derived_metrics.node"));
        assertEquals("checkout", document.get("dimensions.service.name"));
        assertEquals("eu-west-1", document.get("dimensions.cloud.region"));
        assertEquals(6.0, (Double) document.get("metric.value"), 0.0);
    }

    public void testDimensionsAbsentFromTheDocumentAreNotEmitted() {
        Map<String, Object> document = emit(Reduction.SUM, List.of("service.name"), new String[] { null });
        assertThat(document, not(hasKey("dimensions.service.name")));
    }

    public void testRateIsDerivedFromTheIntervalLength() {
        Map<String, Object> document = emit(Reduction.RATE, List.of(), new String[0]);
        // three observations of 1, 2 and 3 over a ten second interval
        assertEquals(0.6, (Double) document.get("metric.value"), 1e-9);
    }

    /**
     * An avg gauge emits its sum plus the observation count, so the mean is SUM(value)/SUM(count). Emitting the mean directly cannot be
     * re-aggregated: averaging per-interval means weights every interval equally regardless of how busy it was.
     */
    public void testAvgEmitsSumAndCount() {
        Map<String, Object> document = emit(Reduction.AVG, List.of(), new String[0]);
        assertEquals(6.0, (Double) document.get("metric.value"), 0.0);
        assertEquals(3L, ((Number) document.get("metric.count")).longValue());
    }

    public void testOnlyAvgCarriesACount() {
        assertThat(emit(Reduction.SUM, List.of(), new String[0]), not(hasKey("metric.count")));
        assertThat(emit(Reduction.MAX, List.of(), new String[0]), not(hasKey("metric.count")));
    }

    /**
     * A time series _id is derived from the tsid and the timestamp, so two partials of one bucket would collide. Offsetting the
     * timestamp by the partial number keeps them distinct, in the same series, and in order.
     */
    public void testPartialsAreOffsetSoTheyDoNotCollide() {
        assertEquals("1970-01-01T00:01:00.000Z", emit(Reduction.SUM, List.of(), 0, new String[0]).get("@timestamp"));
        assertEquals("1970-01-01T00:01:00.001Z", emit(Reduction.SUM, List.of(), 1, new String[0]).get("@timestamp"));
        assertEquals("1970-01-01T00:01:00.007Z", emit(Reduction.SUM, List.of(), 7, new String[0]).get("@timestamp"));
    }

    /**
     * A histogram metric emits the distribution itself, in the shape the {@code exponential_histogram} field parses. It replaces
     * metric.value rather than joining it, because the distribution already carries its own sum, count, min and max.
     */
    @SuppressWarnings("unchecked")
    public void testHistogramMetricsEmitADistributionInsteadOfAValue() {
        Map<String, Object> document = emit(Reduction.HISTOGRAM, List.of(), new String[0]);

        assertThat(document, not(hasKey("metric.value")));
        assertThat(document, not(hasKey("metric.count")));
        Map<String, Object> histogram = (Map<String, Object>) document.get("metric.histogram");

        // the three observations of 1, 2 and 3 the shared fixture records
        assertEquals(6.0, ((Number) histogram.get("sum")).doubleValue(), 1e-9);
        assertEquals(1.0, ((Number) histogram.get("min")).doubleValue(), 1e-9);
        assertEquals(3.0, ((Number) histogram.get("max")).doubleValue(), 1e-9);
        assertNotNull("a histogram must carry the scale its bucket indices are relative to", histogram.get("scale"));

        Map<String, Object> positive = (Map<String, Object>) histogram.get("positive");
        long counted = 0;
        for (Object count : (List<Object>) positive.get("counts")) {
            counted += ((Number) count).longValue();
        }
        assertEquals(3L, counted);
        assertEquals(((List<Object>) positive.get("indices")).size(), ((List<Object>) positive.get("counts")).size());
    }

    /**
     * The emitted shape has to be exactly what the field mapper reads, so the surest check is to parse it back and compare the parts a
     * consumer would use.
     */
    public void testTheEmittedDistributionParsesBackIntoAHistogram() throws Exception {
        IndexRequest request = request(Reduction.HISTOGRAM, List.of(), 0, new String[0]);
        Map<String, Object> document = XContentHelper.convertToMap(request.source(), false, request.getContentType()).v2();

        @SuppressWarnings("unchecked")
        ExponentialHistogram histogram = ExponentialHistogramXContent.parseForTesting(
            (Map<String, Object>) document.get("metric.histogram")
        );
        assertEquals(3L, histogram.valueCount());
        assertEquals(6.0, histogram.sum(), 1e-9);
        assertEquals(1.0, histogram.min(), 1e-9);
        assertEquals(3.0, histogram.max(), 1e-9);
    }

    public void testDestinationIsDerivedFromTheSourceDataStream() {
        IndexRequest request = request(Reduction.SUM, List.of(), 0, new String[0]);
        assertEquals("derived-metrics-logs-my_app-default-10s", request.index());
        assertEquals(DocWriteRequest.OpType.CREATE, request.opType());
    }

    private Map<String, Object> emit(Reduction reduction, List<String> names, String... values) {
        return emit(reduction, names, 0, values);
    }

    private Map<String, Object> emit(Reduction reduction, List<String> names, int partial, String... values) {
        IndexRequest request = request(reduction, names, partial, values);
        return XContentHelper.convertToMap(request.source(), false, request.getContentType()).v2();
    }

    private IndexRequest request(Reduction reduction, List<String> names, int partial, String... values) {
        CompiledMetric metric = new CompiledMetric(
            "ingest.docs.count",
            Trigger.SUCCESS,
            reduction,
            DerivedMetricsPredicate.MATCH_ALL,
            new Source.Constant(1.0),
            names,
            new int[names.size()],
            0,
            TEN_SECONDS
        );
        TableKey key = new TableKey(ProjectId.DEFAULT, "logs-my_app-default", metric, 60_000L, TEN_SECONDS.millis());
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            Scratch scratch = new Scratch();
            for (double value : new double[] { 1.0, 2.0, 3.0 }) {
                buffer.record(key, values, scratch, value);
            }
            var drained = buffer.drainAll();
            try {
                return DerivedMetricsEmitter.toIndexRequest(key, drained.get(0).table(), 0, new BytesRef(), "node-1", partial);
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }
}
