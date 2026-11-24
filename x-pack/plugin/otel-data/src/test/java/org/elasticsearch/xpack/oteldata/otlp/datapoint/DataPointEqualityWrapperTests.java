/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.hash.MurmurHash3.Hash128;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointTsidFunnel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;

/**
 * Verifies that {@link DataPointEqualityWrapper} never groups data points that have different data point TSIDs or timestamps,
 * which is how data points were grouped before the TSID computation was moved out of the grouping.
 */
public class DataPointEqualityWrapperTests extends ESTestCase {

    // small pools so that random data points frequently share keys and values
    private static final List<String> KEYS = List.of("a", "b", "a.b", "data_stream.dataset", "elasticsearch.mapping.hints");
    private static final List<String> UNITS = List.of("", "By");

    private final BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();

    public void testEqualityImpliesSameTsid() {
        for (int i = 0; i < 1000; i++) {
            DataPoint a = randomDataPoint();
            DataPoint b = randomBoolean() ? reorderAttributes(a) : randomDataPoint();
            assertEqualityImpliesSameTsid(a, b);
        }
    }

    public void testAttributeOrderIsIgnored() {
        List<KeyValue> attributes = List.of(keyValue("a", "1"), keyValue("b", 2L), keyValue("c", true), keyValue("d", "x", "y"));
        assertGrouped(attributes, List.of(attributes.get(2), attributes.get(0), attributes.get(3), attributes.get(1)), true);
    }

    public void testIgnoredAttributesAreSkipped() {
        assertGrouped(
            List.of(keyValue("a", "1"), keyValue("elasticsearch.mapping.hints", "aggregate_metric_double")),
            List.of(keyValue("a", "1")),
            true
        );
    }

    public void testNestedAttributesAreGroupedRegardlessOfOrder() {
        KeyValue nested = keyValue("a", KeyValueList.newBuilder().addValues(keyValue("b", "1")).addValues(keyValue("c", 2L)).build());
        KeyValue other = keyValue("d", "x");
        KeyValue ignored = keyValue("elasticsearch.mapping.hints", "aggregate_metric_double");
        assertGrouped(List.of(nested, other, ignored), List.of(other, nested), true);
    }

    public void testNestedAttributeOrderIsIgnored() {
        KeyValue b = keyValue("b", "1");
        KeyValue c = keyValue("c", 2L);
        KeyValue ignored = keyValue("elasticsearch.mapping.hints", "aggregate_metric_double");
        KeyValue nested = keyValue("a", KeyValueList.newBuilder().addValues(b).addValues(c).addValues(ignored).build());
        KeyValue reordered = keyValue("a", KeyValueList.newBuilder().addValues(c).addValues(b).build());
        assertGrouped(List.of(nested), List.of(reordered), true);
    }

    public void testSwappedValuesAreNotGrouped() {
        assertGrouped(List.of(keyValue("a", "1"), keyValue("b", "2")), List.of(keyValue("a", "2"), keyValue("b", "1")), false);
    }

    public void testDuplicateKeysAreNotCollapsed() {
        assertGrouped(List.of(keyValue("a", "1"), keyValue("a", "2")), List.of(keyValue("a", "2")), false);
        assertGrouped(List.of(keyValue("a", "1"), keyValue("a", "2")), List.of(keyValue("a", "2"), keyValue("a", "1")), false);
    }

    public void testNestedAttributesSharingAPathWithDottedKeysAreNotGrouped() {
        KeyValue dotted = keyValue("a.b", "1");
        KeyValue nested = keyValue("a", KeyValueList.newBuilder().addValues(keyValue("b", "2")).build());
        assertGrouped(List.of(dotted, nested), List.of(nested, dotted), false);
        AnyValue nestedValue = nested.getValue();
        KeyValue nestedInArray = attribute(
            "a",
            AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder().addValues(nestedValue)).build()
        );
        assertGrouped(List.of(dotted, nestedInArray), List.of(nestedInArray, dotted), false);
    }

    public void testArrayOrderMatters() {
        assertGrouped(List.of(keyValue("a", "1", "2")), List.of(keyValue("a", "2", "1")), false);
    }

    public void testValueTypesAreDistinguished() {
        assertGrouped(List.of(keyValue("a", 1L)), List.of(keyValue("a", 1.0)), false);
        assertGrouped(List.of(keyValue("a", "true")), List.of(keyValue("a", true)), false);
    }

    private void assertGrouped(List<KeyValue> attributesA, List<KeyValue> attributesB, boolean expectGrouped) {
        Metric metric = createGaugeMetric("metric", "", List.of());
        DataPoint a = new DataPoint.Number(createDoubleDataPoint(1, 0, attributesA), metric);
        DataPoint b = new DataPoint.Number(createDoubleDataPoint(1, 0, attributesB), metric);
        DataPointEqualityWrapper wrapperA = new DataPointEqualityWrapper(a);
        DataPointEqualityWrapper wrapperB = new DataPointEqualityWrapper(b);
        assertEquals(expectGrouped, wrapperA.equals(wrapperB));
        // most attributes that must not be grouped already differ in their hash code, so also check
        // the attribute comparison directly, which would have to tell them apart on a hash collision
        assertEquals(expectGrouped, wrapperA.attributesEqual(wrapperB));
        assertEqualityImpliesSameTsid(a, b);
    }

    private void assertEqualityImpliesSameTsid(DataPoint a, DataPoint b) {
        DataPointEqualityWrapper wrapperA = new DataPointEqualityWrapper(a);
        DataPointEqualityWrapper wrapperB = new DataPointEqualityWrapper(b);
        String message = a.getAttributes() + " vs " + b.getAttributes();
        boolean equal = wrapperA.equals(wrapperB);
        assertEquals(message, equal, wrapperB.equals(wrapperA));
        if (equal) {
            assertEquals(message, wrapperA.hashCode(), wrapperB.hashCode());
            assertEquals(message, tsidHash(a), tsidHash(b));
            assertEquals(message, a.getTimestampUnixNano(), b.getTimestampUnixNano());
            assertEquals(message, a.getStartTimestampUnixNano(), b.getStartTimestampUnixNano());
        }
        // checked regardless of the hash codes, which rarely collide for random attributes
        if (wrapperA.attributesEqual(wrapperB)) {
            assertEquals(message, attributesTsidHash(a.getAttributes()), attributesTsidHash(b.getAttributes()));
        }
    }

    private Hash128 tsidHash(DataPoint dataPoint) {
        return DataPointTsidFunnel.forDataPoint(byteStringAccessor, dataPoint, 0).hash();
    }

    /** The TSID of a data point that only differs in the given attributes from all others. */
    private Hash128 attributesTsidHash(List<KeyValue> attributes) {
        Metric metric = createGaugeMetric("metric", "", List.of());
        return tsidHash(new DataPoint.Number(createDoubleDataPoint(1, 0, attributes), metric));
    }

    private DataPoint randomDataPoint() {
        long timestamp = randomLongBetween(1, 2);
        List<KeyValue> attributes = randomList(0, 4, () -> attribute(randomFrom(KEYS), randomAnyValue(2)));
        var numberDataPoint = createDoubleDataPoint(timestamp, randomLongBetween(0, 1), attributes);
        String unit = randomFrom(UNITS);
        Metric metric = randomBoolean()
            ? createGaugeMetric("metric", unit, List.of(numberDataPoint))
            : createSumMetric(
                "metric",
                unit,
                List.of(numberDataPoint),
                true,
                randomFrom(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            );
        return new DataPoint.Number(numberDataPoint, metric);
    }

    private static KeyValue attribute(String key, AnyValue value) {
        return KeyValue.newBuilder().setKey(key).setValue(value).build();
    }

    private AnyValue randomAnyValue(int depth) {
        int type = randomIntBetween(0, depth > 0 ? 6 : 4);
        AnyValue.Builder builder = AnyValue.newBuilder();
        return switch (type) {
            case 0 -> builder.setStringValue(randomFrom("1", "2", "true")).build();
            case 1 -> builder.setIntValue(randomLongBetween(1, 2)).build();
            case 2 -> builder.setDoubleValue(randomFrom(1.0, 2.0, 0.0, -0.0, Double.NaN)).build();
            case 3 -> builder.setBoolValue(randomBoolean()).build();
            case 4 -> builder.setBytesValue(ByteString.copyFromUtf8(randomFrom("1", "2"))).build();
            case 5 -> builder.setArrayValue(ArrayValue.newBuilder().addAllValues(randomList(0, 3, () -> randomAnyValue(depth - 1))))
                .build();
            default -> builder.setKvlistValue(
                KeyValueList.newBuilder().addAllValues(randomList(0, 3, () -> attribute(randomFrom(KEYS), randomAnyValue(depth - 1))))
            ).build();
        };
    }

    private DataPoint reorderAttributes(DataPoint dataPoint) {
        DataPoint.Number number = (DataPoint.Number) dataPoint;
        List<KeyValue> attributes = new ArrayList<>(number.getAttributes());
        Collections.shuffle(attributes, random());
        return new DataPoint.Number(number.dataPoint().toBuilder().clearAttributes().addAllAttributes(attributes).build(), number.metric());
    }
}
