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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.HistogramMapping;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;

public class MappingHintsTests extends ESTestCase {

    private MappingHints randomDefaultHints() {
        boolean useExponentialHistogram = randomBoolean();
        return MappingHints.fromSettings(
            useExponentialHistogram
                ? OTelPlugin.HistogramMappingSettingValues.EXPONENTIAL_HISTOGRAM
                : OTelPlugin.HistogramMappingSettingValues.HISTOGRAM
        );
    }

    public void testEmptyAttributes() {
        MappingHints defaultHints = randomDefaultHints();
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of());
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertFalse(hints.docCount());
    }

    public void testNoMappingHints() {
        MappingHints defaultHints = randomDefaultHints();
        KeyValue kv = KeyValue.newBuilder()
            .setKey("some.other.key")
            .setValue(AnyValue.newBuilder().setStringValue("some_value").build())
            .build();
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(kv));
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertFalse(hints.docCount());
    }

    public void testSingleMappingHint() {
        MappingHints defaultHints = randomDefaultHints();
        // Test with just aggregate_metric_double hint
        KeyValue aggregateMetricHint = createMappingHint("aggregate_metric_double");
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(aggregateMetricHint));
        assertEquals(HistogramMapping.AGGREGATE_METRIC_DOUBLE, hints.histogramMapping());
        assertFalse(hints.docCount());

        // Test with just _doc_count hint
        KeyValue docCountHint = createMappingHint("_doc_count");
        hints = defaultHints.withConfigFromAttributes(List.of(docCountHint));
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertTrue(hints.docCount());
    }

    public void testMultipleMappingHints() {
        MappingHints defaultHints = randomDefaultHints();
        // Test with both hints
        KeyValue bothHints = createMappingHint("aggregate_metric_double", "_doc_count");
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(bothHints));
        assertEquals(HistogramMapping.AGGREGATE_METRIC_DOUBLE, hints.histogramMapping());
        assertTrue(hints.docCount());
    }

    public void testInvalidHints() {
        MappingHints defaultHints = randomDefaultHints();
        // Test with invalid hint
        KeyValue invalidHint = createMappingHint("invalid_hint");
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(invalidHint));
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertFalse(hints.docCount());

        // Test with mix of valid and invalid hints
        KeyValue mixedHints = createMappingHint("aggregate_metric_double", "invalid_hint", "_doc_count");
        hints = defaultHints.withConfigFromAttributes(List.of(mixedHints));
        assertEquals(HistogramMapping.AGGREGATE_METRIC_DOUBLE, hints.histogramMapping());
        assertTrue(hints.docCount());
    }

    public void testNonArrayValue() {
        MappingHints defaultHints = randomDefaultHints();

        // Test with non-array value
        KeyValue nonArrayHint = KeyValue.newBuilder()
            .setKey("elasticsearch.mapping.hints")
            .setValue(AnyValue.newBuilder().setStringValue("aggregate_metric_double").build())
            .build();
        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(nonArrayHint));
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertFalse(hints.docCount());
    }

    public void testNonStringArrayValues() {
        MappingHints defaultHints = randomDefaultHints();
        // Test with non-string array values
        AnyValue numberValue = AnyValue.newBuilder().setIntValue(42).build();
        AnyValue boolValue = AnyValue.newBuilder().setBoolValue(true).build();

        ArrayValue.Builder arrayBuilder = ArrayValue.newBuilder();
        arrayBuilder.addValues(numberValue);
        arrayBuilder.addValues(boolValue);

        KeyValue invalidTypeHints = KeyValue.newBuilder()
            .setKey("elasticsearch.mapping.hints")
            .setValue(AnyValue.newBuilder().setArrayValue(arrayBuilder).build())
            .build();

        MappingHints hints = defaultHints.withConfigFromAttributes(List.of(invalidTypeHints));
        assertEquals(defaultHints.histogramMapping(), hints.histogramMapping());
        assertFalse(hints.docCount());
    }

    private KeyValue createMappingHint(String... hintValues) {
        ArrayValue.Builder arrayBuilder = ArrayValue.newBuilder();
        for (String hint : hintValues) {
            arrayBuilder.addValues(AnyValue.newBuilder().setStringValue(hint));
        }

        return KeyValue.newBuilder()
            .setKey("elasticsearch.mapping.hints")
            .setValue(AnyValue.newBuilder().setArrayValue(arrayBuilder))
            .build();
    }
}
