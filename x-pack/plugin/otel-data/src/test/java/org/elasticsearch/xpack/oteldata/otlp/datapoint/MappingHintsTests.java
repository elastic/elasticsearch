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
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;

public class MappingHintsTests extends ESTestCase {

    public void testEmptyAttributes() {
        MappingHints hints = MappingHints.fromAttributes(List.of());
        assertFalse(hints.aggregateMetricDouble());
        assertFalse(hints.docCount());
    }

    public void testNoMappingHints() {
        KeyValue kv = KeyValue.newBuilder()
            .setKey("some.other.key")
            .setValue(AnyValue.newBuilder().setStringValue("some_value").build())
            .build();
        MappingHints hints = MappingHints.fromAttributes(List.of(kv));
        assertFalse(hints.aggregateMetricDouble());
        assertFalse(hints.docCount());
    }

    public void testSingleMappingHint() {
        // Test with just aggregate_metric_double hint
        KeyValue aggregateMetricHint = createMappingHint("aggregate_metric_double");
        MappingHints hints = MappingHints.fromAttributes(List.of(aggregateMetricHint));
        assertTrue(hints.aggregateMetricDouble());
        assertFalse(hints.docCount());

        // Test with just _doc_count hint
        KeyValue docCountHint = createMappingHint("_doc_count");
        hints = MappingHints.fromAttributes(List.of(docCountHint));
        assertFalse(hints.aggregateMetricDouble());
        assertTrue(hints.docCount());
    }

    public void testMultipleMappingHints() {
        // Test with both hints
        KeyValue bothHints = createMappingHint("aggregate_metric_double", "_doc_count");
        MappingHints hints = MappingHints.fromAttributes(List.of(bothHints));
        assertTrue(hints.aggregateMetricDouble());
        assertTrue(hints.docCount());
    }

    public void testInvalidHints() {
        // Test with invalid hint
        KeyValue invalidHint = createMappingHint("invalid_hint");
        MappingHints hints = MappingHints.fromAttributes(List.of(invalidHint));
        assertFalse(hints.aggregateMetricDouble());
        assertFalse(hints.docCount());

        // Test with mix of valid and invalid hints
        KeyValue mixedHints = createMappingHint("aggregate_metric_double", "invalid_hint", "_doc_count");
        hints = MappingHints.fromAttributes(List.of(mixedHints));
        assertTrue(hints.aggregateMetricDouble());
        assertTrue(hints.docCount());
    }

    public void testNonArrayValue() {
        // Test with non-array value
        KeyValue nonArrayHint = KeyValue.newBuilder()
            .setKey("elasticsearch.mapping.hints")
            .setValue(AnyValue.newBuilder().setStringValue("aggregate_metric_double").build())
            .build();
        MappingHints hints = MappingHints.fromAttributes(List.of(nonArrayHint));
        assertFalse(hints.aggregateMetricDouble());
        assertFalse(hints.docCount());
    }

    public void testNonStringArrayValues() {
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

        MappingHints hints = MappingHints.fromAttributes(List.of(invalidTypeHints));
        assertFalse(hints.aggregateMetricDouble());
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
