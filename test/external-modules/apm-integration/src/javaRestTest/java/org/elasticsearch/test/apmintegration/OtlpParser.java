/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared base for OTLP protobuf parsers ({@link OtlpTracesParser}, {@link OtlpMetricsParser}).
 * Holds the resource-attribute extraction primitives that both signal types use to convert
 * OTLP {@link KeyValue} lists into the protocol-neutral {@link ReceivedTelemetry} shape.
 */
abstract class OtlpParser {

    /**
     * Convert an OTLP attribute list into a flat {@code Map<String, Object>} keyed verbatim by the
     * OTLP key. The OTel SemConv keys (e.g. {@code service.name}, {@code k8s.cluster.name}) pass
     * through unchanged, which is what {@code AbstractTelemetryIT.assertSdkResourceAttributes}
     * asserts on.
     */
    static Map<String, Object> extractRawAttributes(List<KeyValue> kvs) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (KeyValue kv : kvs) {
            attributes.put(kv.getKey(), toJavaValue(kv.getValue()));
        }
        return Map.copyOf(attributes);
    }

    static Object toJavaValue(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BOOL_VALUE -> value.getBoolValue();
            default -> value.toString();
        };
    }
}
