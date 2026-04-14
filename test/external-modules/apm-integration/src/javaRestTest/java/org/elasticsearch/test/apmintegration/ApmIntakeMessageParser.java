/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Parses a single line of APM intake NDJSON into a protocol-neutral {@link ReceivedTelemetry} event.
 * Intake-specific; a future OTLP decoder will produce the same ADT from OTLP payloads.
 */
public final class ApmIntakeMessageParser {

    static final Set<String> IGNORED_EVENT_NAMES = Set.of("metadata");

    private ApmIntakeMessageParser() {}

    /**
     * Parse one NDJSON line into a received telemetry event, or {@link Optional#empty() empty} if the line should be skipped.
     *
     * @param line one line of NDJSON
     * @throws IOException if the line is malformed or invalid
     */
    public static Optional<ReceivedTelemetry> parseLine(String line) throws IOException {
        if (line == null || line.isBlank()) {
            return Optional.empty();
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line)) {
            Map<String, Object> map = parser.map();
            if (map.containsKey("metricset")) {
                return Optional.of(parseMetricSet(map));
            } else if (map.containsKey("transaction")) {
                return Optional.of(parseTransaction(map));
            } else if (map.containsKey("span")) {
                return Optional.of(parseSpan(map));
            } else if (IGNORED_EVENT_NAMES.containsAll(map.keySet())) {
                // We don't care about these
                return Optional.empty();
            } else {
                throw new IOException("Unexpected event type: " + map.keySet());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static ReceivedTelemetry parseMetricSet(Map<String, Object> root) throws IOException {
        Object metricsetObj = root.get("metricset");
        if ((metricsetObj instanceof Map<?, ?> == false)) {
            throw new IOException("metricset missing or not an object");
        }
        Map<String, Object> metricset = (Map<String, Object>) metricsetObj;
        Map<String, Object> tags = (Map<String, Object>) metricset.getOrDefault("tags", Collections.emptyMap());
        String scopeName = tags.get("otel_instrumentation_scope_name") != null
            ? tags.get("otel_instrumentation_scope_name").toString()
            : "";

        Object samplesObj = metricset.get("samples");
        if (samplesObj == null) {
            return new ReceivedTelemetry.ReceivedMetricSet(scopeName, Map.of());
        }
        if (samplesObj instanceof Map<?, ?> == false) {
            throw new IOException("metricset.samples is not an object");
        }
        Map<String, Object> samplesMap = (Map<String, Object>) samplesObj;

        Map<String, ReceivedTelemetry.ReceivedMetricValue> samples = new HashMap<>();
        for (Map.Entry<String, Object> entry : samplesMap.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> sampleObj) {
                samples.put(entry.getKey(), parseSample((Map<String, Object>) sampleObj));
            } else {
                throw new IOException("metricset.samples entry [" + entry.getKey() + "] is not an object");
            }
        }
        return new ReceivedTelemetry.ReceivedMetricSet(scopeName, Map.copyOf(samples));
    }

    private static ReceivedTelemetry.ReceivedMetricValue parseSample(Map<String, Object> sample) throws IOException {
        if (sample.containsKey("value")) {
            Object v = sample.get("value");
            if (v instanceof Number n) {
                return new ReceivedTelemetry.ValueSample(n);
            }
            throw new IOException("metric sample has value that is not a number");
        }
        if (sample.containsKey("counts")) {
            Object c = sample.get("counts");
            if (c instanceof List<?> list) {
                List<Integer> counts = new ArrayList<>();
                for (Object o : list) {
                    if (o instanceof Number n) {
                        counts.add(n.intValue());
                    } else {
                        throw new IOException("metric sample counts element is not a number");
                    }
                }
                return new ReceivedTelemetry.HistogramSample(List.copyOf(counts));
            }
            throw new IOException("metric sample counts is not a list");
        }
        throw new IOException("metric sample has no value or counts");
    }

    @SuppressWarnings("unchecked")
    private static ReceivedTelemetry parseTransaction(Map<String, Object> root) throws IOException {
        Object transactionObj = root.get("transaction");
        if ((transactionObj instanceof Map<?, ?> == false)) {
            throw new IOException("transaction missing or not an object");
        }
        Map<String, Object> transaction = (Map<String, Object>) transactionObj;
        String name = getString(transaction, "name");
        String traceId = getString(transaction, "trace_id");
        String id = getString(transaction, "id");
        if (name == null || traceId == null) {
            throw new IOException("transaction missing name or trace_id");
        }
        String spanId = id != null ? id : "";
        return new ReceivedTelemetry.ReceivedSpan(name, traceId, spanId, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static ReceivedTelemetry parseSpan(Map<String, Object> root) throws IOException {
        Object spanObj = root.get("span");
        if (spanObj == null || (spanObj instanceof Map<?, ?> == false)) {
            throw new IOException("span missing or not an object");
        }
        Map<String, Object> span = (Map<String, Object>) spanObj;
        String name = getString(span, "name");
        String traceId = getString(span, "trace_id");
        String id = getString(span, "id");
        if (name == null || traceId == null || id == null) {
            throw new IOException("span missing name, trace_id, or id");
        }
        String parentId = getString(span, "parent_id");
        if (parentId == null) {
            parentId = getString(span, "transaction_id");
        }
        return new ReceivedTelemetry.ReceivedSpan(name, traceId, id, Optional.ofNullable(parentId));
    }

    private static String getString(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v != null ? v.toString() : null;
    }
}
