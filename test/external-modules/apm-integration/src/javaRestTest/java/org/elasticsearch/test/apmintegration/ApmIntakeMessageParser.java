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

/**
 * Parses a metricset JSON document into a protocol-neutral {@link ReceivedTelemetry} event.
 * {@link OtlpMetricsParser} renders one such document per OTLP data point and feeds it through here.
 */
public final class ApmIntakeMessageParser {

    private ApmIntakeMessageParser() {}

    /**
     * Parse one metricset JSON document into a received telemetry event.
     *
     * @param line a single-line JSON document with a top-level {@code metricset} object
     * @throws IOException if the document is malformed or is not a metricset
     */
    public static Optional<ReceivedTelemetry> parseLine(String line) throws IOException {
        if (line == null || line.isBlank()) {
            return Optional.empty();
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line)) {
            Map<String, Object> map = parser.map();
            if (map.containsKey("metricset") == false) {
                throw new IOException("Unexpected event type: " + map.keySet());
            }
            return Optional.of(parseMetricSet(map));
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

        long timeUnixNano = (long) metricset.getOrDefault("time_unix_nano", 0L);

        Object samplesObj = metricset.get("samples");
        if (samplesObj == null) {
            return new ReceivedTelemetry.ReceivedMetricSet(scopeName, Map.of(), timeUnixNano);
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
        return new ReceivedTelemetry.ReceivedMetricSet(scopeName, Map.copyOf(samples), timeUnixNano);
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
            Object b = sample.get("bounds");
            List<Double> bounds = new ArrayList<>();
            if (b instanceof List<?> bList) {
                for (Object o : bList) {
                    if (o instanceof Number n) {
                        bounds.add(n.doubleValue());
                    } else {
                        throw new IOException("metric sample bounds element is not a number");
                    }
                }
            }
            if (c instanceof List<?> list) {
                List<Integer> counts = new ArrayList<>();
                for (Object o : list) {
                    if (o instanceof Number n) {
                        counts.add(n.intValue());
                    } else {
                        throw new IOException("metric sample counts element is not a number");
                    }
                }
                return new ReceivedTelemetry.HistogramSample(List.copyOf(bounds), List.copyOf(counts));
            }
            throw new IOException("metric sample counts is not a list");
        }
        throw new IOException("metric sample has no value or counts");
    }
}
