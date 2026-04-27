/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Protocol-neutral representation of received telemetry. Both APM intake NDJSON and
 * OTLP decoders produce these so that tests can assert in a protocol-independent way.
 */
public sealed interface ReceivedTelemetry {

    /**
     * A set of metrics from a single instrumentation scope (e.g. "elasticsearch").
     */
    record ReceivedMetricSet(String instrumentationScopeName, Map<String, ReceivedMetricValue> samples) implements ReceivedTelemetry {
        public ReceivedMetricSet {
            requireNonNull(instrumentationScopeName);
            requireNonNull(samples);
            samples.forEach((k, v) -> {
                Objects.requireNonNull(k);
                Objects.requireNonNull(v);
            });
        }
    }

    /**
     * Root span (aka "transaction") has {@code parentSpanId} empty.
     * {@code attributes} is a flat map of span attributes using dot-notation keys.
     * For APM intake NDJSON these are nested keys (e.g. {@code "context.request.method"});
     * for OTLP, {@link org.elasticsearch.test.apmintegration.OtlpTracesParser} normalises
     * raw OTel semantic keys into the {@code otel.attributes.*} namespace (e.g.
     * {@code "otel.attributes.http.method"}) so that both export paths satisfy the same
     * assertions in {@code AbstractTracesIT}.
     */
    record ReceivedSpan(String name, String traceId, String spanId, Optional<String> parentSpanId, Map<String, Object> attributes)
        implements
            ReceivedTelemetry {
        public ReceivedSpan {
            requireNonNull(name);
            requireNonNull(traceId);
            requireNonNull(spanId);
            parentSpanId.ifPresent(Objects::requireNonNull);
            attributes = Map.copyOf(requireNonNull(attributes));
        }
    }

    /**
     * Value of a single metric sample: either a scalar or histogram counts.
     */
    sealed interface ReceivedMetricValue {}

    /**
     * A single scalar value.
     * @param value the value
     */
    record ValueSample(Number value) implements ReceivedMetricValue {
        public ValueSample {
            requireNonNull(value);
        }
    }

    /**
     * A histogram of counts.
     * @param counts the individual count values
     */
    record HistogramSample(List<Integer> counts) implements ReceivedMetricValue {
        public HistogramSample {
            requireNonNull(counts);
            counts.forEach(Objects::requireNonNull);
        }
    }
}
