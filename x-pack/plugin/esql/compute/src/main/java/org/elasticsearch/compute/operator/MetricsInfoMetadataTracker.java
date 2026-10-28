/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allows METRICS_INFO drivers to stop once matching documents have supplied every metadata value allowed by the mappings.
 * Mapping fields are upper bounds only. They are never added to the result without a matching document.
 * Each metric and data-stream pair must be fully observed by at least one driver before all drivers can stop.
 */
public final class MetricsInfoMetadataTracker {
    private record MetricKey(String metricName, String dataStream) {}

    private static final class ExpectedMetadata {
        private final Set<String> units = new HashSet<>();
        private final Set<String> fieldTypes = new HashSet<>();
        private final Set<String> metricTypes = new HashSet<>();
        private final Set<String> dimensionFields;
        private final AtomicBoolean complete = new AtomicBoolean();

        private ExpectedMetadata(Set<String> dimensionFields) {
            this.dimensionFields = dimensionFields;
        }
    }

    private final Map<MetricKey, ExpectedMetadata> expectedByMetric;
    private final AtomicInteger remainingMetrics;

    /**
     * Builds conservative bounds across all backing indices assigned to this data-node plan.
     * Dimension bounds must include every possible metadata leaf name. Callers must disable early termination when those names are unknown.
     */
    public MetricsInfoMetadataTracker(
        Map<String, List<MetricFieldInfo>> metricsByDataStream,
        Map<String, Set<String>> dimensionFieldsByDataStream
    ) {
        Map<MetricKey, ExpectedMetadata> expected = new HashMap<>();
        metricsByDataStream.forEach((dataStream, metrics) -> {
            Set<String> dimensions = Set.copyOf(dimensionFieldsByDataStream.getOrDefault(dataStream, Set.of()));
            for (MetricFieldInfo metric : metrics) {
                ExpectedMetadata metadata = expected.computeIfAbsent(
                    new MetricKey(metric.name(), dataStream),
                    k -> new ExpectedMetadata(dimensions)
                );
                if (metric.unit() != null) {
                    metadata.units.add(metric.unit());
                }
                metadata.fieldTypes.add(metric.fieldType());
                metadata.metricTypes.add(metric.metricType());
            }
        });
        expectedByMetric = Map.copyOf(expected);
        // Changing signatures can regroup intermediate rows. Keep the full scan for those mappings.
        boolean changingSignatures = expected.values()
            .stream()
            .anyMatch(metadata -> metadata.units.size() > 1 || metadata.fieldTypes.size() > 1 || metadata.metricTypes.size() > 1);
        remainingMetrics = new AtomicInteger(changingSignatures ? -1 : expected.size());
    }

    /**
     * Records metadata accumulated from matching documents by one driver. The supplied sets remain owned by that driver.
     * A metric is complete only when those observations cover its full mapping bounds, including all possible dimension fields.
     */
    public void observe(
        String metricName,
        String dataStream,
        Set<String> units,
        Set<String> fieldTypes,
        Set<String> metricTypes,
        Set<String> dimensionFields
    ) {
        if (remainingMetrics.get() < 0) {
            return;
        }
        ExpectedMetadata expected = expectedByMetric.get(new MetricKey(metricName, dataStream));
        if (expected != null
            && expected.complete.get() == false
            && expected.units.equals(units)
            && expected.fieldTypes.equals(fieldTypes)
            && expected.metricTypes.equals(metricTypes)
            && expected.dimensionFields.equals(dimensionFields)
            && expected.complete.compareAndSet(false, true)) {
            remainingMetrics.decrementAndGet();
        }
    }

    /** Returns whether every possible metric and data-stream pair has a complete contribution in a driver's result. */
    public boolean isComplete() {
        return remainingMetrics.get() == 0;
    }
}
