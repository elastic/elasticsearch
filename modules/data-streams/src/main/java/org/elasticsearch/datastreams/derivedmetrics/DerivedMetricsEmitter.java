/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Accumulator;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.BucketKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.SeriesKey;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a closed bucket into the single document that represents it in the destination time series data stream.
 *
 * <p>The emitted value is always a partial: it covers only what this node observed during the interval. Consumers sum or otherwise
 * reduce across the {@code derived_metrics.node} dimension to get a stream-wide value. Counters are written as gauges for the same
 * reason, since a per-interval partial has no counter reset semantics to preserve.
 */
public final class DerivedMetricsEmitter {

    private static final DateFormatter TIMESTAMP_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    private DerivedMetricsEmitter() {}

    public static IndexRequest toIndexRequest(BucketKey key, Accumulator accumulator, String nodeName) {
        SeriesKey series = key.series();
        Map<String, Object> document = new LinkedHashMap<>();
        document.put(DerivedMetricsDestination.TIMESTAMP_FIELD, TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(key.bucketStartMillis())));
        document.put(DerivedMetricsDestination.METRIC_NAME_FIELD, series.metricName());
        document.put(DerivedMetricsDestination.SOURCE_FIELD, series.sourceDataStream());
        document.put(DerivedMetricsDestination.INTERVAL_FIELD, series.interval());
        document.put(DerivedMetricsDestination.NODE_FIELD, nodeName);
        List<String> names = series.dimensionNames();
        List<String> values = series.dimensionValues();
        for (int i = 0; i < names.size(); i++) {
            document.put(DerivedMetricsDestination.DIMENSION_PREFIX + names.get(i), values.get(i));
        }
        document.put(DerivedMetricsDestination.METRIC_VALUE_FIELD, accumulator.reduce(series.reduction(), key.intervalMillis()));

        return new IndexRequest(DerivedMetricsDestination.destinationFor(series.sourceDataStream(), series.interval())).opType(
            DocWriteRequest.OpType.CREATE
        ).source(document);
    }
}
