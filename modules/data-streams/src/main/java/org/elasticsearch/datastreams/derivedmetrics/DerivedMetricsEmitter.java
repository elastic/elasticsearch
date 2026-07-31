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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one series of a closed bucket into the document that represents it in the destination time series data stream.
 *
 * <p>The emitted value is always a partial: it covers only what this node observed during the interval. Consumers sum or otherwise
 * reduce across the {@code derived_metrics.node} dimension to get a stream-wide value. Counters are written as gauges for the same
 * reason, since a per-interval partial has no counter reset semantics to preserve.
 */
public final class DerivedMetricsEmitter {

    private static final DateFormatter TIMESTAMP_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    private DerivedMetricsEmitter() {}

    /**
     * @param partial which partial of this bucket this is, normally zero. A time series {@code _id} is derived from the tsid and the
     *                timestamp, so two partials of the same series and bucket would collide and the second would be rejected. Offsetting
     *                the timestamp by the partial number keeps them distinct while leaving them in the same series and the same
     *                date_histogram bucket, and orders them for first_value and last_value.
     */
    public static IndexRequest toIndexRequest(
        TableKey key,
        DerivedMetricsSeriesTable table,
        long ordinal,
        BytesRef spare,
        String nodeName,
        int partial
    ) {
        CompiledMetric metric = key.metric();
        List<String> names = metric.dimensions();
        String[] values = table.dimensionsOf(ordinal, names.size(), spare);

        Map<String, Object> document = new LinkedHashMap<>();
        document.put(
            DerivedMetricsDestination.TIMESTAMP_FIELD,
            TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(key.bucketStartMillis() + partial))
        );
        document.put(DerivedMetricsDestination.METRIC_NAME_FIELD, metric.name());
        document.put(DerivedMetricsDestination.SOURCE_FIELD, key.sourceDataStream());
        document.put(DerivedMetricsDestination.INTERVAL_FIELD, metric.interval().name());
        document.put(DerivedMetricsDestination.NODE_FIELD, nodeName);
        for (int i = 0; i < names.size(); i++) {
            if (values[i] != null) {
                document.put(DerivedMetricsDestination.DIMENSION_PREFIX + names.get(i), values[i]);
            }
        }
        document.put(DerivedMetricsDestination.METRIC_VALUE_FIELD, table.reduce(ordinal, metric.reduction(), key.intervalMillis()));
        if (metric.reduction() == Reduction.AVG) {
            // An avg gauge emits its sum in metric.value and its count alongside, so the mean is SUM(value)/SUM(count). Emitting the
            // mean directly cannot be re-aggregated: averaging per-interval means weights every interval equally, which reads far too
            // low whenever the busy intervals differ from the quiet ones.
            document.put(DerivedMetricsDestination.METRIC_COUNT_FIELD, table.countOf(ordinal));
        }

        return new IndexRequest(DerivedMetricsDestination.destinationFor(key.sourceDataStream(), metric.interval().name())).opType(
            DocWriteRequest.OpType.CREATE
        ).source(document);
    }
}
