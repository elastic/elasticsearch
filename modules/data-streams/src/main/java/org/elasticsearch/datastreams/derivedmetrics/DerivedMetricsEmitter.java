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
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

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

        try (XContentBuilder document = XContentFactory.jsonBuilder()) {
            document.startObject();
            document.field(
                DerivedMetricsDestination.TIMESTAMP_FIELD,
                TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(key.bucketStartMillis() + partial))
            );
            document.field(DerivedMetricsDestination.METRIC_NAME_FIELD, metric.name());
            document.field(DerivedMetricsDestination.SOURCE_FIELD, key.sourceDataStream());
            document.field(DerivedMetricsDestination.INTERVAL_FIELD, metric.interval().name());
            document.field(DerivedMetricsDestination.NODE_FIELD, nodeName);
            // Makes the destination self-describing: without this the correct way to combine metric.value across nodes and buckets is
            // knowable only from the source stream's configuration, and a consumer that guesses wrong is wrong invisibly.
            document.field(DerivedMetricsDestination.REDUCTION_FIELD, metric.reduction().name().toLowerCase(Locale.ROOT));
            for (int i = 0; i < names.size(); i++) {
                if (values[i] != null) {
                    document.field(DerivedMetricsDestination.DIMENSION_PREFIX + names.get(i), values[i]);
                }
            }
            if (metric.reduction().isHistogram()) {
                // The distribution carries its own sum, count, min and max, so it replaces metric.value rather than joining it.
                try (ReleasableExponentialHistogram histogram = table.histogramOf(ordinal)) {
                    document.field(DerivedMetricsDestination.METRIC_HISTOGRAM_FIELD);
                    ExponentialHistogramXContent.serialize(document, histogram);
                }
            } else {
                document.field(
                    DerivedMetricsDestination.METRIC_VALUE_FIELD,
                    table.reduce(ordinal, metric.reduction(), key.intervalMillis())
                );
                if (metric.reduction() == Reduction.FIRST || metric.reduction() == Reduction.LAST) {
                    // These are the only reductions whose cross-node value depends on ordering rather than on an associative combine, so
                    // the observation time has to travel with the value for the cluster-wide answer to be recoverable at all.
                    document.field(
                        DerivedMetricsDestination.METRIC_OBSERVED_AT_FIELD,
                        TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(table.observedAtOf(ordinal)))
                    );
                }
                if (metric.reduction() == Reduction.AVG) {
                    // An avg gauge emits its sum in metric.value and its count alongside, so the mean is SUM(value)/SUM(count). Emitting
                    // the mean directly cannot be re-aggregated: averaging per-interval means weights every interval equally, which reads
                    // far too low whenever the busy intervals differ from the quiet ones.
                    document.field(DerivedMetricsDestination.METRIC_COUNT_FIELD, table.countOf(ordinal));
                }
            }
            document.endObject();

            return new IndexRequest(DerivedMetricsDestination.destinationFor(key.sourceDataStream(), metric.interval().name())).opType(
                DocWriteRequest.OpType.CREATE
            ).source(document);
        } catch (IOException e) {
            // building an in-memory document cannot fail on IO, so there is nothing useful a caller could do about this
            throw new UncheckedIOException("unable to build a derived metrics document for [" + metric.name() + "]", e);
        }
    }
}
