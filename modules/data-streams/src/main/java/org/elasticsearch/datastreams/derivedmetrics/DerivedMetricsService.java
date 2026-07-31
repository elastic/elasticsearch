/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Accumulator;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.BucketKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.SeriesKey;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the node-local derived metrics state: it observes writes handed to it by {@link DerivedMetricsIndexingListener}, buffers them per
 * interval, and periodically flushes closed intervals into the managed destination streams.
 *
 * <p>All observation happens on the write thread, so the per-document path is kept to evaluating precompiled predicates and, only when
 * some metric actually needs it, reading a small filtered slice of the document's source. Everything expensive — building documents,
 * bulk indexing — happens on the flush thread.
 */
public class DerivedMetricsService implements Closeable {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsService.class);

    public static final Setting<TimeValue> FLUSH_INTERVAL = Setting.timeSetting(
        "data_streams.derived_metrics.flush_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * How long after an interval ends its bucket stays open, so that writes still in flight when the boundary passes are not lost.
     */
    public static final Setting<TimeValue> FLUSH_GRACE_PERIOD = Setting.timeSetting(
        "data_streams.derived_metrics.flush_grace_period",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_SERIES_PER_NODE = Setting.intSetting(
        "data_streams.derived_metrics.max_series_per_node",
        10_000,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Ceiling for any single source stream, so the node budget is not first-come-first-served. Defaults to the whole node budget, which
     * preserves today's behaviour until an operator chooses to divide it.
     */
    public static final Setting<Integer> MAX_SERIES_PER_STREAM = Setting.intSetting(
        "data_streams.derived_metrics.max_series_per_stream",
        MAX_SERIES_PER_NODE,
        1,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> BULK_SIZE = Setting.intSetting(
        "data_streams.derived_metrics.bulk_size",
        1_000,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Ceiling on bulk requests outstanding at once. Emission is fire and forget, so a destination that cannot keep up would otherwise
     * let every flush add to a queue with nothing bounding it.
     */
    public static final Setting<Integer> MAX_IN_FLIGHT_BULKS = Setting.intSetting(
        "data_streams.derived_metrics.max_in_flight_bulks",
        8,
        1,
        Setting.Property.NodeScope
    );

    private final Client client;
    private final ThreadPool threadPool;
    private final DerivedMetricsBuffer buffer;
    private final TimeValue flushInterval;
    private final long graceMillis;
    private final int bulkSize;
    private final int maxInFlightBulks;
    private final String nodeName;
    private final AtomicInteger inFlightBulks = new AtomicInteger();
    private final AtomicLong droppedForBackpressure = new AtomicLong();

    private volatile Scheduler.Cancellable scheduled;
    private volatile boolean closed;
    private long lastReportedDrops;

    public DerivedMetricsService(Settings settings, Client client, ThreadPool threadPool, String nodeName) {
        this.client = new OriginSettingClient(client, DataStreamDerivedMetrics.DERIVED_METRICS_ORIGIN);
        this.threadPool = threadPool;
        this.buffer = new DerivedMetricsBuffer(MAX_SERIES_PER_NODE.get(settings), MAX_SERIES_PER_STREAM.get(settings));
        this.flushInterval = FLUSH_INTERVAL.get(settings);
        this.graceMillis = FLUSH_GRACE_PERIOD.get(settings).millis();
        this.bulkSize = BULK_SIZE.get(settings);
        this.maxInFlightBulks = MAX_IN_FLIGHT_BULKS.get(settings);
        this.nodeName = nodeName;
    }

    public void init() {
        scheduled = threadPool.scheduleWithFixedDelay(this::flush, flushInterval, threadPool.executor(ThreadPool.Names.MANAGEMENT));
    }

    /**
     * Records one write against the derived metrics of its source data stream.
     *
     * @param succeeded whether the document was indexed. Failed writes only feed metrics triggered by failures, but they still carry
     *                  their dimensions, so a failure series is broken down the same way a success series is.
     */
    public void record(
        ProjectId project,
        String sourceDataStream,
        CompiledDerivedMetrics compiled,
        ParsedDocument parsedDocument,
        boolean succeeded
    ) {
        Trigger trigger = succeeded ? Trigger.SUCCESS : Trigger.FAILURE;
        if (compiled.triggers().contains(trigger) == false) {
            return;
        }
        Map<String, Object> source = null;
        if (compiled.needsSource() && parsedDocument != null) {
            source = DerivedMetricsSourceReader.read(parsedDocument, compiled.requiredPaths());
        }
        long now = threadPool.absoluteTimeInMillis();
        long documentSize = parsedDocument == null ? 0L : parsedDocument.source().estimatedSizeInBytes();

        for (CompiledMetric metric : compiled.metrics()) {
            if (metric.trigger() != trigger) {
                continue;
            }
            if (source != null && metric.predicate().test(source) == false) {
                continue;
            }
            if (source == null && metric.predicate() != DerivedMetricsPredicate.MATCH_ALL) {
                // the predicate needs a source we could not read, so the metric cannot be said to match
                continue;
            }
            Double value = valueOf(metric, source, documentSize);
            if (value == null) {
                continue;
            }
            List<String> dimensionNames = new ArrayList<>(metric.dimensions().size());
            List<String> dimensionValues = new ArrayList<>(metric.dimensions().size());
            resolveDimensions(metric.dimensions(), source, dimensionNames, dimensionValues);
            Interval interval = metric.interval();
            SeriesKey series = new SeriesKey(
                project,
                sourceDataStream,
                metric.name(),
                interval.name(),
                metric.reduction(),
                List.copyOf(dimensionNames),
                List.copyOf(dimensionValues)
            );
            long bucketStart = DerivedMetricsBuffer.bucketStart(now, interval.millis());
            buffer.record(new BucketKey(series, bucketStart, interval.millis()), value);
        }
    }

    private static Double valueOf(CompiledMetric metric, Map<String, Object> source, long documentSize) {
        return switch (metric.source()) {
            case CompiledDerivedMetrics.Source.Constant constant -> constant.value();
            case CompiledDerivedMetrics.Source.DocumentSize unused -> (double) documentSize;
            case CompiledDerivedMetrics.Source.Field field -> source == null
                ? null
                : DerivedMetricsSourceReader.numericValue(source, field.path());
        };
    }

    private static void resolveDimensions(List<String> dimensions, Map<String, Object> source, List<String> names, List<String> values) {
        if (dimensions.isEmpty() || source == null) {
            return;
        }
        for (String dimension : dimensions) {
            String value = DerivedMetricsSourceReader.stringValue(source, dimension);
            if (value != null) {
                names.add(dimension);
                values.add(value);
            }
        }
    }

    /**
     * Emits every bucket whose interval has closed. Intervals with no observations hold no buckets and therefore emit nothing, which is
     * what keeps a quiet stream from producing a steady trickle of zeroes.
     */
    void flush() {
        flush(buffer.drainClosed(threadPool.absoluteTimeInMillis(), graceMillis));
    }

    /**
     * Converts and sends in bulk-sized chunks rather than materialising every closed bucket first, so peak memory during a flush is one
     * bulk rather than the whole drained set.
     */
    private void flush(List<Map.Entry<BucketKey, Accumulator>> closed) {
        if (closed.isEmpty()) {
            reportDrops();
            return;
        }
        // Group by project only: a bulk request can address any number of destinations, but it is scoped to one project.
        Map<ProjectId, BulkRequest> pending = new HashMap<>();
        for (Map.Entry<BucketKey, Accumulator> entry : closed) {
            ProjectId project = entry.getKey().series().project();
            BulkRequest bulk = pending.computeIfAbsent(project, unused -> new BulkRequest());
            bulk.add(DerivedMetricsEmitter.toIndexRequest(entry.getKey(), entry.getValue(), nodeName));
            if (bulk.numberOfActions() >= bulkSize) {
                pending.remove(project);
                send(project, bulk);
            }
        }
        pending.forEach(this::send);
        reportDrops();
    }

    /**
     * Sends one bulk, keeping a ceiling on how many are outstanding. Emission is fire and forget, so without a ceiling a destination
     * that cannot keep up would let every flush add to an unbounded queue of in-flight requests.
     */
    private void send(ProjectId project, BulkRequest bulk) {
        int documents = bulk.numberOfActions();
        if (inFlightBulks.incrementAndGet() > maxInFlightBulks) {
            inFlightBulks.decrementAndGet();
            droppedForBackpressure.addAndGet(documents);
            return;
        }
        client.projectClient(project).bulk(bulk, ActionListener.runAfter(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                if (response.hasFailures() == false) {
                    return;
                }
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        // one message is enough to diagnose a systematic problem, and the destination is managed so failures are not
                        // expected to be per-document
                        logger.warn(() -> "failed to write derived metrics to [" + item.getIndex() + "]", item.getFailure().getCause());
                        return;
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "failed to write [" + documents + "] derived metric documents for project [" + project + "]", e);
            }
        }, inFlightBulks::decrementAndGet));
    }

    private void reportDrops() {
        long shed = droppedForBackpressure.getAndSet(0);
        if (shed > 0) {
            logger.warn(
                "derived metrics dropped [{}] documents because [{}] bulk requests were already in flight; "
                    + "the destination is not keeping up",
                shed,
                maxInFlightBulks
            );
        }
        long dropped = buffer.droppedSeries();
        if (dropped > lastReportedDrops) {
            logger.warn(
                "derived metrics dropped [{}] observations because this node is already tracking [{}] series; "
                    + "reduce the configured dimensions or raise [{}]",
                dropped - lastReportedDrops,
                buffer.size(),
                MAX_SERIES_PER_NODE.getKey()
            );
            lastReportedDrops = dropped;
        }
    }

    // visible for testing
    DerivedMetricsBuffer buffer() {
        return buffer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Scheduler.Cancellable cancellable = scheduled;
        if (cancellable != null) {
            cancellable.cancel();
        }
        // emit whatever is still buffered rather than losing the partial intervals
        flush(buffer.drainAll());
    }
}
