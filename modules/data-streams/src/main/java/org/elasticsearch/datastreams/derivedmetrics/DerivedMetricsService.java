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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
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

    /**
     * The buffer's own circuit breaker. Its limit is a fraction of the heap by default, so a node with a small heap gets a
     * proportionally small metrics budget without anyone configuring it.
     */
    public static final String BREAKER_NAME = "derived_metrics";
    public static final double DEFAULT_BREAKER_LIMIT_FRACTION = 0.05;
    public static final double DEFAULT_BREAKER_OVERHEAD = 1.0;

    /**
     * Five percent of the heap. Expressed as a fraction rather than a fixed size so that a node with a small heap gets a proportionally
     * small metrics budget without anyone having to configure one.
     */
    public static long defaultBreakerLimit() {
        return (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * DEFAULT_BREAKER_LIMIT_FRACTION);
    }

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

    private final ThreadLocal<RecordingScratch> scratches = ThreadLocal.withInitial(RecordingScratch::new);

    private volatile Scheduler.Cancellable scheduled;
    private volatile boolean closed;
    private long lastReportedDrops;

    public DerivedMetricsService(Settings settings, Client client, ThreadPool threadPool, BigArrays bigArrays, String nodeName) {
        this.client = new OriginSettingClient(client, DataStreamDerivedMetrics.DERIVED_METRICS_ORIGIN);
        this.threadPool = threadPool;
        this.buffer = new DerivedMetricsBuffer(bigArrays, MAX_SERIES_PER_NODE.get(settings), MAX_SERIES_PER_STREAM.get(settings));
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
            RecordingScratch scratch = scratches.get();
            String[] values = scratch.dimensionValues(metric.dimensions().size());
            resolveDimensions(metric.dimensions(), source, values);
            Interval interval = metric.interval();
            long bucketStart = DerivedMetricsBuffer.bucketStart(now, interval.millis());
            buffer.record(
                scratch.tableKey(project, sourceDataStream, metric, bucketStart, interval.millis()),
                values,
                scratch.encoding,
                value
            );
        }
    }

    /**
     * Per-thread reusable state for the write path. The dimension buffer, the encoding buffer and the table key are all reused, so
     * recording an observation against a series that already exists allocates nothing at all — which matters because this runs on the
     * indexing thread for every document times every metric.
     */
    private static final class RecordingScratch {
        private final DerivedMetricsDimensionCodec.Scratch encoding = new DerivedMetricsDimensionCodec.Scratch();
        private String[] values = new String[16];
        private TableKey key;

        String[] dimensionValues(int size) {
            if (values.length < size) {
                values = new String[size];
            }
            for (int i = 0; i < size; i++) {
                values[i] = null;
            }
            return values;
        }

        /**
         * A table key changes only when the bucket rolls or the metric differs, so the previous one is reused whenever it still matches.
         */
        TableKey tableKey(ProjectId project, String sourceDataStream, CompiledMetric metric, long bucketStart, long intervalMillis) {
            TableKey previous = key;
            if (previous != null
                && previous.bucketStartMillis() == bucketStart
                && previous.metric() == metric
                && previous.sourceDataStream().equals(sourceDataStream)
                && previous.project().equals(project)) {
                return previous;
            }
            key = new TableKey(project, sourceDataStream, metric, bucketStart, intervalMillis);
            return key;
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

    /**
     * Fills the caller's reusable array with the value of each configured dimension, leaving null where the document did not have one.
     * A document missing a dimension forms its own series rather than sharing a placeholder.
     */
    private static void resolveDimensions(List<String> dimensions, Map<String, Object> source, String[] values) {
        if (dimensions.isEmpty() || source == null) {
            return;
        }
        for (int i = 0; i < dimensions.size(); i++) {
            values[i] = DerivedMetricsSourceReader.stringValue(source, dimensions.get(i));
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
    /**
     * Converts and sends in bulk-sized chunks rather than materialising every closed bucket first, so peak memory during a flush is one
     * bulk rather than the whole drained set. Every drained table is closed, which is what returns its memory to the circuit breaker.
     */
    private void flush(List<Map.Entry<TableKey, DerivedMetricsSeriesTable>> closed) {
        if (closed.isEmpty()) {
            reportDrops();
            return;
        }
        // Group by project only: a bulk can address any number of destinations, but it is scoped to one project.
        Map<ProjectId, BulkRequest> pending = new HashMap<>();
        BytesRef spare = new BytesRef();
        for (Map.Entry<TableKey, DerivedMetricsSeriesTable> entry : closed) {
            TableKey key = entry.getKey();
            DerivedMetricsSeriesTable table = entry.getValue();
            try {
                long series = table.size();
                for (long ordinal = 0; ordinal < series; ordinal++) {
                    ProjectId project = key.project();
                    BulkRequest bulk = pending.computeIfAbsent(project, unused -> new BulkRequest());
                    bulk.add(DerivedMetricsEmitter.toIndexRequest(key, table, ordinal, spare, nodeName, 0));
                    if (bulk.numberOfActions() >= bulkSize) {
                        pending.remove(project);
                        send(project, bulk);
                    }
                }
            } finally {
                table.close();
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
        buffer.close();
    }
}
